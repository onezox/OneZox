// provider-gateway — Phase-02: replaces the Phase-00 provider-stub with the
// real dedicated Go service that owns all provider concerns. Step D5 wired
// the pipeline's plumbing end-to-end against the fake adapter (worker_ref
// -> registry lookup -> adapter Invoke -> stream deltas back over gRPC,
// with real backpressure). Step E added the fleet-wide quota governor;
// Step F added the per-provider circuit breaker; both sit in front of the
// adapter call and return a typed FallbackSignal instead of an error when
// they say no. Step G wraps the whole quota+breaker+adapter sequence in
// request coalescing (Phase-02.txt's own flow order: "coalesce check ->
// quota governor -> breaker check -> adapter") — a concurrent identical
// call joins an already-in-flight one instead of independently consuming
// quota or being separately breaker-gated for work it isn't causing.
// InvokeEmbedding and ProviderHealth stay unimplemented.
//
// Step J4-J6: the real openai/anthropic/google adapters register here
// too, each only if its own API key env var is actually set (envFrom
// mounts provider-gateway-credentials, Step J3) — a missing key means
// that provider is simply absent from the registry, not a startup
// failure, same "proceed with what's available" discipline used
// elsewhere in Phase-02. All three normalize a genuinely different wire
// format behind the same adapters.Adapter contract: OpenAI's uniform SSE
// chunks, Anthropic's named-event stream, and Google's alt=sse variant of
// its own contents/parts shape.
//
// No CockroachDB or MinIO connection, unlike provider-stub: Phase-02.txt's
// DATABASE TABLES REQUIRED section is explicit that this phase owns no
// relational tables.
//
// Step N1-N3 (EC4 telemetry completeness): a root span per Invoke call,
// with coalesce/quota/breaker/adapter/stream each instrumented as a
// direct child (not nested under each other) — Group.Invoke (Step G)
// returns immediately and does the leader's real work
// (quota+breaker+adapter) on a spawned goroutine, so "coalesce" itself is
// genuinely near-instant; the quota/breaker/adapter spans are created
// inside the `call` closure using the SAME root-anchored context, which
// is what makes them siblings of "coalesce" rather than its children.
// /metrics gains three provider-gateway-specific series (adapter call
// duration, breaker state, quota headroom) — the HTTP handler already
// existed (promhttp.Handler(), Step C1) but had nothing but Go runtime
// defaults registered, which is why Prometheus was never actually
// scraping anything meaningful from this service. A structured
// completion log line is added on the success path (previously: error
// paths only, the same gap Phase-01 edge-gateway had before its own Step
// J3) — request_id and finish_reason only, verified via a fresh
// positive-controlled grep (same method as Step L1) that it carries
// nothing key-shaped.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters/anthropic"
	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters/fake"
	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters/google"
	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters/openai"
	"github.com/onezox/OneZox/services/provider-gateway/internal/breaker"
	"github.com/onezox/OneZox/services/provider-gateway/internal/coalesce"
	pb "github.com/onezox/OneZox/services/provider-gateway/internal/pb/provider/v1"
	"github.com/onezox/OneZox/services/provider-gateway/internal/quota"
	relay "github.com/onezox/OneZox/services/provider-gateway/internal/stream"
)

const serviceName = "provider-gateway"

var tracer = otel.Tracer(serviceName)

var (
	adapterCallDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "provider_gateway_adapter_call_duration_seconds",
		Help:    "Duration of adapter.Invoke() establishing a provider stream (not the full drain), labeled by provider and outcome (success|failure).",
		Buckets: prometheus.DefBuckets,
	}, []string{"provider", "outcome"})

	// 0=closed, 1=half_open, 2=open — matches internal/breaker's own
	// three effective states (breaker.Closed/HalfOpen/Open), set directly
	// from the Decision a live breaker.Check just returned so the metric
	// never claims a state that wasn't actually just observed.
	breakerStateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provider_gateway_breaker_state",
		Help: "Per-provider circuit breaker state as of the last check: 0=closed, 1=half_open, 2=open.",
	}, []string{"provider"})

	quotaHeadroomGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provider_gateway_quota_headroom",
		Help: "Remaining requests in the current fleet-wide quota window for a provider (limit - current count, floored at 0).",
	}, []string{"provider"})
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64Or(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	endpoint := envOr("OTEL_EXPORTER_OTLP_ENDPOINT_HOST", "otel-collector-opentelemetry-collector.default.svc.cluster.local:4317")

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

// server implements pb.ProviderServiceServer. Embedding
// UnimplementedProviderServiceServer means InvokeEmbedding and
// ProviderHealth still return codes.Unimplemented automatically — only
// Invoke is overridden below.
type server struct {
	pb.UnimplementedProviderServiceServer
	registry      *adapters.Registry
	quotaCounter  quota.Counter
	quotaPolicy   quota.Policy
	breakerStore  breaker.Store
	breakerConfig breaker.Config
	coalesceGroup *coalesce.Group
	log           *slog.Logger
}

// reportingStream wraps the real adapter stream so the breaker outcome is
// reported exactly once, based on how the stream actually ends — success
// only on reaching a final delta with no error, failure on any error
// (including an EOF that arrives before a final delta, an ill-behaved
// upstream). This wrapping happens inside the coalesced closure, so only
// the leader's actual drain (coalesce.Group.lead, not any follower)
// triggers it — a follower riding an in-flight call reports nothing,
// since it isn't the one that made the call.
type reportingStream struct {
	inner    adapters.Stream
	report   func(success bool)
	reported bool
}

func (r *reportingStream) Recv() (adapters.Delta, error) {
	d, err := r.inner.Recv()
	if err != nil {
		if !r.reported {
			r.reported = true
			r.report(false)
		}
		return d, err
	}
	if d.IsFinal && !r.reported {
		r.reported = true
		r.report(true)
	}
	return d, nil
}

func toAdapterRequest(req *pb.InvokeRequest, model string) adapters.InvokeRequest {
	messages := make([]adapters.Message, len(req.GetMessages()))
	for i, m := range req.GetMessages() {
		messages[i] = adapters.Message{Role: m.GetRole(), Content: m.GetContent()}
	}
	out := adapters.InvokeRequest{
		RequestID: req.GetRequestId(),
		Model:     model,
		Messages:  messages,
	}
	if params := req.GetParams(); params != nil {
		if params.MaxTokens != nil {
			out.MaxTokens = params.MaxTokens
		}
		if params.Temperature != nil {
			out.Temperature = params.Temperature
		}
	}
	return out
}

func (s *server) Invoke(req *pb.InvokeRequest, stream grpc.ServerStreamingServer[pb.InvokeResponse]) error {
	log := s.log.With("request_id", req.GetRequestId(), "worker_ref", req.GetWorkerRef())

	// Root span for the whole request. ctx (not stream.Context()) is what
	// every child span below derives from, directly — coalesce/quota/
	// breaker/adapter/stream are siblings under this one span, per
	// Phase-02.txt's own module list (Part Q), not nested under each
	// other.
	ctx, rootSpan := tracer.Start(stream.Context(), "provider_gateway.invoke",
		trace.WithAttributes(
			attribute.String("request_id", req.GetRequestId()),
			attribute.String("worker_ref", req.GetWorkerRef()),
		),
	)
	defer rootSpan.End()

	providerName, model, err := adapters.ParseWorkerRef(req.GetWorkerRef())
	if err != nil {
		log.Warn("invalid worker_ref", "error", err)
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return status.Error(grpccodes.InvalidArgument, err.Error())
	}
	rootSpan.SetAttributes(attribute.String("provider", providerName))

	adapter, err := s.registry.Lookup(providerName)
	if err != nil {
		log.Warn("unknown provider", "provider", providerName, "error", err)
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return status.Error(grpccodes.NotFound, err.Error())
	}

	adapterReq := toAdapterRequest(req, model)

	// The leader's own context, stripped of cancellation but not values
	// (including the root span attached above, which context.WithoutCancel
	// preserves — that's what makes quota/breaker/adapter spans created
	// inside `call` below children of THIS span even though `call` itself
	// runs on a goroutine coalesce.Group spawns, not this one): this call
	// may end up shared with followers whose own requests outlive the
	// leader's — if the leader's caller disconnects, the underlying
	// adapter call must keep running for their sake, not abort because
	// the leader happened to be the one who triggered it.
	callCtx := context.WithoutCancel(ctx)

	call := func() (adapters.Stream, error) {
		_, quotaSpan := tracer.Start(callCtx, "quota")
		quotaDecision, current, err := quota.Enforce(callCtx, s.quotaCounter, providerName, s.quotaPolicy)
		if err != nil {
			// Fails open (quota.Enforce already returned Allow) — a
			// Redis outage isn't evidence of real provider overload,
			// same reasoning as edge-gateway's Phase-01
			// ratelimit/admission. Headroom gauge is left at its last
			// value rather than set from `current` (which Enforce
			// documents as untrustworthy, 0, on error) — a stale-but-
			// real number beats a confidently wrong one.
			log.Warn("quota check failed, failing open", "provider", providerName, "error", err)
			quotaSpan.RecordError(err)
		} else {
			headroom := s.quotaPolicy.Limit - current
			if headroom < 0 {
				headroom = 0
			}
			quotaHeadroomGauge.WithLabelValues(providerName).Set(float64(headroom))
		}
		quotaSpan.SetAttributes(attribute.String("decision", quotaDecision.String()))
		quotaSpan.End()
		if quotaDecision == quota.Throttle {
			log.Info("fleet-wide quota exhausted, signaling fallback", "provider", providerName)
			return nil, &relay.FallbackError{Reason: pb.FallbackReason_FALLBACK_REASON_QUOTA_EXHAUSTED, Provider: providerName}
		}

		_, breakerSpan := tracer.Start(callCtx, "breaker")
		breakerDecision, err := breaker.Check(callCtx, s.breakerStore, providerName, s.breakerConfig)
		if err != nil {
			// Same fail-open reasoning: don't assert a breaker state we
			// couldn't actually observe.
			log.Warn("breaker check failed, failing open", "provider", providerName, "error", err)
			breakerSpan.RecordError(err)
		} else {
			breakerStateGauge.WithLabelValues(providerName).Set(breakerDecisionToStateValue(breakerDecision))
		}
		breakerSpan.SetAttributes(attribute.String("decision", breakerDecision.String()))
		breakerSpan.End()
		if breakerDecision == breaker.Deny {
			log.Info("breaker open, signaling fallback", "provider", providerName)
			return nil, &relay.FallbackError{Reason: pb.FallbackReason_FALLBACK_REASON_BREAKER_OPEN, Provider: providerName}
		}

		reportOutcome := func(success bool) {
			if err := breaker.ReportResult(callCtx, s.breakerStore, providerName, s.breakerConfig, success); err != nil {
				log.Warn("failed to report breaker outcome", "provider", providerName, "success", success, "error", err)
			}
		}

		adapterStart := time.Now()
		_, adapterSpan := tracer.Start(callCtx, "adapter")
		adapterStream, err := adapter.Invoke(callCtx, adapterReq)
		adapterOutcome := "success"
		if err != nil {
			adapterOutcome = "failure"
			log.Warn("adapter invoke failed", "provider", providerName, "error", err)
			adapterSpan.RecordError(err)
			adapterSpan.SetStatus(codes.Error, err.Error())
		}
		adapterCallDuration.WithLabelValues(providerName, adapterOutcome).Observe(time.Since(adapterStart).Seconds())
		adapterSpan.End()
		if err != nil {
			reportOutcome(false)
			return nil, err
		}
		return &reportingStream{inner: adapterStream, report: reportOutcome}, nil
	}

	_, coalesceSpan := tracer.Start(ctx, "coalesce")
	sharedStream := s.coalesceGroup.Invoke(coalesce.Key(adapterReq), call)
	coalesceSpan.End()

	_, streamSpan := tracer.Start(ctx, "stream")
	var outcome, finishReason string
	sendAndObserve := func(r *pb.InvokeResponse) error {
		if d := r.GetDelta(); d != nil {
			outcome = "delta"
			if fr := d.GetFinishReason(); fr != "" {
				finishReason = fr
			}
		} else if r.GetFallback() != nil {
			outcome = "fallback"
		}
		return stream.Send(r)
	}
	err = relay.Relay(req.GetRequestId(), sharedStream, sendAndObserve)
	streamSpan.SetAttributes(attribute.String("outcome", outcome))
	var upstreamErr *relay.UpstreamError
	if errors.As(err, &upstreamErr) {
		log.Warn("adapter stream error", "provider", providerName, "error", upstreamErr.Unwrap())
		streamSpan.RecordError(err)
		streamSpan.SetStatus(codes.Error, err.Error())
		streamSpan.End()
		rootSpan.SetStatus(codes.Error, upstreamErr.Unwrap().Error())
		return status.Error(grpccodes.Unavailable, upstreamErr.Unwrap().Error())
	}
	streamSpan.End()
	if err != nil {
		// A send failure — the caller went away mid-stream, not a
		// provider failure. Propagated as-is; a coalesced call's other
		// subscribers keep running independently of this one caller
		// leaving. Not logged as a completion (it isn't one).
		rootSpan.RecordError(err)
		return err
	}

	// Step N3: the success-path completion log Phase-01's edge-gateway
	// needed its own Step J3 fix for — request_id (already on `log` via
	// .With above) and finish_reason/outcome only, nothing key-shaped
	// anywhere near this call. Re-verified with a fresh positive-
	// controlled grep after adding this (Phase-02-Progress.txt).
	log.Info("request completed", "provider", providerName, "outcome", outcome, "finish_reason", finishReason)
	return nil
}

// breakerDecisionToStateValue maps a live breaker.Check decision to the
// breaker_state gauge's numeric encoding. AllowTrial IS the half-open
// probe itself, not a separate read of stored state, so it maps to
// half_open (1) — the most accurate state to report at the instant this
// decision was made.
func breakerDecisionToStateValue(d breaker.Decision) float64 {
	switch d {
	case breaker.Allow:
		return 0 // closed
	case breaker.AllowTrial:
		return 1 // half_open
	default: // breaker.Deny
		return 2 // open
	}
}

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	log := slog.New(handler).With("service", serviceName)
	ctx := context.Background()

	tp, err := initTracer(ctx)
	if err != nil {
		log.Error("failed to init tracer", "error", err)
		os.Exit(1)
	}
	defer func() { _ = tp.Shutdown(ctx) }()

	fakeBaseURL := envOr("PROVIDER_FAKE_URL", "http://provider-fake.default.svc.cluster.local:8080")
	registeredAdapters := []adapters.Adapter{fake.New(fakeBaseURL)}

	// Real adapters only register if their key is actually present —
	// "proceed with what's available" (Step J3's own scaffolding
	// discipline), not a hard requirement that all three exist. Only
	// presence is ever logged, never the key value itself
	// (Phase-02.txt SECURITY IMPLEMENTATION: "never logged").
	if os.Getenv("OPENAI_API_KEY") != "" {
		registeredAdapters = append(registeredAdapters, openai.New(os.Getenv("OPENAI_API_KEY"), ""))
	} else {
		log.Warn("OPENAI_API_KEY not set, openai adapter not registered")
	}
	if os.Getenv("ANTHROPIC_API_KEY") != "" {
		registeredAdapters = append(registeredAdapters, anthropic.New(os.Getenv("ANTHROPIC_API_KEY"), ""))
	} else {
		log.Warn("ANTHROPIC_API_KEY not set, anthropic adapter not registered")
	}
	if os.Getenv("GOOGLE_API_KEY") != "" {
		registeredAdapters = append(registeredAdapters, google.New(os.Getenv("GOOGLE_API_KEY"), ""))
	} else {
		log.Warn("GOOGLE_API_KEY not set, google adapter not registered")
	}

	registry := adapters.NewRegistry(registeredAdapters...)

	redisHost := envOr("REDIS_HOST", "redis-cluster-headless.default.svc.cluster.local")
	redisClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{redisHost + ":6379"}})
	quotaCounter := quota.NewRedisCounter(redisClient, log)

	// Placeholder fleet-wide limit, not tuned against any real provider's
	// documented cap yet — same "not tuned against real capacity data"
	// framing Phase-01's admission thresholds used. One shared value
	// across all providers for now; Phase-02 has no per-provider config
	// table (no relational tables this phase at all).
	quotaLimit := envInt64Or("QUOTA_LIMIT_PER_MINUTE", 60)
	quotaPolicy := quota.Policy{Limit: quotaLimit, Window: time.Minute}

	breakerStore := breaker.NewRedisStore(redisClient)
	// Also placeholders, not tuned against any real provider — same
	// framing as the quota limit above.
	breakerConfig := breaker.Config{
		FailureThreshold: int(envInt64Or("BREAKER_FAILURE_THRESHOLD", 5)),
		OpenDuration:     time.Duration(envInt64Or("BREAKER_OPEN_DURATION_SECONDS", 30)) * time.Second,
	}

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterProviderServiceServer(grpcServer, &server{
		registry:      registry,
		quotaCounter:  quotaCounter,
		quotaPolicy:   quotaPolicy,
		breakerStore:  breakerStore,
		breakerConfig: breakerConfig,
		coalesceGroup: coalesce.NewGroup(),
		log:           log,
	})

	go func() {
		log.Info("provider-gateway gRPC listening", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC server exited", "error", err)
			os.Exit(1)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := redisClient.Ping(r.Context()).Err(); err != nil {
			log.Error("readiness check failed", "redis_error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	httpPort := envOr("PORT", "8080")
	log.Info("provider-gateway HTTP listening", "port", httpPort)
	if err := http.ListenAndServe(":"+httpPort, mux); err != nil {
		log.Error("HTTP server exited", "error", err)
		os.Exit(1)
	}
}
