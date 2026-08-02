// provider-gateway — Phase-02: replaces the Phase-00 provider-stub with the
// real dedicated Go service that owns all provider concerns. Step D5 wired
// the pipeline's plumbing end-to-end against the fake adapter (worker_ref
// -> registry lookup -> adapter Invoke -> stream deltas back over gRPC,
// with real backpressure — the gRPC Send in the loop below only proceeds
// once the client has consumed the previous message, which is what
// naturally paces the adapter's own Recv calls, no manual buffering, same
// discipline as Phase-01's SSE relay). Step E adds the fleet-wide quota
// governor in front of the adapter call: exhausted quota returns a typed
// FallbackSignal (reason QUOTA_EXHAUSTED) and closes the stream cleanly —
// "throttles rather than errors" (Phase-02.txt), not a gRPC error the
// caller has to specifically catch. Breaker and coalescing (Steps F-G)
// each insert their own stage between quota and the adapter call as
// they're built. InvokeEmbedding and ProviderHealth stay unimplemented.
//
// No CockroachDB or MinIO connection, unlike provider-stub: Phase-02.txt's
// DATABASE TABLES REQUIRED section is explicit that this phase owns no
// relational tables.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters"
	"github.com/onezox/OneZox/services/provider-gateway/internal/adapters/fake"
	pb "github.com/onezox/OneZox/services/provider-gateway/internal/pb/provider/v1"
	"github.com/onezox/OneZox/services/provider-gateway/internal/quota"
)

const serviceName = "provider-gateway"

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
	registry     *adapters.Registry
	quotaCounter quota.Counter
	quotaPolicy  quota.Policy
	log          *slog.Logger
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

func toPbDelta(requestID string, d adapters.Delta) *pb.InvokeResponse {
	delta := &pb.Delta{
		RequestId: requestID,
		IsFinal:   d.IsFinal,
	}
	if d.Content != nil {
		delta.Content = d.Content
	}
	if d.FinishReason != nil {
		delta.FinishReason = d.FinishReason
	}
	if d.PrefixCacheHandle != nil {
		delta.PrefixCacheHandle = d.PrefixCacheHandle
	}
	return &pb.InvokeResponse{Event: &pb.InvokeResponse_Delta{Delta: delta}}
}

func (s *server) Invoke(req *pb.InvokeRequest, stream grpc.ServerStreamingServer[pb.InvokeResponse]) error {
	log := s.log.With("request_id", req.GetRequestId(), "worker_ref", req.GetWorkerRef())

	providerName, model, err := adapters.ParseWorkerRef(req.GetWorkerRef())
	if err != nil {
		log.Warn("invalid worker_ref", "error", err)
		return status.Error(codes.InvalidArgument, err.Error())
	}

	adapter, err := s.registry.Lookup(providerName)
	if err != nil {
		log.Warn("unknown provider", "provider", providerName, "error", err)
		return status.Error(codes.NotFound, err.Error())
	}

	decision, err := quota.Enforce(stream.Context(), s.quotaCounter, providerName, s.quotaPolicy)
	if err != nil {
		// Fails open (quota.Enforce already returned Allow) — a Redis
		// outage isn't evidence of real provider overload, same
		// reasoning as edge-gateway's Phase-01 ratelimit/admission.
		log.Warn("quota check failed, failing open", "provider", providerName, "error", err)
	}
	if decision == quota.Throttle {
		log.Info("fleet-wide quota exhausted, signaling fallback", "provider", providerName)
		return stream.Send(&pb.InvokeResponse{
			Event: &pb.InvokeResponse_Fallback{
				Fallback: &pb.FallbackSignal{
					RequestId: req.GetRequestId(),
					Provider:  providerName,
					Reason:    pb.FallbackReason_FALLBACK_REASON_QUOTA_EXHAUSTED,
				},
			},
		})
	}

	adapterStream, err := adapter.Invoke(stream.Context(), toAdapterRequest(req, model))
	if err != nil {
		log.Warn("adapter invoke failed", "provider", providerName, "error", err)
		return status.Error(codes.Unavailable, err.Error())
	}

	for {
		delta, err := adapterStream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			log.Warn("adapter stream error", "provider", providerName, "error", err)
			return status.Error(codes.Unavailable, err.Error())
		}
		if err := stream.Send(toPbDelta(req.GetRequestId(), delta)); err != nil {
			// Caller went away mid-stream — nothing more to send, and
			// the adapter's own Stream is responsible for releasing its
			// upstream connection when Recv stops being called.
			return err
		}
		if delta.IsFinal {
			return nil
		}
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
	registry := adapters.NewRegistry(fake.New(fakeBaseURL))

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

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterProviderServiceServer(grpcServer, &server{
		registry:     registry,
		quotaCounter: quotaCounter,
		quotaPolicy:  quotaPolicy,
		log:          log,
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
