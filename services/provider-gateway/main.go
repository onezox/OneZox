// provider-gateway — Phase-02: replaces the Phase-00 provider-stub with the
// real dedicated Go service that owns all provider concerns. This step
// (Step C1) is the skeleton: a gRPC server for provider.v1.ProviderService
// with every RPC left unimplemented (protoc-gen-go-grpc's own
// UnimplementedProviderServiceServer already returns codes.Unimplemented
// for all three methods when embedded and not overridden — no hand-written
// "not wired yet" stub needed), plus the standard /healthz, /readyz,
// /metrics HTTP surface every OneZox service exposes. Adapters, quota,
// breaker, coalescing, streaming, and prefix-cache passthrough land in
// later Phase-02 steps.
//
// No CockroachDB or MinIO connection, unlike provider-stub: Phase-02.txt's
// DATABASE TABLES REQUIRED section is explicit that this phase owns no
// relational tables. Redis arrives with Step E's quota governor, not here.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"

	pb "github.com/onezox/OneZox/services/provider-gateway/internal/pb/provider/v1"
)

const serviceName = "provider-gateway"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
// UnimplementedProviderServiceServer without overriding any method gives
// every RPC codes.Unimplemented for free — this step's entire "not wired
// yet" behavior, with no hand-written stub bodies to keep in sync with the
// interface as it grows.
type server struct {
	pb.UnimplementedProviderServiceServer
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

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterProviderServiceServer(grpcServer, &server{})

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
		// No dependency yet to check (Redis arrives with Step E) — the
		// gRPC server binding successfully above is this step's whole
		// notion of ready.
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
