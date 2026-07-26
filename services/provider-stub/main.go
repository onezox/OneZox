// provider-stub — Phase-00 throwaway health stub (Go).
//
// Proves the toolchain, mesh, and telemetry work end-to-end: on boot it
// connects to CockroachDB, Redis, and MinIO, emits a trace span covering
// the whole sequence, and exposes /healthz, /readyz, and /metrics.
// Replaced by the real provider-gateway in a later phase.
package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	serviceName = "provider-stub"
	tenant      = "onezox-dev"
)

var bootTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "provider_stub_boot_total",
	Help: "Number of successful boot sequences",
})

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

	res, err := resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func writeHealthProbe(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	_, err := pool.Exec(ctx, "INSERT INTO health_probe (service) VALUES ($1)", serviceName)
	if err != nil {
		return err
	}
	log.Info("wrote health_probe row", "table", "health_probe")
	return nil
}

func setAndGetRedisKey(ctx context.Context, rdb *redis.Client, log *slog.Logger) error {
	key := tenant + ":" + serviceName + ":boot"
	value := time.Now().UTC().Format(time.RFC3339)
	if err := rdb.Set(ctx, key, value, 0).Err(); err != nil {
		return err
	}
	got, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	log.Info("redis set/get round-trip verified", "key", key, "value", got)
	return nil
}

func uploadTestObject(ctx context.Context, log *slog.Logger) error {
	endpoint := envOr("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")
	accessKey := envOr("MINIO_ACCESS_KEY", "onezox-admin")
	secretKey := envOr("MINIO_SECRET_KEY", "onezox-local-dev-only")
	bucket := envOr("MINIO_BUCKET", "onezox-artifacts")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return err
	}

	key := "health-checks/" + serviceName + "-" + time.Now().UTC().Format("20060102T150405Z") + ".txt"
	body := []byte("OneZox Phase-00 health check from " + serviceName + " at " + time.Now().UTC().Format(time.RFC3339))
	_, err = client.PutObject(ctx, bucket, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		return err
	}
	log.Info("uploaded test object to MinIO", "bucket", bucket, "key", key)
	return nil
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

	tracer := otel.Tracer(serviceName)
	ctx, span := tracer.Start(ctx, "provider_stub.boot")
	log.Info("starting boot sequence")

	pgHost := envOr("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local")
	pool, err := pgxpool.New(ctx, "postgres://root@"+pgHost+":26257/defaultdb?sslmode=disable")
	if err != nil {
		log.Error("failed to connect to CockroachDB", "error", err)
		os.Exit(1)
	}
	if err := writeHealthProbe(ctx, pool, log); err != nil {
		log.Error("failed to write health_probe row", "error", err)
		os.Exit(1)
	}

	redisHost := envOr("REDIS_HOST", "redis-cluster-headless.default.svc.cluster.local")
	rdb := redis.NewClient(&redis.Options{Addr: redisHost + ":6379"})
	if err := setAndGetRedisKey(ctx, rdb, log); err != nil {
		log.Error("failed redis set/get", "error", err)
		os.Exit(1)
	}

	if err := uploadTestObject(ctx, log); err != nil {
		log.Error("failed to upload test object to MinIO", "error", err)
	}

	bootTotal.Inc()
	log.Info("boot sequence complete")
	span.End()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pgOK := pool.Ping(r.Context()) == nil
		redisOK := rdb.Ping(r.Context()).Err() == nil
		if pgOK && redisOK {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		log.Error("readiness check failed", "pg_ok", pgOK, "redis_ok", redisOK)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	port := envOr("PORT", "8080")
	log.Info("provider-stub listening", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Error("server exited", "error", err)
		os.Exit(1)
	}
}
