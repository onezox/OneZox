// control-plane — Phase-04 Step D: service skeleton only. Registers the
// full ControlService gRPC surface (control.proto) with
// UnimplementedControlServiceServer embedded, so every RPC returns
// codes.Unimplemented for now — RegisterModelManifest/GetModelManifest/
// ListModels (registry/, Step E) and IssueProviderToken (Vault-backed,
// Step J) each get their own real handler in a later, separately-
// committed step, the same "defined here, wired when consumed" pattern
// provider-gateway's own ProviderHealth used in Phase-02.
//
// Off the hot path (Phase-04.txt's own INFRASTRUCTURE REQUIRED section:
// "control-plane runs on a general/control pool") — no streaming,
// backpressure, coalescing, or breaker machinery, unlike edge-gateway/
// data-plane/provider-gateway. A straightforward config/registry gRPC+
// HTTP service. This kind cluster has no separate node-pool/taint
// mechanism to actually place it on a distinct pool (only two untainted
// worker nodes exist), so that framing is aspirational infra context, not
// something this Deployment can enforce locally — same class of gap as
// Terraform/Karpenter being deferred to a cloud phase.
//
// Connects to CockroachDB as the control_plane role (data/migrations/
// 0012), never root — root is a member of the built-in admin role and
// bypasses GRANT/REVOKE entirely, which would make model_manifest's
// storage-layer immutability (0008, adversarially verified in Step C)
// meaningless if this service connected as root instead.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	pb "github.com/onezox/OneZox/services/control-plane/internal/pb/control/v1"
)

const serviceName = "control-plane"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// server implements pb.ControlServiceServer. Embedding
// UnimplementedControlServiceServer means every RPC returns
// codes.Unimplemented until its own step overrides it — RegisterModelManifest/
// GetModelManifest/ListModels (Step E) and IssueProviderToken (Step J)
// are all still unset here, deliberately, per Step D's "skeleton only"
// scope.
type server struct {
	pb.UnimplementedControlServiceServer
	db  *sql.DB
	log *slog.Logger
}

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	log := slog.New(handler).With("service", serviceName)

	cockroachHost := envOr("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local")
	cockroachPort := envOr("COCKROACH_PORT", "26257")
	cockroachUser := envOr("COCKROACH_USER", "control_plane")
	dsn := fmt.Sprintf("postgres://%s@%s:%s/defaultdb?sslmode=disable", cockroachUser, cockroachHost, cockroachPort)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error("failed to open CockroachDB connection", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(context.Background()); err != nil {
		log.Error("failed to ping CockroachDB", "error", err, "host", cockroachHost, "user", cockroachUser)
		os.Exit(1)
	}
	// Success-path log, not only the error path above — confirms the
	// control_plane role (not root) is what's actually connecting.
	log.Info("connected to CockroachDB", "host", cockroachHost, "user", cockroachUser)

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, &server{db: db, log: log})

	go func() {
		log.Info("control-plane gRPC listening", "port", grpcPort)
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
		if err := db.PingContext(r.Context()); err != nil {
			log.Error("readiness check failed", "cockroach_error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	httpPort := envOr("PORT", "8080")
	log.Info("control-plane HTTP listening", "port", httpPort)
	if err := http.ListenAndServe(":"+httpPort, mux); err != nil {
		log.Error("HTTP server exited", "error", err)
		os.Exit(1)
	}
}
