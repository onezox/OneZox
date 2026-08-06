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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/onezox/OneZox/services/control-plane/internal/pb/control/v1"
	"github.com/onezox/OneZox/services/control-plane/internal/registry"
	"github.com/onezox/OneZox/services/control-plane/internal/vaultclient"
)

const serviceName = "control-plane"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// server implements pb.ControlServiceServer. Embedding
// UnimplementedControlServiceServer means IssueProviderToken (Step J)
// still returns codes.Unimplemented — RegisterModelManifest/
// GetModelManifest/ListModels are overridden below (Step E).
type server struct {
	pb.UnimplementedControlServiceServer
	db       *sql.DB
	registry *registry.Service
	log      *slog.Logger
}

func (s *server) RegisterModelManifest(ctx context.Context, req *pb.RegisterModelManifestRequest) (*pb.RegisterModelManifestResponse, error) {
	versionID, err := s.registry.RegisterModelManifest(ctx, req.GetModelRef(), req.GetSpecJson(), req.GetCreatedBy())
	if err != nil {
		s.log.Error("RegisterModelManifest failed", "model_ref", req.GetModelRef(), "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RegisterModelManifestResponse{VersionId: versionID}, nil
}

func (s *server) GetModelManifest(ctx context.Context, req *pb.GetModelManifestRequest) (*pb.GetModelManifestResponse, error) {
	m, err := s.registry.GetModelManifest(ctx, req.GetModelRef(), req.GetVersionId())
	switch {
	case errors.Is(err, registry.ErrNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, registry.ErrInvalidSignature):
		// FailedPrecondition, not Internal: the request itself was fine —
		// the manifest this system holds failed its own integrity check.
		// A caller (or an operator reading logs) should be able to tell
		// "your request was malformed" apart from "the registry's data is
		// suspect," and this is genuinely the latter.
		s.log.Error("refusing to serve manifest: signature verification failed",
			"model_ref", req.GetModelRef(), "version_id", req.GetVersionId())
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		s.log.Error("GetModelManifest failed", "model_ref", req.GetModelRef(), "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.GetModelManifestResponse{
		VersionId: m.VersionID,
		ModelRef:  m.ModelRef,
		SpecJson:  m.SpecJSON,
		Signature: m.Signature,
		CreatedBy: m.CreatedBy,
		CreatedAt: m.CreatedAt.Format(time.RFC3339),
		Status:    m.Status,
	}, nil
}

func (s *server) ListModels(ctx context.Context, req *pb.ListModelsRequest) (*pb.ListModelsResponse, error) {
	entries, err := s.registry.ListModels(ctx)
	if err != nil {
		s.log.Error("ListModels failed", "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	pbEntries := make([]*pb.ListModelsResponse_Entry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.ListModelsResponse_Entry{ModelRef: e.ModelRef, ActiveVersionId: e.ActiveVersionID}
	}
	return &pb.ListModelsResponse{Models: pbEntries}, nil
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

	vaultAddr := envOr("VAULT_ADDR", "http://vault-active.default.svc.cluster.local:8200")
	vaultK8sRole := envOr("VAULT_K8S_ROLE", "control-plane")
	signer := vaultclient.New(vaultAddr, vaultK8sRole)
	registrySvc := registry.NewService(registry.NewCockroachStore(db), signer, log)

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, &server{db: db, registry: registrySvc, log: log})

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
