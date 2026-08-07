// control-plane — Phase-04. Registers the full ControlService gRPC
// surface (control.proto): RegisterModelManifest/GetModelManifest/
// ListModels (registry/, Step E) and IssueProviderToken (Vault-backed,
// Step J) — all four RPCs have real handlers as of Step J, added in
// separate commits, the same "defined here, wired when consumed" pattern
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
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/control-plane/internal/etcdclient"
	pb "github.com/onezox/OneZox/services/control-plane/internal/pb/control/v1"
	"github.com/onezox/OneZox/services/control-plane/internal/providertoken"
	"github.com/onezox/OneZox/services/control-plane/internal/registry"
	"github.com/onezox/OneZox/services/control-plane/internal/rollout"
	"github.com/onezox/OneZox/services/control-plane/internal/vaultclient"
)

const serviceName = "control-plane"

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

// server implements pb.ControlServiceServer. All four RPCs are now
// overridden — UnimplementedControlServiceServer stays embedded as a
// forward-compat guard against a future proto RPC this struct hasn't
// been updated for yet, not because anything here is still unimplemented.
type server struct {
	pb.UnimplementedControlServiceServer
	db            *sql.DB
	registry      *registry.Service
	providerToken *providertoken.Service
	rollout       *rollout.Service
	log           *slog.Logger
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

// IssueProviderToken — Step J. "(Vault-backed; gateway only)" per
// Phase-04.txt: enforced at the network layer (Step K's own
// CiliumNetworkPolicy ingress rule, added once provider-gateway is the
// only service granted a peer rule on control-plane's gRPC port for this
// purpose), not by an in-RPC caller-identity check — this repo has no
// established per-RPC-method (L7) authorization precedent to build on,
// only the L3/L4-plus-mTLS-required shape every other -mtls policy here
// already uses.
func (s *server) IssueProviderToken(ctx context.Context, req *pb.IssueProviderTokenRequest) (*pb.IssueProviderTokenResponse, error) {
	if req.GetProvider() == "" {
		return nil, status.Error(codes.InvalidArgument, "provider is required")
	}
	token, ttlSeconds, err := s.providerToken.IssueToken(ctx, req.GetProvider(), req.GetScope())
	if err != nil {
		// Deliberately no "error" field with err's own text here (unlike
		// every other handler in this file) — the underlying error from
		// vaultclient.ReadProviderSecret could, in principle, echo back
		// path/field details close enough to secret material that this
		// is treated as a boundary, not just a style choice.
		s.log.Error("IssueProviderToken failed", "provider", req.GetProvider())
		return nil, status.Error(codes.Internal, "failed to issue provider token")
	}
	return &pb.IssueProviderTokenResponse{Token: token, TtlSeconds: ttlSeconds}, nil
}

// CreateRollout/PromoteRollout/AbortRollout/GetRolloutStatus — Step L.
// Only the human-triggered half of the rollout state machine is exposed
// as RPCs at all (control.proto's own header, Step D) — the automatic
// timer/AnalysisRun-driven advance and rollback live entirely inside
// this process (Step M), never reachable over the network. Enforced at
// the network layer the same way IssueProviderToken already is
// (control-plane-mtls's own admin-api ingress rule, added at admin-api's
// Step E) — admin-api is the only additional identity authorized to
// reach this port beyond provider-gateway.
func (s *server) CreateRollout(ctx context.Context, req *pb.CreateRolloutRequest) (*pb.CreateRolloutResponse, error) {
	rolloutID, err := s.rollout.CreateRollout(ctx, req.GetModelRef(), req.GetVersionId(), req.GetStrategyJson())
	if err != nil {
		s.log.Error("CreateRollout failed", "model_ref", req.GetModelRef(), "version_id", req.GetVersionId(), "error", err)
		return nil, mapRolloutError(err)
	}
	return &pb.CreateRolloutResponse{RolloutId: rolloutID}, nil
}

func (s *server) PromoteRollout(ctx context.Context, req *pb.PromoteRolloutRequest) (*pb.PromoteRolloutResponse, error) {
	newStage, err := s.rollout.PromoteRollout(ctx, req.GetRolloutId())
	if err != nil {
		s.log.Error("PromoteRollout failed", "rollout_id", req.GetRolloutId(), "error", err)
		return nil, mapRolloutError(err)
	}
	return &pb.PromoteRolloutResponse{NewStage: newStage}, nil
}

func (s *server) AbortRollout(ctx context.Context, req *pb.AbortRolloutRequest) (*pb.AbortRolloutResponse, error) {
	if err := s.rollout.AbortRollout(ctx, req.GetRolloutId()); err != nil {
		s.log.Error("AbortRollout failed", "rollout_id", req.GetRolloutId(), "error", err)
		return nil, mapRolloutError(err)
	}
	return &pb.AbortRolloutResponse{}, nil
}

// GetRolloutStatus is read-only (Step D's own commands-vs-queries split,
// held within control-plane too) — no audit_log write happens for this
// RPC anywhere in the call chain, matching every other pure read in this
// service (GetModelManifest, ListModels).
func (s *server) GetRolloutStatus(ctx context.Context, req *pb.GetRolloutStatusRequest) (*pb.GetRolloutStatusResponse, error) {
	r, err := s.rollout.GetRolloutStatus(ctx, req.GetRolloutId(), req.GetModelRef())
	if err != nil {
		if errors.Is(err, rollout.ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		s.log.Error("GetRolloutStatus failed", "rollout_id", req.GetRolloutId(), "model_ref", req.GetModelRef(), "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.GetRolloutStatusResponse{
		RolloutId:     r.RolloutID,
		ModelRef:      r.ModelRef,
		VersionId:     r.VersionID,
		Stage:         r.Stage,
		Status:        r.Status,
		CanaryPercent: rollout.StagePercent(r.Stage),
		StartedAt:     r.StartedAt.Format(time.RFC3339),
	}
	if r.EndedAt != nil {
		resp.EndedAt = r.EndedAt.Format(time.RFC3339)
	}
	return resp, nil
}

// mapRolloutError translates rollout package sentinel errors to the
// gRPC status codes that actually distinguish them for a caller — "you
// asked for something that doesn't exist" (NotFound) is not the same
// class of problem as "your request conflicts with current state"
// (FailedPrecondition), and neither is an opaque Internal.
func mapRolloutError(err error) error {
	switch {
	case errors.Is(err, rollout.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, rollout.ErrNotRunning),
		errors.Is(err, rollout.ErrAlreadyRunning),
		errors.Is(err, rollout.ErrAlreadyFullyPromoted),
		errors.Is(err, rollout.ErrNoActiveVersion):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
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
	vaultClient := vaultclient.New(vaultAddr, vaultK8sRole)

	etcdEndpoints := strings.Split(envOr("ETCD_ENDPOINTS", "http://etcd.default.svc.cluster.local:2379"), ",")
	etcdCli, err := etcdclient.New(etcdEndpoints)
	if err != nil {
		log.Error("failed to create etcd client", "error", err, "endpoints", etcdEndpoints)
		os.Exit(1)
	}
	defer func() { _ = etcdCli.Close() }()

	registrySvc := registry.NewService(registry.NewCockroachStore(db), vaultClient, etcdCli, log)

	// Genuinely short — separate from and much shorter than Vault's own
	// K8s-auth login TTL (15m, scripts/vault-setup-control-plane.sh).
	// Bounds how long provider-gateway may hold a fetched key in memory
	// before re-fetching (Step M), not a literal per-call Vault lease —
	// see providertoken's own package doc for why that distinction is
	// real for static third-party API keys.
	providerTokenTTL := time.Duration(envInt64Or("PROVIDER_TOKEN_TTL_SECONDS", 300)) * time.Second
	providerTokenSvc := providertoken.NewService(vaultClient, providerTokenTTL, log)

	// Step L: rollout.Service depends on registrySvc directly (the
	// Registry interface it declares) for GetModelManifest/ActivateVersion
	// — reusing the SAME registry instance every other RPC in this
	// process uses, not a second connection to the same data.
	rolloutSvc := rollout.NewService(rollout.NewCockroachStore(db), etcdCli, registrySvc, log)

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, &server{
		db:            db,
		registry:      registrySvc,
		providerToken: providerTokenSvc,
		rollout:       rolloutSvc,
		log:           log,
	})

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
