// admin-api — Phase-05. Bridges admin-panel (Next.js RSC) and control-plane
// (Architecture Part R): a query-only GraphQL surface (admin.graphql) for
// panel reads, and a command-only gRPC surface (admin.proto) for every
// mutating action — the commands-vs-queries split settled at Step D.
//
// Off the hot path, same framing as control-plane (Phase-04.txt: "general/
// control pool") — a plain gateway service, no streaming/backpressure/
// coalescing machinery.
//
// Connects to CockroachDB as the admin_api role (data/migrations/0018),
// never root — same reasoning control-plane's own header already
// documents for its own control_plane role: root bypasses GRANT/REVOKE
// entirely, which would make this role's own zero-grant boundary on
// model_manifest/model_active/policy/pricing (the DB-layer half of the
// EC4 no-bypass proof, Step T) meaningless if this service connected as
// root instead.
//
// Also a gRPC CLIENT of control-plane's ControlService (own generated
// copy, internal/pb/control/v1 — CreateRollout/PromoteRollout/
// AbortRollout/GetRolloutStatus, RegisterModelManifest for
// PublishModelVersion) — mirrors provider-gateway's own IssueProviderToken
// client dial exactly: grpc.NewClient with insecure app-layer transport
// credentials, because Cilium's SPIFFE/SPIRE-backed mTLS (control-plane-
// mtls's own admin-api ingress rule, added this same step) enforces
// transport security at the mesh layer, transparently, the same pattern
// every other internal gRPC hop in this project already uses.
//
// admin-api deliberately has NO Vault client and NO etcd client of its
// own (unlike control-plane/data-plane/edge-gateway/provider-gateway) —
// every credential/registry operation goes through control-plane's own
// RPCs. This is intentional, not a gap: it is the network-layer half of
// the Phase-05 plan's EC4 no-bypass design (Decision 3) — admin-api
// simply has no mechanism that COULD reach Vault or etcd directly, even
// if its own RBAC layer (Step F/G) were somehow bypassed.
package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/onezox/OneZox/services/admin-api/internal/apikeys"
	"github.com/onezox/OneZox/services/admin-api/internal/audit"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
	"github.com/onezox/OneZox/services/admin-api/internal/authz"
	"github.com/onezox/OneZox/services/admin-api/internal/graph"
	pb "github.com/onezox/OneZox/services/admin-api/internal/pb/admin/v1"
	controlpb "github.com/onezox/OneZox/services/admin-api/internal/pb/control/v1"
	providerpb "github.com/onezox/OneZox/services/admin-api/internal/pb/provider/v1"
	"github.com/onezox/OneZox/services/admin-api/internal/promclient"
)

const serviceName = "admin-api"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	handlerLog := slog.NewJSONHandler(os.Stdout, nil)
	log := slog.New(handlerLog).With("service", serviceName)

	cockroachHost := envOr("COCKROACH_HOST", "onezox-crdb-public.default.svc.cluster.local")
	cockroachPort := envOr("COCKROACH_PORT", "26257")
	cockroachUser := envOr("COCKROACH_USER", "admin_api")
	dsn := "postgres://" + cockroachUser + "@" + cockroachHost + ":" + cockroachPort + "/defaultdb?sslmode=disable"

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
	// admin_api role (not root) is what's actually connecting, the same
	// verification control-plane's own Step D log line established.
	log.Info("connected to CockroachDB", "host", cockroachHost, "user", cockroachUser)

	controlPlaneAddr := envOr("CONTROL_PLANE_ADDR", "control-plane.default.svc.cluster.local:50051")
	controlConn, err := grpc.NewClient(controlPlaneAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("failed to create control-plane gRPC client", "error", err, "addr", controlPlaneAddr)
		os.Exit(1)
	}
	defer func() { _ = controlConn.Close() }()
	controlClient := controlpb.NewControlServiceClient(controlConn)

	// One-time boot connectivity check (ListModels, read-only, harmless) —
	// "prove the new mesh hop reachable before anything depends on it,"
	// the same discipline every prior mTLS cutover in this project used
	// (e.g. Phase-04 Step L for IssueProviderToken). Logged, not fatal:
	// control-plane may simply not be Ready yet at this exact instant
	// (pod startup ordering, not a configuration problem), and admin-api
	// has its own real RPCs to serve regardless.
	verifyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := controlClient.ListModels(verifyCtx, &controlpb.ListModelsRequest{}); err != nil {
		log.Warn("control-plane not reachable yet at boot", "addr", controlPlaneAddr, "error", err)
	} else {
		log.Info("control-plane reachable", "addr", controlPlaneAddr)
	}
	cancel()

	// Step F: authn.Store is admin_user only (migration 0018's own
	// SELECT-only grant) — structurally disjoint from api_keys, see
	// authn's own package doc for the full reasoning. Both transports
	// below share this one Store instance and Authenticate call, so a
	// gRPC caller and a GraphQL caller are held to the identical check.
	authnStore := authn.NewCockroachStore(db)

	// Step H: the one audit_log writer, shared by authz's own denial-audit
	// path (Step G) and every RPC handler's own success/failure audit
	// (server.go) — one INSERT statement, one place that issues it.
	auditWriter := audit.NewCockroachWriter(db)

	// Step S: api_keys, local to admin-api's own DB grant — never reaches
	// control-plane (admin.proto's own header comment).
	keyStore := apikeys.NewCockroachStore(db)

	// Step U1b: the panel's read-side backends.
	//
	// provider-gateway for ProviderHealth ONLY (the Provider Console) —
	// same insecure app-layer credentials as the control-plane dial
	// above, for the same reason: Cilium's SPIFFE/SPIRE mTLS enforces
	// transport security at the mesh layer (provider-gateway-mtls's own
	// admin-api ingress rule, added this same step).
	providerGatewayAddr := envOr("PROVIDER_GATEWAY_ADDR", "provider-gateway.default.svc.cluster.local:50051")
	providerConn, err := grpc.NewClient(providerGatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("failed to create provider-gateway gRPC client", "error", err, "addr", providerGatewayAddr)
		os.Exit(1)
	}
	defer func() { _ = providerConn.Close() }()
	providerClient := providerpb.NewProviderServiceClient(providerConn)

	// Prometheus for the dashboard's three SLO numbers. Not fatal if
	// unreachable at boot — promclient degrades a failed query to 0 and
	// logs, so the rest of the dashboard still renders (see its own
	// QueryScalar comment for why that degradation is correct here and
	// deliberately NOT correct for the canary SLO gate).
	prometheusAddr := envOr("PROMETHEUS_ADDR", "http://prometheus-server.default.svc.cluster.local")
	metricsClient := promclient.New(prometheusAddr)
	log.Info("read backends configured", "provider_gateway", providerGatewayAddr, "prometheus", prometheusAddr)

	grpcPort := envOr("GRPC_PORT", "50051")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("failed to listen for gRPC", "error", err, "port", grpcPort)
		os.Exit(1)
	}
	// Step G: authz.UnaryInterceptor MUST run after authn's — it only
	// reads the Identity authn already attached to context, it performs
	// no authentication of its own. grpc.ChainUnaryInterceptor preserves
	// the order its arguments are given in.
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		authn.UnaryInterceptor(authnStore, log),
		authz.UnaryInterceptor(auditWriter, log),
	))
	pb.RegisterAdminServiceServer(grpcServer, &server{db: db, control: controlClient, keys: keyStore, audit: auditWriter, log: log})

	go func() {
		log.Info("admin-api gRPC listening", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC server exited", "error", err)
			os.Exit(1)
		}
	}()

	// GraphQL: query-only resolvers (Step D's own split), every one still
	// the gqlgen-generated panic("not implemented") stub — DefaultRecover
	// (gqlgen's own default, wired in by handler.NewDefaultServer) turns
	// that into a clean per-field GraphQL error response, not a crashed
	// process, the same "return Unimplemented cleanly" property the gRPC
	// side gets for free from UnimplementedAdminServiceServer. Real
	// resolver bodies land in their own later steps (H onward).
	//
	// No playground mounted: admin-api's only real client is admin-panel's
	// own RSC server components (Step D), never a human typing ad hoc
	// queries against a security-critical service — live verification
	// uses a raw POST body instead, the same way every other RPC in this
	// project has been verified via grpcurl.
	graphqlSrv := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{
		Keys:      keyStore,
		Control:   controlClient,
		Providers: providerClient,
		Audit:     audit.NewCockroachReader(db),
		Metrics:   metricsClient,
		Log:       log,
	}}))

	mux := http.NewServeMux()
	// Step F: every /graphql request requires a verified admin credential
	// before it ever reaches a resolver — admin.graphql has no anonymous
	// field, so this wrapper is unconditional, not per-resolver.
	mux.Handle("POST /graphql", authn.HTTPMiddleware(authnStore, log, graphqlSrv))
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
	log.Info("admin-api HTTP listening", "port", httpPort)
	if err := http.ListenAndServe(":"+httpPort, mux); err != nil {
		log.Error("HTTP server exited", "error", err)
		os.Exit(1)
	}
}
