package main

import (
	"context"
	"database/sql"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/admin-api/internal/apikeys"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
	pb "github.com/onezox/OneZox/services/admin-api/internal/pb/admin/v1"
	controlpb "github.com/onezox/OneZox/services/admin-api/internal/pb/control/v1"
)

// controlPublisher is a narrow slice of controlpb.ControlServiceClient —
// only the methods admin-api's own handlers actually call, the same
// pattern provider-gateway's own credentials.TokenFetcher already
// established for calling control-plane (a narrow interface local to the
// consumer, not a dependency on the full 8-method generated client). Kept
// narrow deliberately: a fake implementing this needs exactly these
// methods, and the real *grpc.ClientConn-backed
// controlpb.ControlServiceClient (main.go) satisfies it without any
// adapter. GetRolloutStatus is deliberately NOT here — it's a read, wired
// into the GraphQL side (Step U) when the panel actually needs it, not
// this command-only interface.
type controlPublisher interface {
	RegisterModelManifest(ctx context.Context, in *controlpb.RegisterModelManifestRequest, opts ...grpc.CallOption) (*controlpb.RegisterModelManifestResponse, error)
	CreateRollout(ctx context.Context, in *controlpb.CreateRolloutRequest, opts ...grpc.CallOption) (*controlpb.CreateRolloutResponse, error)
	PromoteRollout(ctx context.Context, in *controlpb.PromoteRolloutRequest, opts ...grpc.CallOption) (*controlpb.PromoteRolloutResponse, error)
	AbortRollout(ctx context.Context, in *controlpb.AbortRolloutRequest, opts ...grpc.CallOption) (*controlpb.AbortRolloutResponse, error)
}

// server implements pb.AdminServiceServer. Every method here runs AFTER
// authn (Step F) and authz (Step G) have already accepted the call — a
// handler never re-checks identity or role, it only reads
// authn.IdentityFromContext for attribution (audit actor, control-plane's
// own created_by field).
type server struct {
	pb.UnimplementedAdminServiceServer
	db      *sql.DB
	control controlPublisher
	keys    apikeys.Store
	log     *slog.Logger
}

// PublishModelVersion — Step H, the first real RPC body. A thin
// passthrough to control-plane's EXISTING RegisterModelManifest (Phase-04,
// unchanged by this step beyond the activation-conditionality fix
// registry.go itself needed — see that file's own updated doc comment):
// this handler signs nothing, verifies nothing, and stores nothing on its
// own. admin-api is the authorized, audited FRONT DOOR to control-plane's
// one signing path, never a second one.
//
// created_by is ALWAYS the authenticated caller's own user_id, never a
// client-supplied field — PublishModelVersionRequest (admin.proto) has no
// such field at all, so there is no parameter to spoof it through even by
// accident.
//
// Ordering (audit.go's own doc comment explains the "only one order is
// possible" reasoning): call control-plane first, THEN write the one
// audit_log row with the real outcome. If the audit write itself fails
// after a real, successful RegisterModelManifest call, this handler
// returns an ERROR to the caller anyway — an unaudited "success" must
// never reach a caller, the explicit fail-loud choice this step's own
// instructions called for. The manifest itself still exists in
// control-plane (immutable, versioned, harmless to have created — it
// simply won't be active for an already-live model_ref, per registry.go's
// own bootstrap-vs-rollout rule), so nothing unsafe happened; the caller
// just correctly cannot be told this was a clean, recorded success.
func (s *server) PublishModelVersion(ctx context.Context, req *pb.PublishModelVersionRequest) (*pb.PublishModelVersionResponse, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		// Unreachable in practice — authn's own interceptor runs first and
		// would already have rejected the call. Fails closed regardless,
		// the same defensive posture authz's interceptor already takes.
		return nil, status.Error(codes.Internal, "no verified identity")
	}

	resp, err := s.control.RegisterModelManifest(ctx, &controlpb.RegisterModelManifestRequest{
		ModelRef: req.GetModelRef(),
		SpecJson: req.GetSpecJson(),
		// created_by carries the ADMIN's own identity through to
		// control-plane's own model_manifest.created_by column — a real,
		// attributable value, not admin-api's own service name, so a
		// manifest's provenance is traceable to the actual operator who
		// authored it even when read directly from control-plane's own
		// storage, independent of admin-api's own audit_log.
		CreatedBy: id.UserID,
	})

	if err != nil {
		s.log.Error("PublishModelVersion: control-plane call failed", "model_ref", req.GetModelRef(), "user_id", id.UserID, "error", err)
		return nil, status.Error(codes.Internal, "failed to publish model version")
	}

	s.log.Info("published model version", "model_ref", req.GetModelRef(), "version_id", resp.GetVersionId(), "user_id", id.UserID)
	return &pb.PublishModelVersionResponse{VersionId: resp.GetVersionId()}, nil
}

// StartRollout/PromoteRollout/AbortRollout — Step L. Same shape as
// PublishModelVersion in every respect that matters: thin passthrough to
// control-plane's own rollout/ module (no state machine logic
// duplicated here), created_by/actor always the authenticated caller
// (never client-supplied), audit_log written for both success and
// control-plane-call failure, and an audit-write failure after a real
// control-plane success still fails the RPC — an unaudited success must
// never reach a caller, the same fail-loud rule Step H established.

func (s *server) StartRollout(ctx context.Context, req *pb.StartRolloutRequest) (*pb.StartRolloutResponse, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no verified identity")
	}

	resp, err := s.control.CreateRollout(ctx, &controlpb.CreateRolloutRequest{
		ModelRef:     req.GetModelRef(),
		VersionId:    req.GetVersionId(),
		StrategyJson: req.GetStrategyJson(),
	})
	if err != nil {
		s.log.Error("StartRollout: control-plane call failed", "model_ref", req.GetModelRef(), "version_id", req.GetVersionId(), "user_id", id.UserID, "error", err)
		return nil, status.Error(codes.Internal, "failed to start rollout")
	}

	s.log.Info("started rollout", "rollout_id", resp.GetRolloutId(), "model_ref", req.GetModelRef(), "version_id", req.GetVersionId(), "user_id", id.UserID)
	return &pb.StartRolloutResponse{RolloutId: resp.GetRolloutId()}, nil
}

func (s *server) PromoteRollout(ctx context.Context, req *pb.PromoteRolloutRequest) (*pb.PromoteRolloutResponse, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no verified identity")
	}

	resp, err := s.control.PromoteRollout(ctx, &controlpb.PromoteRolloutRequest{RolloutId: req.GetRolloutId()})
	if err != nil {
		s.log.Error("PromoteRollout: control-plane call failed", "rollout_id", req.GetRolloutId(), "user_id", id.UserID, "error", err)
		return nil, status.Error(codes.Internal, "failed to promote rollout")
	}

	// The audit row's before_json stays nil deliberately (see the
	// interceptor's own table): the PRIOR stage is already whatever the
	// previous audit_log row for this same rollout_id recorded as its
	// "after", so the full sequence is reconstructable from audit_log
	// alone without a redundant GetRolloutStatus read here.
	s.log.Info("promoted rollout", "rollout_id", req.GetRolloutId(), "new_stage", resp.GetNewStage(), "user_id", id.UserID)
	return &pb.PromoteRolloutResponse{NewStage: resp.GetNewStage()}, nil
}

func (s *server) AbortRollout(ctx context.Context, req *pb.AbortRolloutRequest) (*pb.AbortRolloutResponse, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no verified identity")
	}

	_, err := s.control.AbortRollout(ctx, &controlpb.AbortRolloutRequest{RolloutId: req.GetRolloutId()})
	if err != nil {
		s.log.Error("AbortRollout: control-plane call failed", "rollout_id", req.GetRolloutId(), "user_id", id.UserID, "error", err)
		return nil, status.Error(codes.Internal, "failed to abort rollout")
	}

	s.log.Info("aborted rollout", "rollout_id", req.GetRolloutId(), "user_id", id.UserID)
	return &pb.AbortRolloutResponse{}, nil
}

// CreateApiKey/RevokeApiKey — Step S. Unlike every handler above, these
// never reach control-plane at all (admin.proto's own header comment) —
// api_keys is local to admin-api's own DB grant (migration 0018). Same
// audit shape regardless: success and failure both write a row, and an
// audit-write failure after a real DB mutation still fails the RPC
// (Step H's fail-loud rule, unchanged by which backend the mutation
// landed in).
func (s *server) CreateApiKey(ctx context.Context, req *pb.CreateApiKeyRequest) (*pb.CreateApiKeyResponse, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no verified identity")
	}

	// Step R found this exact branch missing its audit call — the one
	// failure path here with a real, already-resolved actor that skipped
	// it. Under the chokepoint (audit_interceptor.go) that omission is no
	// longer possible: EVERY return from this handler, including this
	// one, is audited by the interceptor wrapping it.
	rawKey, err := apikeys.GenerateRawKey()
	if err != nil {
		s.log.Error("CreateApiKey: failed to generate raw key material", "user_id", id.UserID, "error", err)
		return nil, status.Error(codes.Internal, "failed to create api key")
	}
	hash := apikeys.HashRawKey(rawKey)

	keyID, err := s.keys.Create(ctx, req.GetOrgId(), hash, req.GetScopes())
	if err != nil {
		s.log.Error("CreateApiKey: store call failed", "org_id", req.GetOrgId(), "user_id", id.UserID, "error", err)
		return nil, status.Error(codes.Internal, "failed to create api key")
	}

	s.log.Info("created api key", "key_id", keyID, "org_id", req.GetOrgId(), "user_id", id.UserID)
	return &pb.CreateApiKeyResponse{KeyId: keyID, RawKey: rawKey}, nil
}

func (s *server) RevokeApiKey(ctx context.Context, req *pb.RevokeApiKeyRequest) (*pb.RevokeApiKeyResponse, error) {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "no verified identity")
	}

	found, err := s.keys.Revoke(ctx, req.GetKeyId())
	// found=false (no such key_id, or already revoked) is treated the
	// same as a store error here — both mean "nothing was revoked," and
	// both are still a real admin action worth auditing as a failed
	// attempt, the same uniform failure-is-still-audited shape every
	// other RPC in this file already follows.
	if err != nil || !found {
		if err != nil {
			s.log.Error("RevokeApiKey: store call failed", "key_id", req.GetKeyId(), "user_id", id.UserID, "error", err)
		} else {
			s.log.Warn("RevokeApiKey: no active key found", "key_id", req.GetKeyId(), "user_id", id.UserID)
		}
		return nil, status.Error(codes.Internal, "failed to revoke api key")
	}

	s.log.Info("revoked api key", "key_id", req.GetKeyId(), "user_id", id.UserID)
	return &pb.RevokeApiKeyResponse{}, nil
}
