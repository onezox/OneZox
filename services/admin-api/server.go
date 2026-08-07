package main

import (
	"context"
	"database/sql"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/admin-api/internal/audit"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
	pb "github.com/onezox/OneZox/services/admin-api/internal/pb/admin/v1"
	controlpb "github.com/onezox/OneZox/services/admin-api/internal/pb/control/v1"
)

// controlPublisher is a narrow slice of controlpb.ControlServiceClient —
// only the one method this step's handler actually calls, the same
// pattern provider-gateway's own credentials.TokenFetcher already
// established for calling control-plane (a narrow interface local to the
// consumer, not a dependency on the full 8-method generated client). Kept
// narrow deliberately: a fake implementing this needs exactly one method,
// and the real *grpc.ClientConn-backed controlpb.ControlServiceClient
// (main.go) satisfies it without any adapter. Grows to include
// CreateRollout/PromoteRollout/AbortRollout/GetRolloutStatus as Step L
// gives their own admin-api handlers real bodies.
type controlPublisher interface {
	RegisterModelManifest(ctx context.Context, in *controlpb.RegisterModelManifestRequest, opts ...grpc.CallOption) (*controlpb.RegisterModelManifestResponse, error)
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
	audit   audit.Writer
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

	const action = "publish_model_version"

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

	// A control-plane call failure means NOTHING was published — still
	// worth an audit_log row (Phase-05.txt's own completion checklist:
	// "audit_log captures every admin action," not only successful ones),
	// with after_json left nil since there is no real content to record.
	if err != nil {
		s.log.Error("PublishModelVersion: control-plane call failed", "model_ref", req.GetModelRef(), "user_id", id.UserID, "error", err)
		if auditErr := s.audit.Write(ctx, audit.Entry{
			Actor:  id.UserID,
			Action: action + "_failed",
			Target: req.GetModelRef(),
		}); auditErr != nil {
			s.log.Error("PublishModelVersion: failed to audit a failed publish attempt", "model_ref", req.GetModelRef(), "user_id", id.UserID, "error", auditErr)
		}
		return nil, status.Error(codes.Internal, "failed to publish model version")
	}

	// Success: before_json is nil — publishing a new version never edits
	// or overwrites anything (model_manifest is insert-only), so there is
	// no genuine "before" state for THIS row, matching Step D's own
	// admin.proto header reasoning for why createModelDraft is a query,
	// not a command, in the first place.
	auditErr := s.audit.Write(ctx, audit.Entry{
		Actor:  id.UserID,
		Action: action,
		Target: req.GetModelRef(),
		After: map[string]string{
			"version_id": resp.GetVersionId(),
			"model_ref":  req.GetModelRef(),
			"spec_json":  req.GetSpecJson(),
		},
	})
	if auditErr != nil {
		s.log.Error("PublishModelVersion: manifest published but audit write failed — reporting as failed",
			"model_ref", req.GetModelRef(), "version_id", resp.GetVersionId(), "user_id", id.UserID, "error", auditErr)
		return nil, status.Error(codes.Internal, "publish may have succeeded but could not be recorded; treat as failed and verify with an operator")
	}

	s.log.Info("published model version", "model_ref", req.GetModelRef(), "version_id", resp.GetVersionId(), "user_id", id.UserID)
	return &pb.PublishModelVersionResponse{VersionId: resp.GetVersionId()}, nil
}
