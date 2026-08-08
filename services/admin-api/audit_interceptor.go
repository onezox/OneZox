package main

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/admin-api/internal/audit"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
	pb "github.com/onezox/OneZox/services/admin-api/internal/pb/admin/v1"
)

// Post-M2 audit fix H5 — the success/failure audit CHOKEPOINT.
//
// Denial audit was already chokepointed (authz.UnaryInterceptor, Step G):
// every gRPC call passes through it before any handler body runs, so a
// newly added RPC is covered automatically. Success and failure audit was
// not — it was 13 hand-written s.audit.Write call sites across six
// handlers. That worked, and Step R verified all six exhaustively by
// reading every exit path, but it was REINTRODUCIBLE: nothing failed at
// compile time if a handler simply omitted the call.
//
// That is not hypothetical. Step R's sweep found exactly such a gap —
// CreateApiKey's GenerateRawKey() failure path returned an error with no
// audit write, the one branch with a real resolved actor that skipped it.
// A chokepoint makes that class of omission impossible rather than
// findable.
//
// WHAT MOVED AND WHAT DID NOT. The interceptor reproduces the handlers'
// previous semantics exactly:
//   - a failed call audits "<action>_failed" with no after_json
//   - a successful call audits "<action>" with the same after_json the
//     handler used to build
//   - an audit-write failure AFTER a real success still FAILS the RPC,
//     because an unaudited success must never reach a caller (Step H's
//     fail-loud rule)
//   - an audit-write failure on an already-failing call is logged only:
//     the caller is being refused either way, so the outcome does not
//     change
//
// WHY A TABLE RATHER THAN REFLECTION. target and after_json are drawn
// from request/response fields whose names differ per RPC (model_ref vs.
// rollout_id vs. org_id vs. key_id). Deriving them by reflection would be
// implicit and would silently produce empty targets for any RPC whose
// field naming did not match the guess. An explicit spec per method is
// the same shape authz.allowedRPCs already uses, and
// TestEveryAdminRPCHasAuditCoverage makes forgetting an entry a TEST
// FAILURE rather than a silent gap.

// auditSpec describes how one mutating RPC is audited.
type auditSpec struct {
	// action is the audit_log.action value on success; failures append
	// "_failed", matching what the handlers previously wrote.
	action string
	// target extracts audit_log.target from the request.
	target func(req any) string
	// after builds after_json from the request and the handler's
	// response. nil means "no after_json for this RPC".
	after func(req any, resp any) any
}

// mutatingMethods is the audit chokepoint's own table: gRPC full-method
// name -> how to audit it. Every method in admin.proto's AdminService is
// a mutation (that file's own header states so), so every one appears
// here, and TestEveryAdminRPCHasAuditCoverage enforces that against the
// generated service descriptor rather than against anyone's memory.
var mutatingMethods = map[string]auditSpec{
	"/admin.v1.AdminService/PublishModelVersion": {
		action: "publish_model_version",
		target: func(r any) string { return r.(*pb.PublishModelVersionRequest).GetModelRef() },
		after: func(r any, s any) any {
			req := r.(*pb.PublishModelVersionRequest)
			return map[string]string{
				"version_id": s.(*pb.PublishModelVersionResponse).GetVersionId(),
				"model_ref":  req.GetModelRef(),
				"spec_json":  req.GetSpecJson(),
			}
		},
	},
	"/admin.v1.AdminService/StartRollout": {
		action: "start_rollout",
		target: func(r any) string { return r.(*pb.StartRolloutRequest).GetModelRef() },
		after: func(r any, s any) any {
			req := r.(*pb.StartRolloutRequest)
			return map[string]string{
				"rollout_id":    s.(*pb.StartRolloutResponse).GetRolloutId(),
				"model_ref":     req.GetModelRef(),
				"version_id":    req.GetVersionId(),
				"strategy_json": req.GetStrategyJson(),
			}
		},
	},
	"/admin.v1.AdminService/PromoteRollout": {
		action: "promote_rollout",
		target: func(r any) string { return r.(*pb.PromoteRolloutRequest).GetRolloutId() },
		after: func(_ any, s any) any {
			return map[string]string{"new_stage": s.(*pb.PromoteRolloutResponse).GetNewStage()}
		},
	},
	"/admin.v1.AdminService/AbortRollout": {
		action: "abort_rollout",
		target: func(r any) string { return r.(*pb.AbortRolloutRequest).GetRolloutId() },
		after:  func(_ any, _ any) any { return map[string]string{"status": "aborted"} },
	},
	"/admin.v1.AdminService/CreateApiKey": {
		action: "create_api_key",
		target: func(r any) string { return r.(*pb.CreateApiKeyRequest).GetOrgId() },
		after: func(r any, s any) any {
			req := r.(*pb.CreateApiKeyRequest)
			// key_id/org_id/scopes ONLY — never raw_key, and not even the
			// hash. audit_log exists to be queried and displayed; a
			// credential with no legitimate read-path use has no business
			// being duplicated into a second table. raw_key is returned to
			// the caller exactly once, in the RPC response, and nowhere
			// else, ever.
			return map[string]any{
				"key_id": s.(*pb.CreateApiKeyResponse).GetKeyId(),
				"org_id": req.GetOrgId(),
				"scopes": req.GetScopes(),
			}
		},
	},
	"/admin.v1.AdminService/RevokeApiKey": {
		action: "revoke_api_key",
		target: func(r any) string { return r.(*pb.RevokeApiKeyRequest).GetKeyId() },
		after:  func(_ any, _ any) any { return map[string]string{"status": "revoked"} },
	},
}

// nonMutatingMethods is deliberately empty: admin.proto's AdminService is
// a command-only surface (reads are GraphQL). It exists so that adding a
// genuine read RPC later is a CONSCIOUS classification — the coverage
// test requires every method to appear in exactly one of the two maps,
// so a new RPC cannot default into "unaudited" by omission.
var nonMutatingMethods = map[string]struct{}{}

// AuditUnaryInterceptor writes the success/failure audit row for every
// mutating RPC. Chained AFTER authn and authz in main.go, so by the time
// it runs the caller is authenticated and authorized; a denial never
// reaches here (authz already audited it and returned).
func AuditUnaryInterceptor(w audit.Writer, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		spec, mutating := mutatingMethods[info.FullMethod]
		if !mutating {
			return handler(ctx, req)
		}

		id, ok := authn.IdentityFromContext(ctx)
		if !ok {
			// Unreachable: authn's interceptor runs first. Fails closed,
			// and writes nothing — audit_log.actor is a NOT NULL foreign
			// key into admin_user, so there is no row to attribute this
			// to, the same reasoning authz's own missing-identity branch
			// already documents.
			return nil, status.Error(codes.Internal, "no verified identity")
		}

		resp, err := handler(ctx, req)

		if err != nil {
			if auditErr := w.Write(ctx, audit.Entry{
				Actor:  id.UserID,
				Action: spec.action + "_failed",
				Target: spec.target(req),
			}); auditErr != nil {
				log.Error("failed to audit a failed admin action",
					"method", info.FullMethod, "user_id", id.UserID, "error", auditErr)
			}
			return nil, err
		}

		entry := audit.Entry{
			Actor:  id.UserID,
			Action: spec.action,
			Target: spec.target(req),
		}
		if spec.after != nil {
			entry.After = spec.after(req, resp)
		}

		if auditErr := w.Write(ctx, entry); auditErr != nil {
			// Fail loud: the action really happened, but an unaudited
			// success must never be reported to a caller as a success.
			log.Error("admin action succeeded but audit write failed — reporting as failed",
				"method", info.FullMethod, "user_id", id.UserID, "error", auditErr)
			return nil, status.Error(codes.Internal,
				"the action may have succeeded but could not be recorded; treat as failed and verify with an operator")
		}

		log.Info("admin action audited", "method", info.FullMethod, "user_id", id.UserID)
		return resp, nil
	}
}

// adminServiceMethods returns every RPC the generated service descriptor
// declares — the contract itself, not a hand-maintained list.
func adminServiceMethods() []string {
	out := make([]string, 0, len(pb.AdminService_ServiceDesc.Methods))
	for _, m := range pb.AdminService_ServiceDesc.Methods {
		out = append(out, "/"+pb.AdminService_ServiceDesc.ServiceName+"/"+m.MethodName)
	}
	return out
}
