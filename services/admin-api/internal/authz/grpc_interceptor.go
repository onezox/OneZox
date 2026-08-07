package authz

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/admin-api/internal/audit"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
)

// UnaryInterceptor must be chained AFTER authn.UnaryInterceptor
// (grpc.ChainUnaryInterceptor(authn.UnaryInterceptor(...),
// authz.UnaryInterceptor(...)) in main.go) — it only ever reads an
// Identity that authn already attached, it never authenticates anything
// itself. If no Identity is present (authn's own interceptor should have
// already rejected the call before this one ever runs), this fails
// closed with PermissionDenied rather than assuming an implicit role —
// defense against a future wiring mistake that reorders the chain, not a
// path this package expects to take in practice. That specific failure
// mode is NOT written to audit_log: there is no real, authn-verified
// actor to attribute it to (audit_log.actor is a foreign key into
// admin_user, migration 0017 — it cannot reference an identity that was
// never resolved), so it stays a structured log line only, the same
// boundary authn's own missing/invalid/revoked cases already sit behind.
//
// A genuine RBAC denial is different: a REAL admin_user was
// authenticated, and audit_log.actor CAN reference them — this step's own
// instructions call this out as security-relevant (a potential escalation
// probe), so it IS written to audit_log, with action carrying a
// "_denied" suffix so a reader of the log can tell a completed mutation
// apart from a refused attempt at a glance. Unlike a successful
// mutation's own audit write (server.go's handlers, Step H) — where a
// failed audit write must fail the whole RPC, because an unaudited
// SUCCESS must never reach a caller — a denial's own correctness doesn't
// depend on the audit write succeeding: the caller is refused either way,
// so an audit-write failure here is logged loudly but does not change the
// PermissionDenied outcome already decided.
func UnaryInterceptor(auditWriter audit.Writer, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id, ok := authn.IdentityFromContext(ctx)
		if !ok {
			log.Error("authz ran with no verified identity on context — check interceptor chain order", "method", info.FullMethod)
			return nil, status.Error(codes.PermissionDenied, "no verified identity")
		}

		if !Allowed(id.Role, info.FullMethod) {
			log.Warn("admin action denied", "method", info.FullMethod, "user_id", id.UserID, "role", id.Role)
			if err := auditWriter.Write(ctx, audit.Entry{
				Actor:  id.UserID,
				Action: info.FullMethod + "_denied",
				Target: info.FullMethod,
			}); err != nil {
				log.Error("failed to audit a denied admin action", "method", info.FullMethod, "user_id", id.UserID, "error", err)
			}
			return nil, status.Error(codes.PermissionDenied, "insufficient privileges for this action")
		}

		return handler(ctx, req)
	}
}
