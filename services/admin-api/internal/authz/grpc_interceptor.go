package authz

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
// path this package expects to take in practice.
//
// Every denial is logged with the acting identity and the method it was
// denied — this is what makes Step R's audit-coverage sweep able to
// confirm a denied mutating attempt still produces an audit_log row
// (that write happens at the RPC handler layer, Step H onward; this
// interceptor's own log line is the operational trace, not the audit
// record itself).
func UnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id, ok := authn.IdentityFromContext(ctx)
		if !ok {
			log.Error("authz ran with no verified identity on context — check interceptor chain order", "method", info.FullMethod)
			return nil, status.Error(codes.PermissionDenied, "no verified identity")
		}

		if !Allowed(id.Role, info.FullMethod) {
			log.Warn("admin action denied", "method", info.FullMethod, "user_id", id.UserID, "role", id.Role)
			return nil, status.Error(codes.PermissionDenied, "insufficient privileges for this action")
		}

		return handler(ctx, req)
	}
}
