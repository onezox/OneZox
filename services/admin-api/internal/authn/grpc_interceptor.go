package authn

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metadataKey = "authorization"

// UnaryInterceptor extracts "authorization: bearer <token>" from incoming
// gRPC metadata (never a request message field — Step D's admin.proto
// header already establishes identity as a transport-level concern, the
// same separation Vault's own Kubernetes-auth login uses), authenticates
// it, and attaches the resulting Identity to the context every RPC
// handler receives. Every failure mode collapses to the same
// codes.Unauthenticated (no oracle for which check failed, mirroring
// AuthError's own doc comment) — the raw token itself is never logged,
// only whether authentication succeeded.
func UnaryInterceptor(store Store, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		token := bearerToken(ctx)

		id, err := Authenticate(ctx, store, token)
		if err != nil {
			logAuthFailure(log, info.FullMethod, err)
			return nil, status.Error(codes.Unauthenticated, "invalid or missing admin credential")
		}

		return handler(WithIdentity(ctx, id), req)
	}
}

func bearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(metadataKey)
	if len(values) == 0 {
		return ""
	}
	const prefix = "bearer "
	v := values[0]
	if len(v) > len(prefix) && strings.EqualFold(v[:len(prefix)], prefix) {
		return v[len(prefix):]
	}
	return ""
}

// logAuthFailure logs the OUTCOME (which AuthError variant, which RPC),
// never the token itself — same "credential never appears in logs or
// traces" rule Phase-01 Step H1 positive-controlled for tenant auth,
// carried here.
func logAuthFailure(log *slog.Logger, method string, err error) {
	switch {
	case errors.Is(err, ErrMissingCredential):
		log.Warn("admin auth failed", "method", method, "reason", "missing_credential")
	case errors.Is(err, ErrRevoked):
		log.Warn("admin auth failed", "method", method, "reason", "revoked")
	case errors.Is(err, ErrInvalidCredential):
		log.Warn("admin auth failed", "method", method, "reason", "invalid_credential")
	default:
		log.Error("admin auth failed", "method", method, "reason", "store_error", "error", err)
	}
}
