package authn

import "context"

type identityKey struct{}

// WithIdentity/IdentityFromContext are the ONLY way an Identity crosses
// from the transport layer (grpc_interceptor.go, http_middleware.go) into
// a handler/resolver. Handlers never re-derive identity from a raw token
// themselves — this is what guarantees authz (Step G) and audit (Step H
// onward) always see the exact identity authn actually verified, not a
// second, possibly-different lookup.
func WithIdentity(ctx context.Context, id *Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFromContext returns (nil, false) if no identity was ever
// attached — a handler that forgets to check this fails closed (a nil
// Identity has an empty Role, which Step G's RBAC table treats as
// unknown/denied, never as an implicit admin).
func IdentityFromContext(ctx context.Context) (*Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(*Identity)
	return id, ok && id != nil
}
