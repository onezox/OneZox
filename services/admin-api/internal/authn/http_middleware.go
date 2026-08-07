package authn

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

// HTTPMiddleware is the GraphQL-side counterpart to UnaryInterceptor —
// same extraction, same Authenticate call, same context attachment, same
// "collapse every failure to one generic response" posture. Every
// /graphql request requires a verified admin credential; there is no
// anonymous/public query in this schema (Step D's own admin.graphql has
// no field that doesn't assume an authenticated admin_user), so this
// wrapper rejects before the request ever reaches gqlgen's handler at
// all, rather than leaving individual resolvers to each remember to check.
func HTTPMiddleware(store Store, log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerTokenFromHeader(r.Header.Get("Authorization"))

		id, err := Authenticate(r.Context(), store, token)
		if err != nil {
			logAuthFailure(log, r.URL.Path, err)
			writeUnauthorized(w)
			return
		}

		next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
	})
}

func bearerTokenFromHeader(v string) string {
	const prefix = "bearer "
	if len(v) > len(prefix) && strings.EqualFold(v[:len(prefix)], prefix) {
		return v[len(prefix):]
	}
	return ""
}

// Same generic body regardless of which check failed — mirrors edge-
// gateway's own unauthorized() response shape (Phase-01 Step C): no
// oracle for a caller probing with different bad tokens.
func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"errors": []map[string]string{{"message": "invalid or missing admin credential"}},
	})
}
