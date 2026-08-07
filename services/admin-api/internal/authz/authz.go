// Package authz is AUTHORIZATION — Step G, the layer Step F's authn
// deliberately left empty. authn answers "who is this"; this package
// answers "may this role call this RPC," and nothing here ever re-derives
// or re-checks identity itself — it only reads the Identity authn already
// verified and attached to the context (authn.IdentityFromContext).
//
// ALLOW-LIST, not deny-list — a fail-safe default-deny posture, the
// explicit design requirement for this step. allowedRPCs maps a role to
// the set of gRPC full-method names it may call; any method not present
// under a role's entry is denied to that role, including a brand-new RPC
// added to admin.proto in some future step that nobody has yet added
// here — it is denied to EVERY role, admin included, until someone
// consciously adds it. A deny-list would have the opposite, dangerous
// default: a new mutating RPC would be silently callable by anyone until
// someone remembered to add it to a blocklist. TestUnknownMethodDeniedToEveryRole
// proves this directly, not just the two roles this phase actually has.
//
// GraphQL needs no authz layer this phase: admin.graphql (Step D) is
// query-only, and every query is open to both viewer and admin — the
// Phase-05 plan's own blast-radius table draws the whole role split at
// "may this identity MUTATE anything," and every mutation is one of the
// six gRPC commands below. authn's own HTTPMiddleware (any authenticated
// admin_user, either role) is already the complete gate GraphQL needs.
package authz

const (
	roleViewer = "viewer"
	roleAdmin  = "admin"
)

// Every method name here is the gRPC full-method string
// (grpc.UnaryServerInfo.FullMethod), matching admin.proto's own six
// commands (Step D) exactly — no RPC that mutates a live model outside
// signed manifests + rollout exists in that contract at all (EC4's own
// contract-shape enforcement), so this table cannot grant a bypass verb
// that was never defined in the first place.
const (
	methodPublishModelVersion = "/admin.v1.AdminService/PublishModelVersion"
	methodStartRollout        = "/admin.v1.AdminService/StartRollout"
	methodPromoteRollout      = "/admin.v1.AdminService/PromoteRollout"
	methodAbortRollout        = "/admin.v1.AdminService/AbortRollout"
	methodCreateApiKey        = "/admin.v1.AdminService/CreateApiKey"
	methodRevokeApiKey        = "/admin.v1.AdminService/RevokeApiKey"
)

// allowedRPCs is the ONE table this whole package is built around. Every
// role Phase-05 defines has its own explicit entry — viewer's is
// deliberately empty (not merely absent) so the "viewer may mutate
// nothing" property reads as an intentional statement in this file, not
// something a reader has to infer from the role simply never appearing.
var allowedRPCs = map[string]map[string]bool{
	roleAdmin: {
		methodPublishModelVersion: true,
		methodStartRollout:        true,
		methodPromoteRollout:      true,
		methodAbortRollout:        true,
		methodCreateApiKey:        true,
		methodRevokeApiKey:        true,
	},
	roleViewer: {},
}

// Allowed is pure, total, and safe against every input this could ever
// receive: an unknown role (map miss) or an unrecognized method (nested
// map miss, or missing from a known role's set) both resolve to Go's zero
// value for bool — false — through ordinary nil-map-read semantics, no
// panic, no special-case branch required to make the default deny real.
func Allowed(role, fullMethod string) bool {
	return allowedRPCs[role][fullMethod]
}
