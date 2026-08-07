package authz

import "testing"

// TestAllowedTable is the table this step's own instructions asked for
// explicitly: every mutating RPC × {admin: allow, viewer: deny}, plus the
// deny cases that actually prove the guard guards rather than passing
// against a no-op that returns true for everything — an all-allow table
// alone can't distinguish this guard from one that never denies at all.
func TestAllowedTable(t *testing.T) {
	allRPCs := []string{
		methodPublishModelVersion,
		methodStartRollout,
		methodPromoteRollout,
		methodAbortRollout,
		methodCreateApiKey,
		methodRevokeApiKey,
	}

	tests := []struct {
		name   string
		role   string
		method string
		want   bool
	}{
		// --- allow rows: admin may call every one of the six commands ---
		{"admin may PublishModelVersion", roleAdmin, methodPublishModelVersion, true},
		{"admin may StartRollout", roleAdmin, methodStartRollout, true},
		{"admin may PromoteRollout", roleAdmin, methodPromoteRollout, true},
		{"admin may AbortRollout", roleAdmin, methodAbortRollout, true},
		{"admin may CreateApiKey", roleAdmin, methodCreateApiKey, true},
		{"admin may RevokeApiKey", roleAdmin, methodRevokeApiKey, true},

		// --- deny rows: viewer may call NONE of them (the actual proof) ---
		{"viewer denied PublishModelVersion", roleViewer, methodPublishModelVersion, false},
		{"viewer denied StartRollout", roleViewer, methodStartRollout, false},
		{"viewer denied PromoteRollout", roleViewer, methodPromoteRollout, false},
		{"viewer denied AbortRollout", roleViewer, methodAbortRollout, false},
		{"viewer denied CreateApiKey", roleViewer, methodCreateApiKey, false},
		{"viewer denied RevokeApiKey", roleViewer, methodRevokeApiKey, false},

		// --- unknown/empty role: denied on everything, not just some ---
		{"unknown role denied PublishModelVersion", "superadmin", methodPublishModelVersion, false},
		{"empty role denied PublishModelVersion", "", methodPublishModelVersion, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Allowed(tt.role, tt.method)
			if got != tt.want {
				t.Errorf("Allowed(%q, %q) = %v, want %v", tt.role, tt.method, got, tt.want)
			}
		})
	}

	// Belt-and-suspenders sweep: confirm the deny rows above weren't a
	// partial sample — viewer is denied EVERY one of the six methods.
	for _, m := range allRPCs {
		if Allowed(roleViewer, m) {
			t.Errorf("viewer must be denied %q, got allowed", m)
		}
	}
}

// TestUnknownMethodDeniedToEveryRole is the fail-safe-default proof named
// explicitly in this step's own instructions: a method name that isn't in
// admin.proto at all yet (standing in for a future RPC nobody has
// consciously granted) must be denied to EVERY role that exists today,
// admin included — the allow-list's whole point is that a new mutating
// RPC starts DENIED, not implicitly inherited by whichever role already
// has a broad grant.
func TestUnknownMethodDeniedToEveryRole(t *testing.T) {
	future := "/admin.v1.AdminService/SetActiveModelDirectly"

	if Allowed(roleAdmin, future) {
		t.Errorf("an RPC absent from the allow-list must be denied even to admin, got allowed: %q", future)
	}
	if Allowed(roleViewer, future) {
		t.Errorf("an RPC absent from the allow-list must be denied to viewer, got allowed: %q", future)
	}
}
