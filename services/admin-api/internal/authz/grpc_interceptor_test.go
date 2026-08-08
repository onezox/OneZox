package authz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/onezox/OneZox/services/admin-api/internal/audit"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
)

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func handlerCtx() context.Context {
	return authn.WithIdentity(context.Background(), &authn.Identity{UserID: "u1", OrgID: "o1", Role: roleViewer})
}

// TestInterceptorDeniesAndAuditsEveryMethod is Step R's own chokepoint
// proof: this interceptor is what makes "all admin actions audited"
// structural rather than hand-wired — every gRPC call, for every method
// admin.proto will ever declare, passes through it before any handler
// body runs (grpc.ChainUnaryInterceptor wraps the whole server, not a
// per-method opt-in). Table-driven over the SAME six methods
// TestAllowedTable already covers, not one representative sample —
// Step R's own "exhaustive, not sampled" standard applies to the
// interceptor's actual audit-write behavior, not just the pure
// Allowed() boolean authz_test.go already proves.
func TestInterceptorDeniesAndAuditsEveryMethod(t *testing.T) {
	methods := []string{
		methodPublishModelVersion,
		methodStartRollout,
		methodPromoteRollout,
		methodAbortRollout,
		methodCreateApiKey,
		methodRevokeApiKey,
	}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			auditW := audit.NewFakeWriter()
			interceptor := UnaryInterceptor(auditW, testLog())
			handlerCalled := false
			handler := func(ctx context.Context, req any) (any, error) {
				handlerCalled = true
				return "should never run", nil
			}

			// roleViewer has an empty allow-list (authz.go) — every one
			// of these six methods must be denied to it.
			_, err := interceptor(handlerCtx(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)

			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("err = %v, want PermissionDenied", err)
			}
			if handlerCalled {
				t.Fatal("handler was called despite denial — the chokepoint let a mutation through")
			}
			if len(auditW.Entries) != 1 {
				t.Fatalf("got %d audit entries, want exactly 1", len(auditW.Entries))
			}
			e := auditW.Entries[0]
			if e.Actor != "u1" {
				t.Errorf("actor = %q, want u1", e.Actor)
			}
			if e.Action != method+"_denied" {
				t.Errorf("action = %q, want %q", e.Action, method+"_denied")
			}
			if e.Target != method {
				t.Errorf("target = %q, want %q", e.Target, method)
			}
		})
	}
}

// TestInterceptorAllowsAdminThrough — the positive control: an admin
// role reaching a real method must pass through to the handler, and the
// interceptor itself must NOT write an audit entry for an allowed call
// (that is the RPC handler's own job, server.go — proven separately by
// server_test.go's own success-path tests). Conflating the two would
// double-audit every successful mutation.
func TestInterceptorAllowsAdminThrough(t *testing.T) {
	auditW := audit.NewFakeWriter()
	interceptor := UnaryInterceptor(auditW, testLog())
	ctx := authn.WithIdentity(context.Background(), &authn.Identity{UserID: "u2", OrgID: "o1", Role: roleAdmin})
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: methodPublishModelVersion}, handler)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want the handler's own response passed through unchanged", resp)
	}
	if !handlerCalled {
		t.Fatal("handler was never called despite an allowed role")
	}
	if len(auditW.Entries) != 0 {
		t.Errorf("got %d audit entries from the interceptor itself, want 0 (that's the handler's job)", len(auditW.Entries))
	}
}

// TestInterceptorDenialOutcomeSurvivesAuditWriteFailure — the
// interceptor's own doc comment claims a denial's correctness doesn't
// depend on the audit write succeeding (unlike a success-path audit
// failure, which server.go's handlers DO fail the RPC over). Proves
// that claim rather than trusting the comment.
func TestInterceptorDenialOutcomeSurvivesAuditWriteFailure(t *testing.T) {
	auditW := audit.NewFakeWriter()
	auditW.Err = errors.New("audit_log insert failed")
	interceptor := UnaryInterceptor(auditW, testLog())
	handler := func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler was called despite denial")
		return nil, nil
	}

	_, err := interceptor(handlerCtx(), nil, &grpc.UnaryServerInfo{FullMethod: methodAbortRollout}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("err = %v, want PermissionDenied even though the audit write itself failed", err)
	}
}

// TestInterceptorMissingIdentityFailsClosedWithoutAudit — defense
// against a future interceptor-chain reordering mistake (this package's
// own doc comment). No real, authn-verified actor exists to attribute a
// row to (audit_log.actor is a NOT NULL foreign key into admin_user),
// so this is the one denial path that does NOT audit — a structural
// impossibility, not an oversight, and distinct from CreateApiKey's own
// GenerateRawKey fix (Step R): that path DOES have a real actor already
// resolved, this one does not.
func TestInterceptorMissingIdentityFailsClosedWithoutAudit(t *testing.T) {
	auditW := audit.NewFakeWriter()
	interceptor := UnaryInterceptor(auditW, testLog())
	handler := func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler was called with no verified identity on context")
		return nil, nil
	}

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: methodPublishModelVersion}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("err = %v, want PermissionDenied", err)
	}
	if len(auditW.Entries) != 0 {
		t.Errorf("got %d audit entries, want 0 — no real identity to attribute one to", len(auditW.Entries))
	}
}
