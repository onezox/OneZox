package main

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
	pb "github.com/onezox/OneZox/services/admin-api/internal/pb/admin/v1"
)

func interceptorLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func identityCtx() context.Context {
	return authn.WithIdentity(context.Background(), &authn.Identity{UserID: "u1", OrgID: "o1", Role: "admin"})
}

// TestEveryAdminRPCHasAuditCoverage is the STRUCTURAL guarantee this whole
// file exists for, and the reason the audit chokepoint is not merely a
// tidier version of the 13 hand-wired calls it replaced.
//
// It enumerates the RPCs from the GENERATED SERVICE DESCRIPTOR — the
// contract itself — and requires each to be classified in exactly one of
// mutatingMethods or nonMutatingMethods. Adding an RPC to admin.proto
// without deciding how it is audited FAILS THIS TEST. That converts audit
// coverage from something a human has to sweep for (Step R did exactly
// that, by hand, and found a real gap) into something the build enforces.
func TestEveryAdminRPCHasAuditCoverage(t *testing.T) {
	methods := adminServiceMethods()
	if len(methods) == 0 {
		t.Fatal("service descriptor reported zero methods — the test would be vacuous")
	}

	for _, m := range methods {
		_, mutating := mutatingMethods[m]
		_, exempt := nonMutatingMethods[m]

		switch {
		case mutating && exempt:
			t.Errorf("%s is in BOTH mutatingMethods and nonMutatingMethods — classify it once", m)
		case !mutating && !exempt:
			t.Errorf("%s has NO audit classification.\n"+
				"  Every RPC on AdminService must be listed in either mutatingMethods "+
				"(audited by AuditUnaryInterceptor) or nonMutatingMethods (deliberately not audited).\n"+
				"  A new mutating RPC must never default into being unaudited by omission.", m)
		}
	}

	// Guard the other direction too: an entry left behind for an RPC that
	// no longer exists would silently rot.
	live := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		live[m] = struct{}{}
	}
	for m := range mutatingMethods {
		if _, ok := live[m]; !ok {
			t.Errorf("mutatingMethods has an entry for %s, which the service descriptor no longer declares", m)
		}
	}
}

// TestAuditSpecsProduceUsableEntries makes sure each spec's extractors
// actually work against its own request/response types. Without this a
// wrong type assertion would only surface as a panic in production.
func TestAuditSpecsProduceUsableEntries(t *testing.T) {
	cases := []struct {
		method     string
		req, resp  any
		wantAction string
		wantTarget string
	}{
		{"/admin.v1.AdminService/PublishModelVersion",
			&pb.PublishModelVersionRequest{ModelRef: "openai", SpecJson: `{}`},
			&pb.PublishModelVersionResponse{VersionId: "v-1"}, "publish_model_version", "openai"},
		{"/admin.v1.AdminService/StartRollout",
			&pb.StartRolloutRequest{ModelRef: "openai", VersionId: "v-1", StrategyJson: `{}`},
			&pb.StartRolloutResponse{RolloutId: "r-1"}, "start_rollout", "openai"},
		{"/admin.v1.AdminService/PromoteRollout",
			&pb.PromoteRolloutRequest{RolloutId: "r-1"},
			&pb.PromoteRolloutResponse{NewStage: "canary_10"}, "promote_rollout", "r-1"},
		{"/admin.v1.AdminService/AbortRollout",
			&pb.AbortRolloutRequest{RolloutId: "r-1"},
			&pb.AbortRolloutResponse{}, "abort_rollout", "r-1"},
		{"/admin.v1.AdminService/CreateApiKey",
			&pb.CreateApiKeyRequest{OrgId: "org-1", Scopes: []string{"chat.completions"}},
			&pb.CreateApiKeyResponse{KeyId: "k-1", RawKey: "oz_SECRET_VALUE"}, "create_api_key", "org-1"},
		{"/admin.v1.AdminService/RevokeApiKey",
			&pb.RevokeApiKeyRequest{KeyId: "k-1"},
			&pb.RevokeApiKeyResponse{}, "revoke_api_key", "k-1"},
	}

	if len(cases) != len(mutatingMethods) {
		t.Fatalf("this test covers %d methods but mutatingMethods has %d — add the missing case", len(cases), len(mutatingMethods))
	}

	for _, c := range cases {
		spec, ok := mutatingMethods[c.method]
		if !ok {
			t.Fatalf("%s missing from mutatingMethods", c.method)
		}
		if spec.action != c.wantAction {
			t.Errorf("%s action = %q, want %q", c.method, spec.action, c.wantAction)
		}
		if got := spec.target(c.req); got != c.wantTarget {
			t.Errorf("%s target = %q, want %q", c.method, got, c.wantTarget)
		}
		if spec.after != nil {
			if spec.after(c.req, c.resp) == nil {
				t.Errorf("%s after() returned nil despite being non-nil spec", c.method)
			}
		}
	}
}

// TestCreateApiKeyAuditNeverCarriesTheRawKey — the raw key is returned to
// the caller exactly once, in the RPC response, and must never be
// duplicated into audit_log.
func TestCreateApiKeyAuditNeverCarriesTheRawKey(t *testing.T) {
	spec := mutatingMethods["/admin.v1.AdminService/CreateApiKey"]
	after := spec.after(
		&pb.CreateApiKeyRequest{OrgId: "org-1", Scopes: []string{"chat.completions"}},
		&pb.CreateApiKeyResponse{KeyId: "k-1", RawKey: "oz_SECRET_VALUE"},
	)
	m, ok := after.(map[string]any)
	if !ok {
		t.Fatalf("after = %#v, want map[string]any", after)
	}
	for k, v := range m {
		if s, isStr := v.(string); isStr && s == "oz_SECRET_VALUE" {
			t.Fatalf("audit after_json key %q carries the RAW KEY", k)
		}
	}
	if _, present := m["raw_key"]; present {
		t.Error("after_json has a raw_key field")
	}
	if _, present := m["hash"]; present {
		t.Error("after_json has a hash field")
	}
}

func runThrough(t *testing.T, w audit.Writer, method string, req any, handler grpc.UnaryHandler) (any, error) {
	t.Helper()
	return AuditUnaryInterceptor(w, interceptorLog())(
		identityCtx(), req, &grpc.UnaryServerInfo{FullMethod: method}, handler)
}

// TestInterceptorAuditsSuccessForEveryMutatingRPC — the coverage claim,
// exercised rather than asserted: drive each mutating method through the
// interceptor and confirm exactly one audit row results, with NO
// per-handler audit call anywhere.
func TestInterceptorAuditsSuccessForEveryMutatingRPC(t *testing.T) {
	responses := map[string]any{
		"/admin.v1.AdminService/PublishModelVersion": &pb.PublishModelVersionResponse{VersionId: "v-1"},
		"/admin.v1.AdminService/StartRollout":        &pb.StartRolloutResponse{RolloutId: "r-1"},
		"/admin.v1.AdminService/PromoteRollout":      &pb.PromoteRolloutResponse{NewStage: "canary_10"},
		"/admin.v1.AdminService/AbortRollout":        &pb.AbortRolloutResponse{},
		"/admin.v1.AdminService/CreateApiKey":        &pb.CreateApiKeyResponse{KeyId: "k-1", RawKey: "oz_x"},
		"/admin.v1.AdminService/RevokeApiKey":        &pb.RevokeApiKeyResponse{},
	}
	requests := map[string]any{
		"/admin.v1.AdminService/PublishModelVersion": &pb.PublishModelVersionRequest{ModelRef: "openai"},
		"/admin.v1.AdminService/StartRollout":        &pb.StartRolloutRequest{ModelRef: "openai"},
		"/admin.v1.AdminService/PromoteRollout":      &pb.PromoteRolloutRequest{RolloutId: "r-1"},
		"/admin.v1.AdminService/AbortRollout":        &pb.AbortRolloutRequest{RolloutId: "r-1"},
		"/admin.v1.AdminService/CreateApiKey":        &pb.CreateApiKeyRequest{OrgId: "org-1"},
		"/admin.v1.AdminService/RevokeApiKey":        &pb.RevokeApiKeyRequest{KeyId: "k-1"},
	}

	for method, spec := range mutatingMethods {
		t.Run(method, func(t *testing.T) {
			w := audit.NewFakeWriter()
			_, err := runThrough(t, w, method, requests[method], func(context.Context, any) (any, error) {
				return responses[method], nil
			})
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if len(w.Entries) != 1 {
				t.Fatalf("got %d audit entries, want exactly 1", len(w.Entries))
			}
			e := w.Entries[0]
			if e.Actor != "u1" || e.Action != spec.action {
				t.Errorf("entry = %+v, want actor u1 action %s", e, spec.action)
			}
		})
	}
}

func TestInterceptorAuditsFailureWithFailedSuffix(t *testing.T) {
	w := audit.NewFakeWriter()
	_, err := runThrough(t, w, "/admin.v1.AdminService/StartRollout",
		&pb.StartRolloutRequest{ModelRef: "openai"},
		func(context.Context, any) (any, error) {
			return nil, status.Error(codes.Internal, "control-plane down")
		})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want Internal", err)
	}
	if len(w.Entries) != 1 || w.Entries[0].Action != "start_rollout_failed" {
		t.Fatalf("entries = %+v, want one start_rollout_failed", w.Entries)
	}
	if w.Entries[0].After != nil {
		t.Error("failed attempt carries after_json, want nil")
	}
}

// TestInterceptorFailsLoudWhenAuditWriteFailsAfterSuccess preserves Step
// H's rule through the refactor: an unaudited SUCCESS must never reach a
// caller.
func TestInterceptorFailsLoudWhenAuditWriteFailsAfterSuccess(t *testing.T) {
	w := audit.NewFakeWriter()
	w.Err = errors.New("audit_log insert failed")
	handlerRan := false

	_, err := runThrough(t, w, "/admin.v1.AdminService/PublishModelVersion",
		&pb.PublishModelVersionRequest{ModelRef: "openai"},
		func(context.Context, any) (any, error) {
			handlerRan = true
			return &pb.PublishModelVersionResponse{VersionId: "v-1"}, nil
		})

	if !handlerRan {
		t.Fatal("handler never ran")
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want Internal — an unaudited success must not be reported as success", err)
	}
}

// ...but a failing call whose audit ALSO fails keeps its original error:
// the caller is refused either way, so the outcome does not change.
func TestInterceptorKeepsHandlerErrorWhenFailureAuditAlsoFails(t *testing.T) {
	w := audit.NewFakeWriter()
	w.Err = errors.New("audit_log insert failed")
	sentinel := status.Error(codes.FailedPrecondition, "rollout is not running")

	_, err := runThrough(t, w, "/admin.v1.AdminService/PromoteRollout",
		&pb.PromoteRolloutRequest{RolloutId: "r-1"},
		func(context.Context, any) (any, error) { return nil, sentinel })

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v, want the handler's original FailedPrecondition", err)
	}
}

// TestInterceptorIgnoresNonMutatingMethods — an unclassified method must
// pass straight through without an audit row, so the interceptor cannot
// invent entries for RPCs it does not own.
func TestInterceptorIgnoresNonMutatingMethods(t *testing.T) {
	w := audit.NewFakeWriter()
	ran := false
	_, err := runThrough(t, w, "/admin.v1.AdminService/SomeFutureReadRPC", nil,
		func(context.Context, any) (any, error) { ran = true; return "ok", nil })
	if err != nil || !ran {
		t.Fatalf("err = %v, handlerRan = %v", err, ran)
	}
	if len(w.Entries) != 0 {
		t.Fatalf("got %d audit entries for an unclassified method, want 0", len(w.Entries))
	}
}

// TestInterceptorFailsClosedWithoutIdentity — defensive; authn runs first
// in the chain, so this is unreachable in practice.
func TestInterceptorFailsClosedWithoutIdentity(t *testing.T) {
	w := audit.NewFakeWriter()
	_, err := AuditUnaryInterceptor(w, interceptorLog())(
		context.Background(),
		&pb.AbortRolloutRequest{RolloutId: "r-1"},
		&grpc.UnaryServerInfo{FullMethod: "/admin.v1.AdminService/AbortRollout"},
		func(context.Context, any) (any, error) { return &pb.AbortRolloutResponse{}, nil })

	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want Internal", err)
	}
	if len(w.Entries) != 0 {
		t.Errorf("wrote %d audit entries with no verified identity, want 0 — audit_log.actor is a NOT NULL FK", len(w.Entries))
	}
}
