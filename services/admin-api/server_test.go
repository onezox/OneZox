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

	"github.com/onezox/OneZox/services/admin-api/internal/apikeys"
	"github.com/onezox/OneZox/services/admin-api/internal/authn"
	pb "github.com/onezox/OneZox/services/admin-api/internal/pb/admin/v1"
	controlpb "github.com/onezox/OneZox/services/admin-api/internal/pb/control/v1"
)

// fakeControlPublisher implements the narrow controlPublisher interface —
// no dependency on the full generated gRPC client, matching provider-
// gateway's own credentials.FakeFetcher precedent. One err/response pair
// per method so each RPC's own failure mode can be tested independently.
type fakeControlPublisher struct {
	versionID string
	err       error
	lastReq   *controlpb.RegisterModelManifestRequest

	rolloutID      string
	createErr      error
	lastCreateReq  *controlpb.CreateRolloutRequest
	newStage       string
	promoteErr     error
	lastPromoteReq *controlpb.PromoteRolloutRequest
	abortErr       error
	lastAbortReq   *controlpb.AbortRolloutRequest
}

func (f *fakeControlPublisher) RegisterModelManifest(ctx context.Context, in *controlpb.RegisterModelManifestRequest, opts ...grpc.CallOption) (*controlpb.RegisterModelManifestResponse, error) {
	f.lastReq = in
	if f.err != nil {
		return nil, f.err
	}
	return &controlpb.RegisterModelManifestResponse{VersionId: f.versionID}, nil
}

func (f *fakeControlPublisher) CreateRollout(ctx context.Context, in *controlpb.CreateRolloutRequest, opts ...grpc.CallOption) (*controlpb.CreateRolloutResponse, error) {
	f.lastCreateReq = in
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &controlpb.CreateRolloutResponse{RolloutId: f.rolloutID}, nil
}

func (f *fakeControlPublisher) PromoteRollout(ctx context.Context, in *controlpb.PromoteRolloutRequest, opts ...grpc.CallOption) (*controlpb.PromoteRolloutResponse, error) {
	f.lastPromoteReq = in
	if f.promoteErr != nil {
		return nil, f.promoteErr
	}
	return &controlpb.PromoteRolloutResponse{NewStage: f.newStage}, nil
}

func (f *fakeControlPublisher) AbortRollout(ctx context.Context, in *controlpb.AbortRolloutRequest, opts ...grpc.CallOption) (*controlpb.AbortRolloutResponse, error) {
	f.lastAbortReq = in
	if f.abortErr != nil {
		return nil, f.abortErr
	}
	return &controlpb.AbortRolloutResponse{}, nil
}

// Audit fix H5 — WHAT THIS FILE TESTS, AND WHAT IT NO LONGER DOES.
//
// These tests exercise HANDLER behaviour: request mapping (including that
// created_by is the authenticated caller and never client-supplied),
// response mapping, error codes, and control-plane / store interaction.
//
// They deliberately assert nothing about audit_log any more. Audit is no
// longer a handler concern at all — it is enforced by
// AuditUnaryInterceptor wrapping every mutating RPC, and the server
// struct no longer even holds an audit.Writer. Coverage, the
// success/failure distinction, fail-loud-on-audit-failure, and the
// structural guarantee that a NEW RPC cannot escape auditing all live in
// audit_interceptor_test.go, tested once at the layer that owns them
// instead of re-asserted six times here.
func testServer(control controlPublisher) *server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &server{control: control, keys: apikeys.NewFakeStore(), log: log}
}

func testServerWithKeys(keys apikeys.Store) *server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &server{keys: keys, log: log}
}

func adminCtx() context.Context {
	return authn.WithIdentity(context.Background(), &authn.Identity{UserID: "u1", OrgID: "o1", Role: "admin"})
}

func TestPublishModelVersionSuccess(t *testing.T) {
	control := &fakeControlPublisher{versionID: "v-123"}
	s := testServer(control)

	resp, err := s.PublishModelVersion(adminCtx(), &pb.PublishModelVersionRequest{
		ModelRef: "openai", SpecJson: `{"worker_ref":"openai:gpt-4o-mini"}`,
	})
	if err != nil {
		t.Fatalf("PublishModelVersion: %v", err)
	}
	if resp.GetVersionId() != "v-123" {
		t.Errorf("version_id = %q, want v-123", resp.GetVersionId())
	}
	// created_by must be the AUTHENTICATED caller's user_id, never a
	// client-supplied value (the request message has no such field at all).
	if control.lastReq.GetCreatedBy() != "u1" {
		t.Errorf("created_by sent to control-plane = %q, want u1", control.lastReq.GetCreatedBy())
	}
	if control.lastReq.GetSpecJson() != `{"worker_ref":"openai:gpt-4o-mini"}` {
		t.Errorf("spec_json forwarded = %q, want it passed through byte-for-byte", control.lastReq.GetSpecJson())
	}
}

func TestPublishModelVersionControlPlaneFailureReturnsInternal(t *testing.T) {
	control := &fakeControlPublisher{err: errors.New("control-plane unreachable")}
	s := testServer(control)

	_, err := s.PublishModelVersion(adminCtx(), &pb.PublishModelVersionRequest{ModelRef: "openai", SpecJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

func TestPublishModelVersionNoIdentityFailsClosed(t *testing.T) {
	control := &fakeControlPublisher{versionID: "v-789"}
	s := testServer(control)

	_, err := s.PublishModelVersion(context.Background(), &pb.PublishModelVersionRequest{ModelRef: "openai", SpecJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal (no identity on context)", err)
	}
	if control.lastReq != nil {
		t.Error("control-plane was called despite no verified identity — must fail before any downstream call")
	}
}

func TestStartRolloutSuccess(t *testing.T) {
	control := &fakeControlPublisher{rolloutID: "r-1"}
	s := testServer(control)

	resp, err := s.StartRollout(adminCtx(), &pb.StartRolloutRequest{
		ModelRef: "openai", VersionId: "v-2", StrategyJson: `{}`,
	})
	if err != nil {
		t.Fatalf("StartRollout: %v", err)
	}
	if resp.GetRolloutId() != "r-1" {
		t.Errorf("rollout_id = %q, want r-1", resp.GetRolloutId())
	}
	if control.lastCreateReq.GetModelRef() != "openai" || control.lastCreateReq.GetVersionId() != "v-2" {
		t.Errorf("unexpected CreateRollout request: %+v", control.lastCreateReq)
	}
}

func TestStartRolloutControlPlaneFailureReturnsInternal(t *testing.T) {
	control := &fakeControlPublisher{createErr: errors.New("model_ref already has a running rollout")}
	s := testServer(control)

	_, err := s.StartRollout(adminCtx(), &pb.StartRolloutRequest{ModelRef: "openai", VersionId: "v-2", StrategyJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

func TestPromoteRolloutSuccess(t *testing.T) {
	control := &fakeControlPublisher{newStage: "canary_10"}
	s := testServer(control)

	resp, err := s.PromoteRollout(adminCtx(), &pb.PromoteRolloutRequest{RolloutId: "r-1"})
	if err != nil {
		t.Fatalf("PromoteRollout: %v", err)
	}
	if resp.GetNewStage() != "canary_10" {
		t.Errorf("new_stage = %q, want canary_10", resp.GetNewStage())
	}
	if control.lastPromoteReq.GetRolloutId() != "r-1" {
		t.Errorf("unexpected PromoteRollout request: %+v", control.lastPromoteReq)
	}
}

func TestPromoteRolloutControlPlaneFailureReturnsInternal(t *testing.T) {
	control := &fakeControlPublisher{promoteErr: errors.New("rollout is not running")}
	s := testServer(control)

	if _, err := s.PromoteRollout(adminCtx(), &pb.PromoteRolloutRequest{RolloutId: "r-1"}); status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

func TestAbortRolloutSuccess(t *testing.T) {
	control := &fakeControlPublisher{}
	s := testServer(control)

	if _, err := s.AbortRollout(adminCtx(), &pb.AbortRolloutRequest{RolloutId: "r-1"}); err != nil {
		t.Fatalf("AbortRollout: %v", err)
	}
	if control.lastAbortReq.GetRolloutId() != "r-1" {
		t.Errorf("unexpected AbortRollout request: %+v", control.lastAbortReq)
	}
}

func TestAbortRolloutControlPlaneFailureReturnsInternal(t *testing.T) {
	control := &fakeControlPublisher{abortErr: errors.New("rollout is not running")}
	s := testServer(control)

	if _, err := s.AbortRollout(adminCtx(), &pb.AbortRolloutRequest{RolloutId: "r-1"}); status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

// TestRolloutRPCsNoIdentityFailClosed covers all three rollout handlers
// with the same defensive no-identity case — unreachable in practice
// (authn's interceptor runs first) but must fail closed regardless, and
// must never reach control-plane.
func TestRolloutRPCsNoIdentityFailClosed(t *testing.T) {
	control := &fakeControlPublisher{rolloutID: "r-1", newStage: "canary_10"}
	s := testServer(control)
	ctx := context.Background()

	if _, err := s.StartRollout(ctx, &pb.StartRolloutRequest{ModelRef: "openai", VersionId: "v-2", StrategyJson: `{}`}); status.Code(err) != codes.Internal {
		t.Errorf("StartRollout: err = %v, want codes.Internal", err)
	}
	if _, err := s.PromoteRollout(ctx, &pb.PromoteRolloutRequest{RolloutId: "r-1"}); status.Code(err) != codes.Internal {
		t.Errorf("PromoteRollout: err = %v, want codes.Internal", err)
	}
	if _, err := s.AbortRollout(ctx, &pb.AbortRolloutRequest{RolloutId: "r-1"}); status.Code(err) != codes.Internal {
		t.Errorf("AbortRollout: err = %v, want codes.Internal", err)
	}
	if control.lastCreateReq != nil || control.lastPromoteReq != nil || control.lastAbortReq != nil {
		t.Error("control-plane was called despite no verified identity")
	}
}

// Step S: CreateApiKey/RevokeApiKey — backed by apikeys.Store rather than
// control-plane (admin.proto's own header: these never reach it at all).

func TestCreateApiKeySuccess(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	s := testServerWithKeys(keys)

	resp, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "org-1", Scopes: []string{"chat.completions"}})
	if err != nil {
		t.Fatalf("CreateApiKey: %v", err)
	}
	if resp.GetKeyId() == "" {
		t.Error("key_id is empty")
	}
	if resp.GetRawKey() == "" {
		t.Error("raw_key is empty — it must be returned exactly once, here")
	}
	if len(keys.CreateCalls) != 1 || keys.CreateCalls[0].OrgID != "org-1" {
		t.Fatalf("unexpected Create calls: %+v", keys.CreateCalls)
	}
	// The stored value must be a HASH, never the raw key itself.
	if keys.CreateCalls[0].Hash == resp.GetRawKey() {
		t.Error("hash passed to the store equals the raw key — the raw key must never be stored")
	}
}

func TestCreateApiKeyStoreFailureReturnsInternal(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.CreateErr = errors.New("org_id not found")
	s := testServerWithKeys(keys)

	if _, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "no-such-org"}); status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

func TestRevokeApiKeySuccess(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	s := testServerWithKeys(keys)

	created, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "org-1"})
	if err != nil {
		t.Fatalf("setup CreateApiKey: %v", err)
	}
	if _, err := s.RevokeApiKey(adminCtx(), &pb.RevokeApiKeyRequest{KeyId: created.GetKeyId()}); err != nil {
		t.Fatalf("RevokeApiKey: %v", err)
	}
	if len(keys.RevokeCalls) != 1 || keys.RevokeCalls[0] != created.GetKeyId() {
		t.Fatalf("unexpected Revoke calls: %+v", keys.RevokeCalls)
	}
}

// TestRevokeApiKeyUnknownIdReturnsInternal — a key_id that does not exist
// (or is already revoked) must be a real failure, not a silent no-op.
// The interceptor turns that failure into a revoke_api_key_failed audit
// row; this test's job is only that the handler refuses.
func TestRevokeApiKeyUnknownIdReturnsInternal(t *testing.T) {
	keys := apikeys.NewFakeStore()
	s := testServerWithKeys(keys)

	if _, err := s.RevokeApiKey(adminCtx(), &pb.RevokeApiKeyRequest{KeyId: "does-not-exist"}); status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

func TestApiKeyRPCsNoIdentityFailClosed(t *testing.T) {
	keys := apikeys.NewFakeStore()
	s := testServerWithKeys(keys)
	ctx := context.Background()

	if _, err := s.CreateApiKey(ctx, &pb.CreateApiKeyRequest{OrgId: "org-1"}); status.Code(err) != codes.Internal {
		t.Errorf("CreateApiKey: err = %v, want codes.Internal", err)
	}
	if _, err := s.RevokeApiKey(ctx, &pb.RevokeApiKeyRequest{KeyId: "k-1"}); status.Code(err) != codes.Internal {
		t.Errorf("RevokeApiKey: err = %v, want codes.Internal", err)
	}
	if len(keys.CreateCalls) != 0 || len(keys.RevokeCalls) != 0 {
		t.Error("store was called despite no verified identity — must fail before ever touching the store")
	}
}
