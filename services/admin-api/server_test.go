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
	"github.com/onezox/OneZox/services/admin-api/internal/audit"
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

func testServer(control controlPublisher, auditWriter audit.Writer) *server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &server{control: control, keys: apikeys.NewFakeStore(), audit: auditWriter, log: log}
}

func testServerWithKeys(keys apikeys.Store, auditWriter audit.Writer) *server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &server{keys: keys, audit: auditWriter, log: log}
}

func adminCtx() context.Context {
	return authn.WithIdentity(context.Background(), &authn.Identity{UserID: "u1", OrgID: "o1", Role: "admin"})
}

func TestPublishModelVersionSuccess(t *testing.T) {
	control := &fakeControlPublisher{versionID: "v-123"}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

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

	if len(auditW.Entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(auditW.Entries))
	}
	e := auditW.Entries[0]
	if e.Actor != "u1" || e.Action != "publish_model_version" || e.Target != "openai" {
		t.Errorf("audit entry = %+v, want actor=u1 action=publish_model_version target=openai", e)
	}
	if e.Before != nil {
		t.Errorf("before_json = %v, want nil (publish never edits a prior state)", e.Before)
	}
	if e.After == nil {
		t.Error("after_json is nil, want the published version's real content")
	}
}

func TestPublishModelVersionControlPlaneFailureIsAudited(t *testing.T) {
	control := &fakeControlPublisher{err: errors.New("control-plane unreachable")}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

	_, err := s.PublishModelVersion(adminCtx(), &pb.PublishModelVersionRequest{ModelRef: "openai", SpecJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}

	if len(auditW.Entries) != 1 {
		t.Fatalf("got %d audit entries, want 1 (a failed attempt is still an admin action)", len(auditW.Entries))
	}
	if auditW.Entries[0].Action != "publish_model_version_failed" {
		t.Errorf("action = %q, want publish_model_version_failed", auditW.Entries[0].Action)
	}
}

// TestPublishModelVersionAuditFailureFailsTheCall is this step's own
// central design requirement: a manifest that genuinely got published in
// control-plane, but whose audit_log write then fails, must NOT be
// reported to the caller as a success — an unaudited success must never
// be reachable (EC3).
func TestPublishModelVersionAuditFailureFailsTheCall(t *testing.T) {
	control := &fakeControlPublisher{versionID: "v-456"}
	auditW := audit.NewFakeWriter()
	auditW.Err = errors.New("audit_log insert failed")
	s := testServer(control, auditW)

	_, err := s.PublishModelVersion(adminCtx(), &pb.PublishModelVersionRequest{ModelRef: "openai", SpecJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal — an unaudited success must not reach the caller as success", err)
	}
}

func TestPublishModelVersionNoIdentityFailsClosed(t *testing.T) {
	control := &fakeControlPublisher{versionID: "v-789"}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

	_, err := s.PublishModelVersion(context.Background(), &pb.PublishModelVersionRequest{ModelRef: "openai", SpecJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal (no identity on context)", err)
	}
	if len(auditW.Entries) != 0 {
		t.Errorf("got %d audit entries, want 0 — no real identity to attribute one to", len(auditW.Entries))
	}
}

func TestStartRolloutSuccess(t *testing.T) {
	control := &fakeControlPublisher{rolloutID: "r-1"}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

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
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "start_rollout" || auditW.Entries[0].Actor != "u1" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

func TestStartRolloutControlPlaneFailureIsAudited(t *testing.T) {
	control := &fakeControlPublisher{createErr: errors.New("model_ref already has a running rollout")}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

	_, err := s.StartRollout(adminCtx(), &pb.StartRolloutRequest{ModelRef: "openai", VersionId: "v-2", StrategyJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "start_rollout_failed" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

func TestStartRolloutAuditFailureFailsTheCall(t *testing.T) {
	control := &fakeControlPublisher{rolloutID: "r-1"}
	auditW := audit.NewFakeWriter()
	auditW.Err = errors.New("audit_log insert failed")
	s := testServer(control, auditW)

	_, err := s.StartRollout(adminCtx(), &pb.StartRolloutRequest{ModelRef: "openai", VersionId: "v-2", StrategyJson: `{}`})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal — an unaudited success must not reach the caller as success", err)
	}
}

func TestPromoteRolloutSuccess(t *testing.T) {
	control := &fakeControlPublisher{newStage: "canary_10"}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

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
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "promote_rollout" || auditW.Entries[0].Target != "r-1" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

func TestPromoteRolloutAuditFailureFailsTheCall(t *testing.T) {
	control := &fakeControlPublisher{newStage: "stable"}
	auditW := audit.NewFakeWriter()
	auditW.Err = errors.New("audit_log insert failed")
	s := testServer(control, auditW)

	_, err := s.PromoteRollout(adminCtx(), &pb.PromoteRolloutRequest{RolloutId: "r-1"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
}

func TestAbortRolloutSuccess(t *testing.T) {
	control := &fakeControlPublisher{}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

	if _, err := s.AbortRollout(adminCtx(), &pb.AbortRolloutRequest{RolloutId: "r-1"}); err != nil {
		t.Fatalf("AbortRollout: %v", err)
	}
	if control.lastAbortReq.GetRolloutId() != "r-1" {
		t.Errorf("unexpected AbortRollout request: %+v", control.lastAbortReq)
	}
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "abort_rollout" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

func TestAbortRolloutControlPlaneFailureIsAudited(t *testing.T) {
	control := &fakeControlPublisher{abortErr: errors.New("rollout is not running")}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)

	_, err := s.AbortRollout(adminCtx(), &pb.AbortRolloutRequest{RolloutId: "r-1"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "abort_rollout_failed" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

// TestRolloutRPCsNoIdentityFailClosed covers all three new handlers with
// the same defensive no-identity case PublishModelVersion already has —
// unreachable in practice (authn's interceptor runs first) but must fail
// closed regardless, and must never attempt an audit write with no real
// actor to attribute it to.
func TestRolloutRPCsNoIdentityFailClosed(t *testing.T) {
	control := &fakeControlPublisher{rolloutID: "r-1", newStage: "canary_10"}
	auditW := audit.NewFakeWriter()
	s := testServer(control, auditW)
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
	if len(auditW.Entries) != 0 {
		t.Errorf("got %d audit entries, want 0 — no real identity to attribute any to", len(auditW.Entries))
	}
}

// Step S: CreateApiKey/RevokeApiKey — same success/failure/no-identity
// shape as every RPC above, but backed by apikeys.Store instead of
// control-plane (admin.proto's own header comment: these never reach
// control-plane at all).

func TestCreateApiKeySuccess(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	auditW := audit.NewFakeWriter()
	s := testServerWithKeys(keys, auditW)

	resp, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "org-1", Scopes: []string{"chat.completions"}})
	if err != nil {
		t.Fatalf("CreateApiKey: %v", err)
	}
	if resp.GetKeyId() == "" {
		t.Error("key_id is empty")
	}
	if resp.GetRawKey() == "" {
		t.Error("raw_key is empty — must be returned exactly once")
	}

	if len(keys.CreateCalls) != 1 || keys.CreateCalls[0].OrgID != "org-1" {
		t.Fatalf("unexpected Create calls: %+v", keys.CreateCalls)
	}
	// The stored hash must NOT equal the raw key returned to the caller —
	// proves the store never receives the raw value itself.
	if keys.CreateCalls[0].Hash == resp.GetRawKey() {
		t.Error("hash passed to the store equals the raw key — raw key must never be stored")
	}

	if len(auditW.Entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(auditW.Entries))
	}
	e := auditW.Entries[0]
	if e.Actor != "u1" || e.Action != "create_api_key" || e.Target != "org-1" {
		t.Errorf("audit entry = %+v, want actor=u1 action=create_api_key target=org-1", e)
	}
	// after_json must never carry the raw key or its hash — only
	// key_id/org_id/scopes (server.go's own CreateApiKey doc comment).
	after, ok := e.After.(map[string]any)
	if !ok {
		t.Fatalf("after_json = %#v, want map[string]any", e.After)
	}
	if _, present := after["raw_key"]; present {
		t.Error("audit after_json contains raw_key — must never be recorded")
	}
	if _, present := after["hash"]; present {
		t.Error("audit after_json contains hash — must never be recorded")
	}
}

func TestCreateApiKeyStoreFailureIsAudited(t *testing.T) {
	keys := apikeys.NewFakeStore() // ValidOrgIDs nil-but-unset means every org_id "exists"; force a failure instead
	keys.CreateErr = errors.New("org_id not found")
	auditW := audit.NewFakeWriter()
	s := testServerWithKeys(keys, auditW)

	_, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "no-such-org"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "create_api_key_failed" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

func TestCreateApiKeyAuditFailureFailsTheCall(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	auditW := audit.NewFakeWriter()
	auditW.Err = errors.New("audit_log insert failed")
	s := testServerWithKeys(keys, auditW)

	_, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "org-1"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal — a key genuinely created but unaudited must never report success", err)
	}
	if len(keys.CreateCalls) != 1 {
		t.Fatalf("got %d Create calls, want exactly 1 (the key really was created)", len(keys.CreateCalls))
	}
}

func TestRevokeApiKeySuccess(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	auditW := audit.NewFakeWriter()
	s := testServerWithKeys(keys, auditW)

	created, err := s.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "org-1"})
	if err != nil {
		t.Fatalf("setup CreateApiKey: %v", err)
	}
	auditW.Entries = nil // isolate this test's own assertions to the revoke call

	if _, err := s.RevokeApiKey(adminCtx(), &pb.RevokeApiKeyRequest{KeyId: created.GetKeyId()}); err != nil {
		t.Fatalf("RevokeApiKey: %v", err)
	}
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "revoke_api_key" || auditW.Entries[0].Target != created.GetKeyId() {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

// TestRevokeApiKeyUnknownIdIsAudited — no such key_id (or already
// revoked) must still audit the attempt, not silently no-op: EC3's own
// completeness requirement applies to a REFUSED mutation just as much as
// a genuine one (Step R).
func TestRevokeApiKeyUnknownIdIsAudited(t *testing.T) {
	keys := apikeys.NewFakeStore()
	auditW := audit.NewFakeWriter()
	s := testServerWithKeys(keys, auditW)

	_, err := s.RevokeApiKey(adminCtx(), &pb.RevokeApiKeyRequest{KeyId: "does-not-exist"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal", err)
	}
	if len(auditW.Entries) != 1 || auditW.Entries[0].Action != "revoke_api_key_failed" {
		t.Fatalf("unexpected audit entries: %+v", auditW.Entries)
	}
}

func TestRevokeApiKeyAuditFailureFailsTheCall(t *testing.T) {
	keys := apikeys.NewFakeStore()
	keys.ValidOrgIDs = map[string]bool{"org-1": true}
	setupAuditW := audit.NewFakeWriter()
	setupServer := testServerWithKeys(keys, setupAuditW)
	created, err := setupServer.CreateApiKey(adminCtx(), &pb.CreateApiKeyRequest{OrgId: "org-1"})
	if err != nil {
		t.Fatalf("setup CreateApiKey: %v", err)
	}

	auditW := audit.NewFakeWriter()
	auditW.Err = errors.New("audit_log insert failed")
	s := testServerWithKeys(keys, auditW)

	_, err = s.RevokeApiKey(adminCtx(), &pb.RevokeApiKeyRequest{KeyId: created.GetKeyId()})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want codes.Internal — a key genuinely revoked but unaudited must never report success", err)
	}
	if len(keys.RevokeCalls) != 1 {
		t.Fatalf("got %d Revoke calls, want exactly 1 (the key really was revoked)", len(keys.RevokeCalls))
	}
}

func TestApiKeyRPCsNoIdentityFailClosed(t *testing.T) {
	keys := apikeys.NewFakeStore()
	auditW := audit.NewFakeWriter()
	s := testServerWithKeys(keys, auditW)
	ctx := context.Background()

	if _, err := s.CreateApiKey(ctx, &pb.CreateApiKeyRequest{OrgId: "org-1"}); status.Code(err) != codes.Internal {
		t.Errorf("CreateApiKey: err = %v, want codes.Internal", err)
	}
	if _, err := s.RevokeApiKey(ctx, &pb.RevokeApiKeyRequest{KeyId: "k-1"}); status.Code(err) != codes.Internal {
		t.Errorf("RevokeApiKey: err = %v, want codes.Internal", err)
	}
	if len(auditW.Entries) != 0 {
		t.Errorf("got %d audit entries, want 0 — no real identity to attribute any to", len(auditW.Entries))
	}
	if len(keys.CreateCalls) != 0 || len(keys.RevokeCalls) != 0 {
		t.Error("store was called despite no verified identity — must fail before ever touching the store")
	}
}
