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
	controlpb "github.com/onezox/OneZox/services/admin-api/internal/pb/control/v1"
)

// fakeControlPublisher implements the narrow controlPublisher interface —
// no dependency on the full generated gRPC client, matching provider-
// gateway's own credentials.FakeFetcher precedent.
type fakeControlPublisher struct {
	versionID string
	err       error
	lastReq   *controlpb.RegisterModelManifestRequest
}

func (f *fakeControlPublisher) RegisterModelManifest(ctx context.Context, in *controlpb.RegisterModelManifestRequest, opts ...grpc.CallOption) (*controlpb.RegisterModelManifestResponse, error) {
	f.lastReq = in
	if f.err != nil {
		return nil, f.err
	}
	return &controlpb.RegisterModelManifestResponse{VersionId: f.versionID}, nil
}

func testServer(control controlPublisher, auditWriter audit.Writer) *server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &server{control: control, audit: auditWriter, log: log}
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
