// Package etcdclient distributes registered manifests to etcd
// (Phase-04.txt's own etcd key layout: /onezox/manifests/{model_ref}/
// {version_id} -> distributed manifest, /onezox/active/{model_ref} ->
// active version pointer) — the mechanism data-plane's own model_registry
// cache (Step Q) watches.
//
// JSON-encodes the whole manifest as one envelope, with spec_json kept as
// a plain Go string field throughout (never re-parsed into a generic
// map/interface{} and re-marshaled) — that's what keeps this byte-exact.
// encoding/json's string escaping is a lossless round-trip for arbitrary
// UTF-8 content, unlike CockroachDB's JSONB column type (the exact bug
// Step E hit and fixed by moving spec_json to STRING, data/migrations/
// 0013): a generic re-serialization step here would reintroduce that
// same class of break one layer further down the pipe, silently breaking
// every consumer's independent signature verification.
package etcdclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ManifestEnvelope is exactly model_manifest's own row shape
// (registry.Manifest) — field-for-field, so nothing needs reshaping
// between CockroachDB and etcd.
type ManifestEnvelope struct {
	VersionID string `json:"version_id"`
	ModelRef  string `json:"model_ref"`
	SpecJSON  string `json:"spec_json"`
	Signature string `json:"signature"`
	CreatedBy string `json:"created_by"`
	CreatedAt string `json:"created_at"`
	Status    string `json:"status"`
}

type Client struct {
	cli *clientv3.Client
}

func New(endpoints []string) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to etcd: %w", err)
	}
	return &Client{cli: cli}, nil
}

func (c *Client) Close() error {
	return c.cli.Close()
}

// PublishManifest writes the immutable manifest record to
// /onezox/manifests/{model_ref}/{version_id}. Idempotent (a re-publish of
// the same version_id overwrites with identical content — model_manifest
// rows are themselves immutable, so this can never legitimately differ).
// Individual string params (not a struct) so *Client satisfies
// registry.Publisher directly, by duck typing, without an adapter type.
func (c *Client) PublishManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy, createdAt, status string) error {
	m := ManifestEnvelope{
		VersionID: versionID,
		ModelRef:  modelRef,
		SpecJSON:  specJSON,
		Signature: signature,
		CreatedBy: createdBy,
		CreatedAt: createdAt,
		Status:    status,
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling manifest envelope: %w", err)
	}
	key := fmt.Sprintf("/onezox/manifests/%s/%s", modelRef, versionID)
	if _, err := c.cli.Put(ctx, key, string(data)); err != nil {
		return fmt.Errorf("publishing manifest to etcd: %w", err)
	}
	return nil
}

// ActiveEnvelope is the Phase-05 shape of /onezox/active/{model_ref} —
// grown from a bare version_id string (Phase-04) into a small envelope
// carrying the staged-canary state, additively: every Phase-04 consumer
// that only ever cared about "the one live version" reads Stable exactly
// where it used to read the whole value; Canary/CanaryPercent are new
// fields a canary-unaware reader simply never looks at. Canary is an
// empty string, not a pointer/null, when no canary is in progress — a
// version_id is a UUID and therefore never legitimately empty, so this is
// a safe, JSON-simple sentinel that needs no null-handling in any of the
// three consumer languages (Go, Python, Rust all treat "" the same way:
// falsy, no special-case unwrapping required).
type ActiveEnvelope struct {
	Stable        string `json:"stable"`
	Canary        string `json:"canary"`
	CanaryPercent int    `json:"canary_percent"`
}

// PublishActive updates /onezox/active/{model_ref} — the ONE mutable
// pointer this package writes, mirroring model_active's own mutability
// (data/migrations/0009); the manifest content at PublishManifest's key
// never changes once written. Sets Stable and clears any canary state
// (Canary: "", CanaryPercent: 0) — this is RegisterModelManifest's own
// bootstrap-activation call (registry.go), which by definition is never
// itself an in-progress canary. Step L's rollout module writes a
// DIFFERENT, more targeted update for setting/advancing canary state,
// once it exists to consume one — this method's own contract (activate
// versionID as the stable pointer, no canary) is unchanged from what
// RegisterModelManifest has always needed from it.
func (c *Client) PublishActive(ctx context.Context, modelRef, versionID string) error {
	env := ActiveEnvelope{Stable: versionID}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshaling active envelope: %w", err)
	}
	key := fmt.Sprintf("/onezox/active/%s", modelRef)
	if _, err := c.cli.Put(ctx, key, string(data)); err != nil {
		return fmt.Errorf("publishing active pointer to etcd: %w", err)
	}
	return nil
}
