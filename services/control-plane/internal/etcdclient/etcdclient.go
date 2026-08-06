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

// PublishActive updates /onezox/active/{model_ref} — the ONE mutable
// pointer this package writes, mirroring model_active's own mutability
// (data/migrations/0009); the manifest content at PublishManifest's key
// never changes once written.
func (c *Client) PublishActive(ctx context.Context, modelRef, versionID string) error {
	key := fmt.Sprintf("/onezox/active/%s", modelRef)
	if _, err := c.cli.Put(ctx, key, versionID); err != nil {
		return fmt.Errorf("publishing active pointer to etcd: %w", err)
	}
	return nil
}
