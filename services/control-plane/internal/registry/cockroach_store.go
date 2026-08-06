package registry

import (
	"context"
	"database/sql"
	"errors"
)

// CockroachStore is the real Store implementation, backed by
// model_manifest/model_active (data/migrations/0008-0009). Every call
// here runs as the control_plane DB role (main.go's own connection) —
// InsertManifest relies on that role having INSERT but not UPDATE/DELETE
// on model_manifest for the immutability guarantee to hold; this type
// itself has no extra enforcement beyond issuing plain SQL, the database
// itself is what refuses a mutation attempt.
type CockroachStore struct {
	db *sql.DB
}

func NewCockroachStore(db *sql.DB) *CockroachStore {
	return &CockroachStore{db: db}
}

func (c *CockroachStore) InsertManifest(ctx context.Context, versionID, modelRef, specJSON, signature, createdBy string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO model_manifest (version_id, model_ref, spec_json, signature, created_by)
		VALUES ($1, $2, $3, $4, $5)
	`, versionID, modelRef, specJSON, signature, createdBy)
	return err
}

// SetActive upserts the model_active pointer — this table IS mutable by
// design (data/migrations/0009), unlike model_manifest.
func (c *CockroachStore) SetActive(ctx context.Context, modelRef, versionID string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO model_active (model_ref, active_version_id, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (model_ref) DO UPDATE SET active_version_id = excluded.active_version_id, updated_at = now()
	`, modelRef, versionID)
	return err
}

func scanManifest(row *sql.Row) (*Manifest, error) {
	var m Manifest
	if err := row.Scan(&m.VersionID, &m.ModelRef, &m.SpecJSON, &m.Signature, &m.CreatedBy, &m.CreatedAt, &m.Status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &m, nil
}

func (c *CockroachStore) GetManifestByVersion(ctx context.Context, versionID string) (*Manifest, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT version_id, model_ref, spec_json, signature, created_by, created_at, status
		FROM model_manifest WHERE version_id = $1
	`, versionID)
	return scanManifest(row)
}

func (c *CockroachStore) GetActiveManifest(ctx context.Context, modelRef string) (*Manifest, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT mm.version_id, mm.model_ref, mm.spec_json, mm.signature, mm.created_by, mm.created_at, mm.status
		FROM model_manifest mm
		JOIN model_active ma ON ma.active_version_id = mm.version_id
		WHERE ma.model_ref = $1
	`, modelRef)
	return scanManifest(row)
}

func (c *CockroachStore) ListActive(ctx context.Context) ([]Entry, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT model_ref, active_version_id FROM model_active ORDER BY model_ref`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ModelRef, &e.ActiveVersionID); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
