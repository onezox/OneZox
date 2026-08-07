package authn

import (
	"context"
	"database/sql"
	"errors"
)

// CockroachStore is the real Store implementation — the ONLY SQL this
// package ever issues, and it names admin_user explicitly, nowhere near
// api_keys. Runs as the admin_api role (migration 0018), which itself
// only has SELECT on admin_user — this store could not write to it even
// if a future bug tried to.
type CockroachStore struct {
	db *sql.DB
}

func NewCockroachStore(db *sql.DB) *CockroachStore {
	return &CockroachStore{db: db}
}

func (c *CockroachStore) LookupByHash(ctx context.Context, hash string) (*Row, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT user_id, org_id, role, revoked_at
		FROM admin_user WHERE credential_hash = $1
	`, hash)

	var r Row
	if err := row.Scan(&r.UserID, &r.OrgID, &r.Role, &r.RevokedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}
