// Package policy implements per-org policy rule reads/writes, backed by
// the policy table (data/migrations/0010). Like pricing/, no gRPC RPC
// exposes this yet — Phase-04.txt specifies no policy RPC, and this
// phase's own scope is registry + manifests + etcd + Vault; policy's real
// consumer and semantics belong to a later phase. This package is the
// storage/service layer for whenever that consumer arrives.
//
// rules_json is an opaque JSON blob (no fixed schema forced here) — this
// phase has no defined policy rule shape to enforce; inventing one would
// be a design decision belonging to whichever later phase actually reads
// and acts on policy rules.
package policy

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrNotFound = errors.New("policy not found")

type Entry struct {
	PolicyID  string
	OrgID     string
	RulesJSON string
	UpdatedAt time.Time
}

type Store interface {
	Insert(ctx context.Context, orgID, rulesJSON string) error
	GetCurrent(ctx context.Context, orgID string) (*Entry, error)
}

type CockroachStore struct {
	db *sql.DB
}

func NewCockroachStore(db *sql.DB) *CockroachStore {
	return &CockroachStore{db: db}
}

func (c *CockroachStore) Insert(ctx context.Context, orgID, rulesJSON string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO policy (org_id, rules_json)
		VALUES ($1, $2)
	`, orgID, rulesJSON)
	return err
}

// GetCurrent returns the most recently updated policy row for orgID —
// same "latest wins by timestamp, application convention not a DB
// constraint" shape as pricing.Store.GetCurrent.
func (c *CockroachStore) GetCurrent(ctx context.Context, orgID string) (*Entry, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT policy_id, org_id, rules_json, updated_at
		FROM policy
		WHERE org_id = $1
		ORDER BY updated_at DESC
		LIMIT 1
	`, orgID)

	var e Entry
	if err := row.Scan(&e.PolicyID, &e.OrgID, &e.RulesJSON, &e.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &e, nil
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) SetPolicy(ctx context.Context, orgID, rulesJSON string) error {
	return s.store.Insert(ctx, orgID, rulesJSON)
}

func (s *Service) GetCurrentPolicy(ctx context.Context, orgID string) (*Entry, error) {
	return s.store.GetCurrent(ctx, orgID)
}
