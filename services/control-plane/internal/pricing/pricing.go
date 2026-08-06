// Package pricing implements per-model unit-cost reads/writes, backed by
// the pricing table (data/migrations/0011). No gRPC RPC exposes this yet
// — Phase-04.txt's own APIS CREATED section lists exactly four RPCs
// (RegisterModelManifest/GetModelManifest/ListModels/IssueProviderToken)
// and none for pricing; "consumed by scheduler cost-gating in Phase-06"
// is Phase-04.txt's own words for who actually calls this. This package
// is the storage/service layer Phase-06 will call into (directly, or a
// future RPC wrapping it) — inventing an RPC not specified here would be
// scope this phase isn't chartered to add.
//
// Scope note carried from the migration: this package provides pricing
// DATA only. It does not wire usage_event.usd_cost — that is a separate,
// conscious follow-on, not Phase-04 scope.
//
// Unlike model_manifest, pricing rows are NOT signed or immutable at the
// storage layer — no requirement for that was specified for this table.
// SetPricing always inserts a new row (never updates one in place) purely
// as an application-level convention for keeping price history via
// effective_at, not a security boundary the database itself enforces.
package pricing

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrNotFound = errors.New("pricing not found")

// UnitCosts is unit_costs_json's own shape — USD per million tokens,
// input/output priced separately (the near-universal provider billing
// unit as of this phase).
type UnitCosts struct {
	InputPerMillionTokens  float64 `json:"input_per_million_tokens"`
	OutputPerMillionTokens float64 `json:"output_per_million_tokens"`
	Currency               string  `json:"currency"`
}

type Entry struct {
	PricingID   string
	ModelRef    string
	UnitCosts   UnitCosts
	EffectiveAt time.Time
}

type Store interface {
	Insert(ctx context.Context, modelRef string, unitCosts UnitCosts) error
	GetCurrent(ctx context.Context, modelRef string) (*Entry, error)
	ListCurrent(ctx context.Context) ([]Entry, error)
}

type CockroachStore struct {
	db *sql.DB
}

func NewCockroachStore(db *sql.DB) *CockroachStore {
	return &CockroachStore{db: db}
}

func (c *CockroachStore) Insert(ctx context.Context, modelRef string, unitCosts UnitCosts) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO pricing (model_ref, unit_costs_json)
		VALUES ($1, $2)
	`, modelRef, unitCostsJSON(unitCosts))
	return err
}

// GetCurrent returns the most recently effective pricing row for
// modelRef — "current" meaning highest effective_at, not necessarily
// effective_at <= now() (this phase has no scheduled-future-pricing
// concept, every row inserted here is immediately effective).
func (c *CockroachStore) GetCurrent(ctx context.Context, modelRef string) (*Entry, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT pricing_id, model_ref, unit_costs_json, effective_at
		FROM pricing
		WHERE model_ref = $1
		ORDER BY effective_at DESC
		LIMIT 1
	`, modelRef)
	return scanEntry(row)
}

func (c *CockroachStore) ListCurrent(ctx context.Context) ([]Entry, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT DISTINCT ON (model_ref) pricing_id, model_ref, unit_costs_json, effective_at
		FROM pricing
		ORDER BY model_ref, effective_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var entries []Entry
	for rows.Next() {
		var (
			e        Entry
			costJSON []byte
		)
		if err := rows.Scan(&e.PricingID, &e.ModelRef, &costJSON, &e.EffectiveAt); err != nil {
			return nil, err
		}
		if err := unmarshalUnitCosts(costJSON, &e.UnitCosts); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) SetPricing(ctx context.Context, modelRef string, unitCosts UnitCosts) error {
	return s.store.Insert(ctx, modelRef, unitCosts)
}

func (s *Service) GetCurrentPricing(ctx context.Context, modelRef string) (*Entry, error) {
	return s.store.GetCurrent(ctx, modelRef)
}

func (s *Service) ListCurrentPricing(ctx context.Context) ([]Entry, error) {
	return s.store.ListCurrent(ctx)
}
