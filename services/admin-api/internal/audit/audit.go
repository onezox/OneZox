// Package audit is EC3's own storage layer: every admin action, mutating
// or denied, written to audit_log (data/migrations/0017) as an immutable
// append — the admin_api DB role has SELECT+INSERT only there (migration
// 0018), no UPDATE/DELETE, the same mechanism model_manifest's own
// immutability uses (Phase-04 migrations 0008/0012).
//
// Two operations, matching that grant exactly: the one INSERT (Writer,
// below) and — since Step U1b — the SELECT backing the panel's own Audit
// section (Reader, reader.go). No UPDATE or DELETE statement exists
// anywhere in this package, because no grant exists that could run one;
// Step Q proved that adversarially at the database itself.
//
// Ordering (Step H's own explicit design question): before_json/after_json
// for a mutation aren't knowable until the mutation's own result comes
// back — control-plane generates version_id itself, admin-api doesn't
// choose it — so there is only one possible order: perform the action,
// THEN write the one audit row that records its real outcome. Because
// audit_log is INSERT-only, a two-phase "pending, then complete" row is
// not just undesirable, it is impossible (no UPDATE grant exists to make
// the second phase work) — this package's shape (one Write call, one
// complete Entry) is not a choice made in isolation, it is the only shape
// the schema's own immutability guarantee permits.
//
// What happens if the write itself fails, after a real mutation already
// succeeded in control-plane, is decided at the call site (server.go's
// RPC handlers): the RPC returns an error to the caller even though the
// underlying action happened — an unaudited "success" response must never
// reach a caller, per this step's own instruction that a lost audit must
// fail, not silently drop. This is a deliberate FAIL-LOUD choice, not a
// true distributed-transaction guarantee ("impossible") — control-plane's
// own database and admin-api's own audit_log are two separate connections
// with no shared transaction possible across that RPC boundary; a real
// outbox/saga pattern would be needed for atomicity stronger than this,
// and nothing in Phase-05.txt's own scope asks for one.
package audit

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Entry is one row's worth of what gets written. Before/After are `any`
// and marshaled to JSON here (audit_log.before_json/after_json are JSONB
// — unlike model_manifest.spec_json or rollout.strategy_json, nothing
// ever signs or byte-compares this content, so JSONB's own reformatting
// is harmless here, migration 0017's own reasoning) — nil means "no
// value," written as SQL NULL, not the JSON literal "null".
type Entry struct {
	Actor  string // admin_user.user_id — always a real, authn-verified identity (see server.go)
	Action string
	Target string
	Before any
	After  any
}

// Writer is implemented by CockroachWriter in production and FakeWriter
// in tests — kept as an interface so authz's own denial-audit path
// (Step G/H wiring in main.go) and every RPC handler's own success/
// failure audit share one real implementation without either depending
// on *sql.DB directly.
type Writer interface {
	Write(ctx context.Context, e Entry) error
}

type CockroachWriter struct {
	db *sql.DB
}

func NewCockroachWriter(db *sql.DB) *CockroachWriter {
	return &CockroachWriter{db: db}
}

func (w *CockroachWriter) Write(ctx context.Context, e Entry) error {
	beforeJSON, err := marshalNullable(e.Before)
	if err != nil {
		return err
	}
	afterJSON, err := marshalNullable(e.After)
	if err != nil {
		return err
	}

	_, err = w.db.ExecContext(ctx, `
		INSERT INTO audit_log (actor, action, target, before_json, after_json)
		VALUES ($1, $2, $3, $4, $5)
	`, e.Actor, e.Action, e.Target, beforeJSON, afterJSON)
	return err
}

// marshalNullable returns (nil, nil) for a nil value — becomes SQL NULL,
// not the two-byte JSON string "null", which would misrepresent "no
// before-state" (an insert-only action like publishModelVersion, which
// has no prior version to record) as if a JSON null value had been
// deliberately recorded.
func marshalNullable(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}
