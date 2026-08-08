package audit

import (
	"context"
	"database/sql"
	"time"
)

// Record is one audit_log row as the panel's Audit section reads it —
// distinct from Entry (the write shape) because the two genuinely
// differ: a write supplies Before/After as `any` to be marshaled, a read
// returns them as the JSON text already stored, plus audit_id/ts which
// the database itself generates and a writer never supplies.
type Record struct {
	AuditID    string
	Actor      string
	Action     string
	Target     string
	BeforeJSON *string
	AfterJSON  *string
	TS         string
}

// Reader is the SELECT half of this package's own SELECT+INSERT grant.
type Reader interface {
	// List returns rows most recent first. actor/action empty mean "no
	// filter on that column". limit is always applied — see the
	// implementation's own default/clamp, mirroring rollout.Service's
	// ListRollouts bounds for the same reason.
	List(ctx context.Context, limit int, actor, action string) ([]Record, error)
}

// DefaultAuditListLimit bounds List when a caller passes 0 — an audit
// view is append-only and grows forever, so unbounded is never the
// right default.
const DefaultAuditListLimit = 50

const maxAuditListLimit = 500

type CockroachReader struct {
	db *sql.DB
}

func NewCockroachReader(db *sql.DB) *CockroachReader {
	return &CockroachReader{db: db}
}

// List builds its WHERE clause from fixed, server-controlled fragments
// with the caller's values bound as parameters — never string-concatenated
// into the SQL, so an `actor` or `action` value can't alter the query's
// own shape.
func (r *CockroachReader) List(ctx context.Context, limit int, actor, action string) ([]Record, error) {
	if limit <= 0 {
		limit = DefaultAuditListLimit
	}
	if limit > maxAuditListLimit {
		limit = maxAuditListLimit
	}

	// $1/$2 are the filters; an empty string means "match everything"
	// via the OR, which keeps this one prepared statement covering all
	// four filter combinations rather than four hand-built queries.
	rows, err := r.db.QueryContext(ctx, `
		SELECT audit_id, actor, action, target, before_json, after_json, ts
		FROM audit_log
		WHERE ($1 = '' OR actor::STRING = $1)
		  AND ($2 = '' OR action = $2)
		ORDER BY ts DESC
		LIMIT $3
	`, actor, action, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Record
	for rows.Next() {
		var rec Record
		var before, after sql.NullString
		var ts time.Time
		if err := rows.Scan(&rec.AuditID, &rec.Actor, &rec.Action, &rec.Target, &before, &after, &ts); err != nil {
			return nil, err
		}
		if before.Valid {
			v := before.String
			rec.BeforeJSON = &v
		}
		if after.Valid {
			v := after.String
			rec.AfterJSON = &v
		}
		rec.TS = ts.Format(time.RFC3339)
		out = append(out, rec)
	}
	return out, rows.Err()
}

// FakeReader is the in-memory Reader for unit tests.
type FakeReader struct {
	Records []Record
	Err     error

	LastLimit  int
	LastActor  string
	LastAction string
}

func (f *FakeReader) List(ctx context.Context, limit int, actor, action string) ([]Record, error) {
	f.LastLimit, f.LastActor, f.LastAction = limit, actor, action
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Records, nil
}
