package rollout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// CockroachStore is the real Store implementation, backed by the
// rollout table (data/migrations/0016, 0019). Runs as the control_plane
// DB role — the same role every other control-plane write already uses,
// unchanged by this package.
type CockroachStore struct {
	db *sql.DB
}

func NewCockroachStore(db *sql.DB) *CockroachStore {
	return &CockroachStore{db: db}
}

func (c *CockroachStore) InsertRollout(ctx context.Context, r Rollout) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO rollout (rollout_id, model_ref, version_id, strategy_json, stage, status, stable_version_id, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, r.RolloutID, r.ModelRef, r.VersionID, r.StrategyJSON, r.Stage, r.Status, r.StableVersionID, r.StartedAt)
	return err
}

func (c *CockroachStore) GetRollout(ctx context.Context, rolloutID string) (*Rollout, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT rollout_id, model_ref, version_id, strategy_json, stage, status, stable_version_id, started_at, ended_at, stage_entered_at
		FROM rollout WHERE rollout_id = $1
	`, rolloutID)
	return scanRollout(row)
}

func (c *CockroachStore) GetRunningRolloutByModelRef(ctx context.Context, modelRef string) (*Rollout, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT rollout_id, model_ref, version_id, strategy_json, stage, status, stable_version_id, started_at, ended_at, stage_entered_at
		FROM rollout WHERE model_ref = $1 AND status = 'running'
		ORDER BY started_at DESC LIMIT 1
	`, modelRef)
	return scanRollout(row)
}

func (c *CockroachStore) ListRunningRollouts(ctx context.Context) ([]Rollout, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT rollout_id, model_ref, version_id, strategy_json, stage, status, stable_version_id, started_at, ended_at, stage_entered_at
		FROM rollout WHERE status = 'running'
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Rollout
	for rows.Next() {
		r, err := scanRollout(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// ListRollouts — Step U1a's own backing read. modelRef empty means
// every model; limit is always applied (Service.ListRollouts supplies
// the default), so this can never issue an unbounded scan.
func (c *CockroachStore) ListRollouts(ctx context.Context, modelRef string, limit int) ([]Rollout, error) {
	const cols = `rollout_id, model_ref, version_id, strategy_json, stage, status, stable_version_id, started_at, ended_at, stage_entered_at`
	var rows *sql.Rows
	var err error
	if modelRef == "" {
		rows, err = c.db.QueryContext(ctx, `SELECT `+cols+` FROM rollout ORDER BY started_at DESC LIMIT $1`, limit)
	} else {
		rows, err = c.db.QueryContext(ctx, `SELECT `+cols+` FROM rollout WHERE model_ref = $1 ORDER BY started_at DESC LIMIT $2`, modelRef, limit)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Rollout
	for rows.Next() {
		r, err := scanRollout(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (c *CockroachStore) GetMostRecentRolloutByModelRef(ctx context.Context, modelRef string) (*Rollout, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT rollout_id, model_ref, version_id, strategy_json, stage, status, stable_version_id, started_at, ended_at, stage_entered_at
		FROM rollout WHERE model_ref = $1
		ORDER BY started_at DESC LIMIT 1
	`, modelRef)
	return scanRollout(row)
}

// UpdateRollout always refreshes stage_entered_at to now() — every call
// site either genuinely changes the stage (advanceStage) or terminalizes
// the rollout (revertCanary, which the reconciler never reconciles again
// since ListRunningRollouts only returns status='running'), so there is
// no call site where refreshing it unconditionally is wrong.
//
// Post-M2 CRITICAL fix — this is now a COMPARE-AND-SWAP. The WHERE
// clause carries the caller's read-time expectation, so the database
// itself arbitrates concurrent writers instead of last-write-wins:
//
//	AND stage = $2       the rollout has not moved since the caller read it
//	AND status = 'running'  it has not been terminalized since either
//
// A losing writer changes ZERO rows and gets ErrConcurrentUpdate. This
// is a single atomic statement, not a read-then-write in a transaction:
// there is no window between the check and the update for anything to
// interleave into, and it needs no retry loop or isolation-level
// reasoning to be correct.
//
// status='running' is what makes the human-abort race safe. If an
// operator aborts a canary while a reconciler is mid-advance, the abort
// sets status='aborted' and the reconciler's UPDATE then matches no row
// — it cannot resurrect a rollout an operator deliberately stopped.
// revertCanary passes r.Stage as its own expectation, so terminalizing
// writes are equally guarded.
func (c *CockroachStore) UpdateRollout(ctx context.Context, rolloutID, expectedStage, stage, status string, endedAt *time.Time) error {
	res, err := c.db.ExecContext(ctx, `
		UPDATE rollout SET stage = $3, status = $4, ended_at = $5, stage_entered_at = now()
		WHERE rollout_id = $1 AND stage = $2 AND status = 'running'
	`, rolloutID, expectedStage, stage, status, endedAt)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("reading rows affected for rollout %s: %w", rolloutID, err)
	}
	if n == 0 {
		// Zero rows means the predicate did not hold — another writer got
		// here first. Not a database error, and deliberately not silent:
		// the caller must be able to tell "I did nothing" from "I did it."
		return ErrConcurrentUpdate
	}
	return nil
}

// scanRow is satisfied by both *sql.Row and *sql.Rows — lets this one
// scan function serve every SELECT above without duplication.
type scanRow interface {
	Scan(dest ...any) error
}

// scanRollout returns (nil, nil) on sql.ErrNoRows — "not found" is a
// normal, expected outcome for GetRollout/GetRunningRolloutByModelRef/
// GetMostRecentRolloutByModelRef (a model_ref with no rollout history
// yet), not an error condition; Service methods translate a nil result
// into ErrNotFound only where that's the correct semantics for the
// specific call (e.g. GetRolloutStatus), and treat it as "no running
// rollout, proceed" where that's correct instead (CreateRollout's own
// concurrency check).
func scanRollout(row scanRow) (*Rollout, error) {
	var r Rollout
	if err := row.Scan(
		&r.RolloutID, &r.ModelRef, &r.VersionID, &r.StrategyJSON,
		&r.Stage, &r.Status, &r.StableVersionID, &r.StartedAt, &r.EndedAt, &r.StageEnteredAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}
