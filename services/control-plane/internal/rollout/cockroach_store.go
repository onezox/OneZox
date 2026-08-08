package rollout

import (
	"context"
	"database/sql"
	"errors"
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
func (c *CockroachStore) UpdateRollout(ctx context.Context, rolloutID, stage, status string, endedAt *time.Time) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE rollout SET stage = $2, status = $3, ended_at = $4, stage_entered_at = now() WHERE rollout_id = $1
	`, rolloutID, stage, status, endedAt)
	return err
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
