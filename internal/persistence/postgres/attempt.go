package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jyork/pandectrun/internal/engine"
)

// CreateStepAttempt creates a running attempt for a running step execution,
// assigns its ID and start time, and emits step.attempt_started atomically.
//
// Parameters:
//   - in: the parent step execution ID and one-based attempt number.
//
// Returns:
//   - StepAttemptID: the repository-assigned ID of the running attempt.
//   - error: nil on success, ErrNotFound when the parent step does not exist,
//     or ErrInvalidTransition when the parent step is not running.
func (r *ExecutionRepository) CreateStepAttempt(ctx context.Context, in engine.NewStepAttempt) (engine.StepAttemptID, error) {
	var id int64
	err := r.withTransaction(ctx, func(tx pgx.Tx) error {
		parent, status, err := lockStep(ctx, tx, in.StepExecutionID)
		if err != nil {
			return err
		}
		if status != engine.StepRunning {
			return fmt.Errorf("%w: step %s -> create attempt", engine.ErrInvalidTransition, status)
		}
		if in.Attempt == 0 || in.Attempt > 2147483647 {
			return engine.ErrInvalidInput
		}
		var started time.Time
		err = tx.QueryRow(ctx, `INSERT INTO step_attempts (step_execution_id, attempt, started_at)
			VALUES ($1, $2, clock_timestamp()) ON CONFLICT (step_execution_id, attempt) DO NOTHING RETURNING id, started_at`, int64(in.StepExecutionID), in.Attempt).Scan(&id, &started)
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ErrAlreadyExists
		}
		if err != nil {
			return err
		}
		return appendAttemptEvent(ctx, tx, parent, in.StepExecutionID, engine.StepAttemptID(id), engine.EventStepAttemptStarted, started)
	})
	if err != nil {
		return 0, err
	}
	return engine.StepAttemptID(id), nil
}

// CompleteStepAttempt transitions a running attempt to completed.
//
// Parameters:
//   - id: the running attempt to complete.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the attempt is not running.
func (r *ExecutionRepository) CompleteStepAttempt(ctx context.Context, id engine.StepAttemptID) error {
	return r.finishAttempt(ctx, id, engine.StepAttemptCompleted, nil)
}

// FailStepAttempt transitions a running attempt to failed, persists its error,
// and emits step.attempt_failed atomically.
//
// Parameters:
//   - id: the running attempt to fail.
//   - execErr: the attempt error to persist.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the attempt is not running.
func (r *ExecutionRepository) FailStepAttempt(ctx context.Context, id engine.StepAttemptID, execErr engine.ExecutionError) error {
	return r.finishAttempt(ctx, id, engine.StepAttemptFailed, &execErr)
}

// CancelStepAttempt transitions a running attempt to cancelled. Repeating the
// operation after cancellation is idempotent.
//
// Parameters:
//   - id: the running attempt to cancel.
//
// Returns:
//   - error: nil when cancellation succeeds or is already complete,
//     ErrNotFound when id does not exist, or ErrInvalidTransition when the
//     attempt is in another terminal state.
func (r *ExecutionRepository) CancelStepAttempt(ctx context.Context, id engine.StepAttemptID) error {
	return r.finishAttempt(ctx, id, engine.StepAttemptCancelled, nil)
}

// RecordStepRetryScheduled records that another attempt will be made after the
// identified failed attempt. The attempt must already be in the failed state.
//
// Parameters:
//   - id: the failed attempt after which another attempt will be scheduled.
//
// Returns:
//   - error: nil on success, ErrNotFound when the attempt or parent step does
//     not exist, or ErrInvalidTransition when the attempt is not failed.
func (r *ExecutionRepository) RecordStepRetryScheduled(ctx context.Context, id engine.StepAttemptID) error {
	return r.withTransaction(ctx, func(tx pgx.Tx) error {
		parent, step, status, err := lockAttempt(ctx, tx, id)
		if err != nil {
			return err
		}
		if status != engine.StepAttemptFailed {
			return fmt.Errorf("%w: attempt %s -> retry scheduled", engine.ErrInvalidTransition, status)
		}
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return err
		}
		return appendAttemptEvent(ctx, tx, parent, step, id, engine.EventStepRetryScheduled, now)
	})
}

// lockAttempt acquires locks in execution, step, attempt order and returns
// the immutable parent IDs and current attempt status.
func lockAttempt(ctx context.Context, tx pgx.Tx, id engine.StepAttemptID) (engine.ExecutionID, engine.StepExecutionID, engine.StepAttemptStatus, error) {
	var step int64
	err := tx.QueryRow(ctx, "SELECT step_execution_id FROM step_attempts WHERE id = $1", int64(id)).Scan(&step)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", engine.ErrNotFound
	}
	if err != nil {
		return "", 0, "", err
	}
	stepID := engine.StepExecutionID(step)
	parent, _, err := lockStep(ctx, tx, stepID)
	if err != nil {
		return "", 0, "", err
	}
	var status string
	err = tx.QueryRow(ctx, "SELECT status FROM step_attempts WHERE id = $1 FOR UPDATE", int64(id)).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", engine.ErrNotFound
	}
	if err != nil {
		return "", 0, "", err
	}
	return parent, stepID, engine.StepAttemptStatus(status), nil
}

// finishAttempt applies a terminal transition and writes a failure event
// in the same transaction when appropriate.
func (r *ExecutionRepository) finishAttempt(ctx context.Context, id engine.StepAttemptID, target engine.StepAttemptStatus, failure *engine.ExecutionError) error {
	return r.withTransaction(ctx, func(tx pgx.Tx) error {
		parent, step, status, err := lockAttempt(ctx, tx, id)
		if err != nil {
			return err
		}
		if status == engine.StepAttemptCancelled && target == engine.StepAttemptCancelled {
			return nil
		}
		if status != engine.StepAttemptRunning {
			return fmt.Errorf("%w: attempt %s -> %s", engine.ErrInvalidTransition, status, target)
		}
		var encoded json.RawMessage
		if failure != nil {
			encoded, err = json.Marshal(failure)
			if err != nil {
				return err
			}
		}
		var completed time.Time
		err = tx.QueryRow(ctx, `UPDATE step_attempts SET status = $2, error = $3,
			completed_at = clock_timestamp() WHERE id = $1 RETURNING completed_at`, int64(id), string(target), encoded).Scan(&completed)
		if err != nil {
			return err
		}
		if target == engine.StepAttemptFailed {
			return appendAttemptEvent(ctx, tx, parent, step, id, engine.EventStepAttemptFailed, completed)
		}
		return nil
	})
}

// appendAttemptEvent writes an attempt-scoped event in the caller's transaction.
func appendAttemptEvent(ctx context.Context, tx pgx.Tx, parent engine.ExecutionID, step engine.StepExecutionID, id engine.StepAttemptID, event engine.ExecutionEventType, created time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO execution_events (execution_id, step_execution_id, step_attempt_id, type, created_at)
		VALUES ($1::text::uuid, $2, $3, $4, $5)`, string(parent), int64(step), int64(id), string(event), created)
	return err
}

// ListStepAttempts returns attempts for stepID ordered by attempt number.
// It returns ErrNotFound when the step execution does not exist.
//
// Parameters:
//   - stepID: the step execution whose attempts are requested.
//
// Returns:
//   - []StepAttempt: detached attempts ordered by one-based attempt number.
//   - error: nil on success, or ErrNotFound when stepID does not exist.
func (r *ExecutionRepository) ListStepAttempts(ctx context.Context, stepID engine.StepExecutionID) ([]engine.StepAttempt, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM step_executions WHERE id = $1)", int64(stepID)).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check parent step: %w", err)
	}
	if !exists {
		return nil, engine.ErrNotFound
	}
	rows, err := r.pool.Query(ctx, `SELECT id, step_execution_id, attempt, status, started_at, completed_at, error::text
		FROM step_attempts WHERE step_execution_id = $1 ORDER BY attempt`, int64(stepID))
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()
	out := make([]engine.StepAttempt, 0)
	for rows.Next() {
		var a engine.StepAttempt
		var id, step int64
		var number int32
		var status string
		var failure *string
		if err := rows.Scan(&id, &step, &number, &status, &a.StartedAt, &a.CompletedAt, &failure); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		a.ID = engine.StepAttemptID(id)
		a.StepExecutionID = engine.StepExecutionID(step)
		a.Attempt = uint(number)
		a.Status = engine.StepAttemptStatus(status)
		if failure != nil {
			if err := json.Unmarshal([]byte(*failure), &a.Error); err != nil {
				return nil, fmt.Errorf("decode attempt error: %w", err)
			}
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	return out, nil
}
