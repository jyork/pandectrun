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

const stepColumns = "id, execution_id::text, step_id, status, output::text, error::text, started_at, completed_at"

// CreateStepExecution creates a pending runtime record for a logical workflow step.
// It returns ErrNotFound when the parent execution does not exist.
//
// Parameters:
//   - in: the parent execution ID and logical workflow step ID.
//
// Returns:
//   - StepExecutionID: the repository-assigned ID of the new step execution.
//   - error: nil on success, or ErrNotFound when the parent execution does not exist.
func (r *ExecutionRepository) CreateStepExecution(ctx context.Context, in engine.NewStepExecution) (engine.StepExecutionID, error) {
	var id int64
	err := r.withTransaction(ctx, func(tx pgx.Tx) error {
		if err := lockExecution(ctx, tx, in.ExecutionID); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `INSERT INTO step_executions (execution_id, step_id)
			VALUES ($1::text::uuid, $2) ON CONFLICT (execution_id, step_id) DO NOTHING RETURNING id`, string(in.ExecutionID), in.StepID).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ErrAlreadyExists
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return engine.StepExecutionID(id), nil
}

// GetStepExecution returns a copy of the step execution identified by id.
// It returns ErrNotFound when the step execution does not exist.
//
// Parameters:
//   - id: the step execution ID to retrieve.
//
// Returns:
//   - StepExecution: a detached copy of the persisted step execution.
//   - error: nil on success, or ErrNotFound when id does not exist.
func (r *ExecutionRepository) GetStepExecution(ctx context.Context, id engine.StepExecutionID) (engine.StepExecution, error) {
	return scanStep(r.pool.QueryRow(ctx, "SELECT "+stepColumns+" FROM step_executions WHERE id = $1", int64(id)))
}

// ListStepExecutions returns the step executions belonging to executionID in
// deterministic ID order. It returns ErrNotFound when the execution does not exist.
//
// Parameters:
//   - executionID: the parent execution whose step executions are requested.
//
// Returns:
//   - []StepExecution: detached step executions in deterministic ID order.
//   - error: nil on success, or ErrNotFound when executionID does not exist.
func (r *ExecutionRepository) ListStepExecutions(ctx context.Context, executionID engine.ExecutionID) ([]engine.StepExecution, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM executions WHERE id = $1::text::uuid)", string(executionID)).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check parent execution: %w", err)
	}
	if !exists {
		return nil, engine.ErrNotFound
	}
	rows, err := r.pool.Query(ctx, "SELECT "+stepColumns+" FROM step_executions WHERE execution_id = $1::text::uuid ORDER BY id", string(executionID))
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	defer rows.Close()
	out := make([]engine.StepExecution, 0)
	for rows.Next() {
		step, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	return out, nil
}

// scanStep decodes a step row while preserving SQL NULL and JSON null.
func scanStep(row pgx.Row) (engine.StepExecution, error) {
	var step engine.StepExecution
	var id int64
	var executionID, status string
	var output, failure *string
	err := row.Scan(&id, &executionID, &step.StepID, &status, &output, &failure, &step.StartedAt, &step.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.StepExecution{}, engine.ErrNotFound
	}
	if err != nil {
		return engine.StepExecution{}, fmt.Errorf("scan step: %w", err)
	}
	step.ID = engine.StepExecutionID(id)
	step.ExecutionID = engine.ExecutionID(executionID)
	step.Status = engine.StepStatus(status)
	if output != nil {
		step.Output = json.RawMessage(*output)
	}
	if failure != nil {
		if err := json.Unmarshal([]byte(*failure), &step.Error); err != nil {
			return engine.StepExecution{}, fmt.Errorf("decode step error: %w", err)
		}
	}
	return step, nil
}

// StartStep transitions a pending step to running, records its start time, and
// emits step.started atomically.
//
// Parameters:
//   - id: the pending step execution to start.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the step is not pending.
func (r *ExecutionRepository) StartStep(ctx context.Context, id engine.StepExecutionID) error {
	return r.transitionStep(ctx, id, engine.StepRunning, nil, nil)
}

// CompleteStep transitions a running step to completed, persists its immutable
// output, and emits step.completed atomically.
//
// Parameters:
//   - id: the running step execution to complete.
//   - output: the immutable successful output to persist.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the step is not running.
func (r *ExecutionRepository) CompleteStep(ctx context.Context, id engine.StepExecutionID, output json.RawMessage) error {
	return r.transitionStep(ctx, id, engine.StepCompleted, output, nil)
}

// FailStep transitions a running step to failed, persists its terminal error,
// and emits step.failed atomically.
//
// Parameters:
//   - id: the running step execution to fail.
//   - execErr: the terminal step error to persist.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the step is not running.
func (r *ExecutionRepository) FailStep(ctx context.Context, id engine.StepExecutionID, execErr engine.ExecutionError) error {
	return r.transitionStep(ctx, id, engine.StepFailed, nil, &execErr)
}

// CancelStep transitions a pending or running step to cancelled and emits
// step.cancelled atomically. Repeating the operation after cancellation is idempotent.
//
// Parameters:
//   - id: the pending or running step execution to cancel.
//
// Returns:
//   - error: nil when cancellation succeeds or is already complete,
//     ErrNotFound when id does not exist, or ErrInvalidTransition when the
//     step is in another terminal state.
func (r *ExecutionRepository) CancelStep(ctx context.Context, id engine.StepExecutionID) error {
	return r.transitionStep(ctx, id, engine.StepCancelled, nil, nil)
}

// lockExecution acquires the parent lock before any child row lock.
// All event-producing operations for an execution share this lock.
func lockExecution(ctx context.Context, tx pgx.Tx, id engine.ExecutionID) error {
	var found string
	err := tx.QueryRow(ctx, "SELECT id::text FROM executions WHERE id = $1::text::uuid FOR UPDATE", string(id)).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.ErrNotFound
	}
	return err
}

// lockStep resolves the immutable parent ID, locks the execution, then locks
// the step and reads its current state. The lock order is execution then step.
func lockStep(ctx context.Context, tx pgx.Tx, id engine.StepExecutionID) (engine.ExecutionID, engine.StepStatus, error) {
	var parent, status string
	err := tx.QueryRow(ctx, "SELECT execution_id::text FROM step_executions WHERE id = $1", int64(id)).Scan(&parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", engine.ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	executionID := engine.ExecutionID(parent)
	if err := lockExecution(ctx, tx, executionID); err != nil {
		return "", "", err
	}
	err = tx.QueryRow(ctx, "SELECT status FROM step_executions WHERE id = $1 FOR UPDATE", int64(id)).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", engine.ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	return executionID, engine.StepStatus(status), nil
}

// transitionStep validates the locked state and atomically writes the new
// state and its event. Completed output cannot be overwritten.
func (r *ExecutionRepository) transitionStep(ctx context.Context, id engine.StepExecutionID, target engine.StepStatus, output json.RawMessage, failure *engine.ExecutionError) error {
	return r.withTransaction(ctx, func(tx pgx.Tx) error {
		parent, from, err := lockStep(ctx, tx, id)
		if err != nil {
			return err
		}
		if target == engine.StepCancelled && from == engine.StepCancelled {
			return nil
		}
		var allowed bool
		var event engine.ExecutionEventType
		switch target {
		case engine.StepRunning:
			allowed = from == engine.StepPending
			event = engine.EventStepStarted
		case engine.StepCompleted:
			allowed = from == engine.StepRunning
			event = engine.EventStepCompleted
		case engine.StepFailed:
			allowed = from == engine.StepRunning
			event = engine.EventStepFailed
		case engine.StepCancelled:
			allowed = from == engine.StepPending || from == engine.StepRunning
			event = engine.EventStepCancelled
		}
		if !allowed {
			return fmt.Errorf("%w: step %s -> %s", engine.ErrInvalidTransition, from, target)
		}
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return err
		}
		var encoded json.RawMessage
		if failure != nil {
			encoded, err = json.Marshal(failure)
			if err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE step_executions SET status = $2,
			started_at = CASE WHEN $2 = 'running' THEN $3::timestamptz ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed', 'failed', 'cancelled') THEN $3::timestamptz ELSE completed_at END,
			output = $4, error = $5 WHERE id = $1`, int64(id), string(target), now, output, encoded)
		if err != nil {
			return err
		}
		return appendStepEvent(ctx, tx, parent, id, event, now)
	})
}

// appendStepEvent inserts a step-scoped event in the caller's transaction.
func appendStepEvent(ctx context.Context, tx pgx.Tx, parent engine.ExecutionID, id engine.StepExecutionID, event engine.ExecutionEventType, created time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO execution_events (execution_id, step_execution_id, type, created_at)
		VALUES ($1::text::uuid, $2, $3, $4)`, string(parent), int64(id), string(event), created)
	return err
}
