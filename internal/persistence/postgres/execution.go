package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jyork/pandectrun/internal/engine"
)

const executionColumns = "id::text, workflow_id, workflow_version, status, input::text, output::text, error::text, created_at, started_at, completed_at"

// CreateExecution creates a pending execution, assigns its ID and creation time,
// and records an execution.created event atomically.
//
// Parameters:
//   - in: caller-supplied workflow identity, version, and input for the new execution.
//
// Returns:
//   - ExecutionID: the repository-assigned ID of the new execution.
//   - error: nil on success; otherwise an error preventing creation.
func (r *ExecutionRepository) CreateExecution(ctx context.Context, in engine.NewExecution) (engine.ExecutionID, error) {
	id := engine.ExecutionID(uuid.NewV4().String())
	err := r.withTransaction(ctx, func(tx pgx.Tx) error {
		var created time.Time
		err := tx.QueryRow(ctx, `INSERT INTO executions (id, workflow_id, workflow_version, input)
			VALUES ($1::text::uuid, $2, $3, $4) RETURNING created_at`, string(id), in.WorkflowID, in.WorkflowVersion, in.Input).Scan(&created)
		if err != nil {
			return err
		}
		return appendExecutionEvent(ctx, tx, id, engine.EventExecutionCreated, created)
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

// GetExecution returns a copy of the execution identified by id.
// It returns ErrNotFound when the execution does not exist.
//
// Parameters:
//   - id: the execution ID to retrieve.
//
// Returns:
//   - Execution: a detached copy of the persisted execution.
//   - error: nil on success, or ErrNotFound when id does not exist.
func (r *ExecutionRepository) GetExecution(ctx context.Context, id engine.ExecutionID) (engine.Execution, error) {
	return scanExecution(r.pool.QueryRow(ctx, "SELECT "+executionColumns+" FROM executions WHERE id = $1::text::uuid", string(id)))
}

// ListExecutions returns executions matching q in deterministic creation order.
// A positive Limit bounds the result; zero or a negative value is unbounded.
//
// Parameters:
//   - q: optional workflow, status, and result-limit filters.
//
// Returns:
//   - []Execution: detached execution records matching q in deterministic order.
//   - error: nil on success.
func (r *ExecutionRepository) ListExecutions(ctx context.Context, q engine.ExecutionQuery) ([]engine.Execution, error) {
	statuses := make([]string, 0, len(q.Status))
	for _, status := range q.Status {
		statuses = append(statuses, string(status))
	}
	var limit *int
	if q.Limit > 0 {
		limit = &q.Limit
	}
	rows, err := r.pool.Query(ctx, "SELECT "+executionColumns+` FROM executions
		WHERE ($1 = '' OR workflow_id = $1)
		AND (cardinality($2::text[]) = 0 OR status = ANY($2::text[]))
		ORDER BY created_at, id LIMIT $3`, q.WorkflowID, statuses, limit)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()
	out := make([]engine.Execution, 0)
	for rows.Next() {
		e, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	return out, nil
}

// scanExecution decodes a row, preserving SQL NULL versus JSON null for payloads.
func scanExecution(row pgx.Row) (engine.Execution, error) {
	var e engine.Execution
	var id, status string
	var input, output, failure *string
	err := row.Scan(&id, &e.WorkflowID, &e.WorkflowVersion, &status, &input, &output, &failure, &e.CreatedAt, &e.StartedAt, &e.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.Execution{}, engine.ErrNotFound
	}
	if err != nil {
		return engine.Execution{}, fmt.Errorf("scan execution: %w", err)
	}
	e.ID = engine.ExecutionID(id)
	e.Status = engine.ExecutionStatus(status)
	if input != nil {
		e.Input = json.RawMessage(*input)
	}
	if output != nil {
		e.Output = json.RawMessage(*output)
	}
	if failure != nil {
		if err := json.Unmarshal([]byte(*failure), &e.Error); err != nil {
			return engine.Execution{}, fmt.Errorf("decode execution error: %w", err)
		}
	}
	return e, nil
}

// StartExecution transitions a pending execution to running, records its start time,
// and emits execution.started atomically.
//
// Parameters:
//   - id: the pending execution to start.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the execution is not pending.
func (r *ExecutionRepository) StartExecution(ctx context.Context, id engine.ExecutionID) error {
	return r.transitionExecution(ctx, id, engine.ExecutionRunning, nil, nil)
}

// CompleteExecution transitions a running or cancel-requested execution to completed,
// persists its output, and emits execution.completed atomically.
//
// Parameters:
//   - id: the running or cancel-requested execution to complete.
//   - output: the final workflow output to persist.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the execution cannot transition to completed.
func (r *ExecutionRepository) CompleteExecution(ctx context.Context, id engine.ExecutionID, output json.RawMessage) error {
	return r.transitionExecution(ctx, id, engine.ExecutionCompleted, output, nil)
}

// FailExecution transitions a running or cancel-requested execution to failed,
// persists the terminal error, and emits execution.failed atomically.
//
// Parameters:
//   - id: the running or cancel-requested execution to fail.
//   - execErr: the terminal error to persist.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the execution cannot transition to failed.
func (r *ExecutionRepository) FailExecution(ctx context.Context, id engine.ExecutionID, execErr engine.ExecutionError) error {
	return r.transitionExecution(ctx, id, engine.ExecutionFailed, nil, &execErr)
}

// RequestCancellation records cancellation intent for a pending or running execution.
// Repeated requests for cancel-requested or cancelled executions are idempotent.
//
// Parameters:
//   - id: the pending or running execution for which cancellation is requested.
//
// Returns:
//   - error: nil when cancellation is requested or already effective,
//     ErrNotFound when id does not exist, or ErrInvalidTransition when the
//     execution is already in another terminal state.
func (r *ExecutionRepository) RequestCancellation(ctx context.Context, id engine.ExecutionID) error {
	return r.transitionExecution(ctx, id, engine.ExecutionCancelRequested, nil, nil)
}

// CompleteCancellation transitions a cancel-requested execution to cancelled and
// records its completion time. Repeating the operation after cancellation is idempotent.
//
// Parameters:
//   - id: the cancel-requested execution to finalize as cancelled.
//
// Returns:
//   - error: nil when cancellation is completed or already complete,
//     ErrNotFound when id does not exist, or ErrInvalidTransition when the
//     execution is not cancel-requested.
func (r *ExecutionRepository) CompleteCancellation(ctx context.Context, id engine.ExecutionID) error {
	return r.transitionExecution(ctx, id, engine.ExecutionCancelled, nil, nil)
}

// transitionExecution locks the execution before validating and changing its state.
// The lock serializes lifecycle mutations and their associated event writes.
func (r *ExecutionRepository) transitionExecution(ctx context.Context, id engine.ExecutionID, target engine.ExecutionStatus, output json.RawMessage, failure *engine.ExecutionError) error {
	return r.withTransaction(ctx, func(tx pgx.Tx) error {
		var current string
		err := tx.QueryRow(ctx, "SELECT status FROM executions WHERE id = $1::text::uuid FOR UPDATE", string(id)).Scan(&current)
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ErrNotFound
		}
		if err != nil {
			return err
		}
		from := engine.ExecutionStatus(current)
		if target == engine.ExecutionCancelRequested && (from == engine.ExecutionCancelRequested || from == engine.ExecutionCancelled) ||
			target == engine.ExecutionCancelled && from == engine.ExecutionCancelled {
			return nil
		}

		var event engine.ExecutionEventType
		var allowed bool
		switch target {
		case engine.ExecutionRunning:
			allowed = from == engine.ExecutionPending
			event = engine.EventExecutionStarted
		case engine.ExecutionCancelRequested:
			allowed = from == engine.ExecutionPending || from == engine.ExecutionRunning
			event = engine.EventExecutionCancelRequested
		case engine.ExecutionCompleted:
			allowed = from == engine.ExecutionRunning || from == engine.ExecutionCancelRequested
			event = engine.EventExecutionCompleted
		case engine.ExecutionFailed:
			allowed = from == engine.ExecutionRunning || from == engine.ExecutionCancelRequested
			event = engine.EventExecutionFailed
		case engine.ExecutionCancelled:
			allowed = from == engine.ExecutionCancelRequested
			event = engine.EventExecutionCancelled
		}
		if !allowed {
			return fmt.Errorf("%w: execution %s -> %s", engine.ErrInvalidTransition, from, target)
		}

		// Capture time after acquiring the lock, not at transaction start.
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
		_, err = tx.Exec(ctx, `UPDATE executions SET status = $2,
			started_at = CASE WHEN $2 = 'running' THEN $3::timestamptz ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed', 'failed', 'cancelled') THEN $3 ELSE completed_at END,
			output = $4, error = $5 WHERE id = $1::text::uuid`, string(id), string(target), now, output, encoded)
		if err != nil {
			return err
		}
		return appendExecutionEvent(ctx, tx, id, event, now)
	})
}

// appendExecutionEvent inserts an execution-scoped event within the caller's transaction.
func appendExecutionEvent(ctx context.Context, tx pgx.Tx, id engine.ExecutionID, event engine.ExecutionEventType, created time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO execution_events (execution_id, type, created_at)
		VALUES ($1::text::uuid, $2, $3)`, string(id), string(event), created)
	return err
}
