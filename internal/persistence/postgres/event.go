package postgres

import (
	"context"
	"fmt"

	"github.com/jyork/pandectrun/internal/engine"
)

// ListEvents returns a copy of the chronological event history for executionID.
// It returns ErrNotFound when the execution does not exist.
//
// Parameters:
//   - executionID: the execution whose lifecycle history is requested.
//
// Returns:
//   - []ExecutionEvent: a copy of the execution's chronological event history.
//   - error: nil on success, or ErrNotFound when executionID does not exist.
func (r *ExecutionRepository) ListEvents(ctx context.Context, executionID engine.ExecutionID) ([]engine.ExecutionEvent, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM executions WHERE id = $1::text::uuid)", string(executionID)).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check event parent: %w", err)
	}
	if !exists {
		return nil, engine.ErrNotFound
	}
	rows, err := r.pool.Query(ctx, `SELECT type, step_execution_id, step_attempt_id, created_at
		FROM execution_events WHERE execution_id = $1::text::uuid ORDER BY id`, string(executionID))
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	out := make([]engine.ExecutionEvent, 0)
	for rows.Next() {
		var event engine.ExecutionEvent
		var kind string
		var step, attempt *int64
		if err := rows.Scan(&kind, &step, &attempt, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		event.ExecutionID = executionID
		event.Type = engine.ExecutionEventType(kind)
		if step != nil {
			event.StepExecutionID = engine.StepExecutionID(*step)
		}
		if attempt != nil {
			event.StepAttemptID = engine.StepAttemptID(*attempt)
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return out, nil
}
