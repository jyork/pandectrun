package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
	"uuid"

	"github.com/jyork/pandectrun/internal/engine"
)

// ExecutionRepository is an in-memory ExecutionRepository implementation.
// It serializes mutations with a mutex so lifecycle state and emitted events are
// updated atomically, matching the transaction boundary expected from durable stores.
type ExecutionRepository struct {
	mu sync.RWMutex

	executions map[engine.ExecutionID]engine.Execution
	steps      map[engine.StepExecutionID]engine.StepExecution
	attempts   map[engine.StepAttemptID]engine.StepAttempt
	events     map[engine.ExecutionID][]engine.ExecutionEvent

	nextStep    int64
	nextAttempt int64
}

var _ engine.ExecutionRepository = (*ExecutionRepository)(nil)

// NewExecutionRepository returns an empty repository suitable for local
// execution and tests. Execution IDs are UUIDs; step and attempt IDs are instance-local integers.
//
// Returns:
//   - *ExecutionRepository: a newly initialized, empty repository.
func NewExecutionRepository() *ExecutionRepository {
	return &ExecutionRepository{
		executions: make(map[engine.ExecutionID]engine.Execution),
		steps:      make(map[engine.StepExecutionID]engine.StepExecution),
		attempts:   make(map[engine.StepAttemptID]engine.StepAttempt),
		events:     make(map[engine.ExecutionID][]engine.ExecutionEvent),
	}
}

// CreateExecution creates a pending execution, assigns its ID and creation time,
// and records an execution.created event atomically.
//
// Parameters:
//   - in: caller-supplied workflow identity, version, and input for the new execution.
//
// Returns:
//   - ExecutionID: the repository-assigned ID of the new execution.
//   - error: nil on success; otherwise an error preventing creation.
func (r *ExecutionRepository) CreateExecution(_ context.Context, in engine.NewExecution) (engine.ExecutionID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := engine.ExecutionID(uuid.NewV4().String())
	now := time.Now().UTC()
	r.executions[id] = engine.Execution{ID: id, WorkflowID: in.WorkflowID, WorkflowVersion: in.WorkflowVersion, Status: engine.ExecutionPending, Input: cloneRawMessage(in.Input), CreatedAt: now}
	r.appendEventLocked(id, engine.ExecutionEvent{ExecutionID: id, Type: engine.EventExecutionCreated, CreatedAt: now})
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
func (r *ExecutionRepository) GetExecution(_ context.Context, id engine.ExecutionID) (engine.Execution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.executions[id]
	if !ok {
		return engine.Execution{}, engine.ErrNotFound
	}
	return cloneExecution(e), nil
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
func (r *ExecutionRepository) ListExecutions(_ context.Context, q engine.ExecutionQuery) ([]engine.Execution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	statuses := make(map[engine.ExecutionStatus]struct{}, len(q.Status))
	for _, s := range q.Status {
		statuses[s] = struct{}{}
	}
	out := make([]engine.Execution, 0)
	for _, e := range r.executions {
		if q.WorkflowID != "" && e.WorkflowID != q.WorkflowID {
			continue
		}
		if len(statuses) > 0 {
			if _, ok := statuses[e.Status]; !ok {
				continue
			}
		}
		out = append(out, cloneExecution(e))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
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
func (r *ExecutionRepository) StartExecution(_ context.Context, id engine.ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]
	if !ok {
		return engine.ErrNotFound
	}
	if e.Status != engine.ExecutionPending {
		return transitionError("execution", string(e.Status), string(engine.ExecutionRunning))
	}
	now := time.Now().UTC()
	e.Status = engine.ExecutionRunning
	e.StartedAt = &now
	r.executions[id] = e
	r.appendEventLocked(id, engine.ExecutionEvent{ExecutionID: id, Type: engine.EventExecutionStarted, CreatedAt: now})
	return nil
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
func (r *ExecutionRepository) CompleteExecution(_ context.Context, id engine.ExecutionID, output json.RawMessage) error {
	return r.finishExecution(id, engine.ExecutionCompleted, output, nil, engine.EventExecutionCompleted)
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
func (r *ExecutionRepository) FailExecution(_ context.Context, id engine.ExecutionID, execErr engine.ExecutionError) error {
	return r.finishExecution(id, engine.ExecutionFailed, nil, &execErr, engine.EventExecutionFailed)
}

// finishExecution applies a terminal execution transition and its event while
// holding the repository lock, keeping state and history consistent.
//
// Parameters:
//   - id: the execution to transition.
//   - status: the terminal status to assign.
//   - output: the output to persist for successful completion.
//   - execErr: the terminal error to persist, or nil when none applies.
//   - eventType: the lifecycle event emitted for the transition.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the current state cannot reach status.
func (r *ExecutionRepository) finishExecution(id engine.ExecutionID, status engine.ExecutionStatus, output json.RawMessage, execErr *engine.ExecutionError, eventType engine.ExecutionEventType) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]
	if !ok {
		return engine.ErrNotFound
	}
	if e.Status != engine.ExecutionRunning && e.Status != engine.ExecutionCancelRequested {
		return transitionError("execution", string(e.Status), string(status))
	}
	now := time.Now().UTC()
	e.Status = status
	e.Output = cloneRawMessage(output)
	e.Error = cloneExecutionError(execErr)
	e.CompletedAt = &now
	r.executions[id] = e
	r.appendEventLocked(id, engine.ExecutionEvent{ExecutionID: id, Type: eventType, CreatedAt: now})
	return nil
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
func (r *ExecutionRepository) RequestCancellation(_ context.Context, id engine.ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]
	if !ok {
		return engine.ErrNotFound
	}
	if e.Status == engine.ExecutionCancelRequested || e.Status == engine.ExecutionCancelled {
		return nil
	}
	if e.Status != engine.ExecutionPending && e.Status != engine.ExecutionRunning {
		return transitionError("execution", string(e.Status), string(engine.ExecutionCancelRequested))
	}
	now := time.Now().UTC()
	e.Status = engine.ExecutionCancelRequested
	r.executions[id] = e
	r.appendEventLocked(id, engine.ExecutionEvent{ExecutionID: id, Type: engine.EventExecutionCancelRequested, CreatedAt: now})
	return nil
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
func (r *ExecutionRepository) CompleteCancellation(_ context.Context, id engine.ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]
	if !ok {
		return engine.ErrNotFound
	}
	if e.Status == engine.ExecutionCancelled {
		return nil
	}
	if e.Status != engine.ExecutionCancelRequested {
		return transitionError("execution", string(e.Status), string(engine.ExecutionCancelled))
	}
	now := time.Now().UTC()
	e.Status = engine.ExecutionCancelled
	e.CompletedAt = &now
	r.executions[id] = e
	r.appendEventLocked(id, engine.ExecutionEvent{ExecutionID: id, Type: engine.EventExecutionCancelled, CreatedAt: now})
	return nil
}

// CreateStepExecution creates a pending runtime record for a logical workflow step.
// It returns ErrNotFound when the parent execution does not exist.
//
// Parameters:
//   - in: the parent execution ID and logical workflow step ID.
//
// Returns:
//   - StepExecutionID: the repository-assigned ID of the new step execution.
//   - error: nil on success, or ErrNotFound when the parent execution does not exist.
func (r *ExecutionRepository) CreateStepExecution(_ context.Context, in engine.NewStepExecution) (engine.StepExecutionID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.executions[in.ExecutionID]; !ok {
		return 0, engine.ErrNotFound
	}
	r.nextStep++
	id := engine.StepExecutionID(r.nextStep)
	r.steps[id] = engine.StepExecution{ID: id, ExecutionID: in.ExecutionID, StepID: in.StepID, Status: engine.StepPending}
	return id, nil
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
func (r *ExecutionRepository) GetStepExecution(_ context.Context, id engine.StepExecutionID) (engine.StepExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.steps[id]
	if !ok {
		return engine.StepExecution{}, engine.ErrNotFound
	}
	return cloneStepExecution(s), nil
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
func (r *ExecutionRepository) ListStepExecutions(_ context.Context, executionID engine.ExecutionID) ([]engine.StepExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.executions[executionID]; !ok {
		return nil, engine.ErrNotFound
	}
	out := make([]engine.StepExecution, 0)
	for _, s := range r.steps {
		if s.ExecutionID == executionID {
			out = append(out, cloneStepExecution(s))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out, nil
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
func (r *ExecutionRepository) StartStep(_ context.Context, id engine.StepExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
	if !ok {
		return engine.ErrNotFound
	}
	if s.Status != engine.StepPending {
		return transitionError("step", string(s.Status), string(engine.StepRunning))
	}
	now := time.Now().UTC()
	s.Status = engine.StepRunning
	s.StartedAt = &now
	r.steps[id] = s
	r.appendEventLocked(s.ExecutionID, engine.ExecutionEvent{ExecutionID: s.ExecutionID, Type: engine.EventStepStarted, StepExecutionID: id, CreatedAt: now})
	return nil
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
func (r *ExecutionRepository) CompleteStep(_ context.Context, id engine.StepExecutionID, output json.RawMessage) error {
	return r.finishStep(id, engine.StepCompleted, output, nil, engine.EventStepCompleted)
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
func (r *ExecutionRepository) FailStep(_ context.Context, id engine.StepExecutionID, execErr engine.ExecutionError) error {
	return r.finishStep(id, engine.StepFailed, nil, &execErr, engine.EventStepFailed)
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
func (r *ExecutionRepository) CancelStep(_ context.Context, id engine.StepExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
	if !ok {
		return engine.ErrNotFound
	}
	if s.Status == engine.StepCancelled {
		return nil
	}
	if s.Status != engine.StepPending && s.Status != engine.StepRunning {
		return transitionError("step", string(s.Status), string(engine.StepCancelled))
	}
	now := time.Now().UTC()
	s.Status = engine.StepCancelled
	s.CompletedAt = &now
	r.steps[id] = s
	r.appendEventLocked(s.ExecutionID, engine.ExecutionEvent{ExecutionID: s.ExecutionID, Type: engine.EventStepCancelled, StepExecutionID: id, CreatedAt: now})
	return nil
}

// finishStep applies a terminal step transition and its event while holding the
// repository lock, including the step's output or terminal error.
//
// Parameters:
//   - id: the step execution to transition.
//   - status: the terminal status to assign.
//   - output: the successful output to persist, or nil when none applies.
//   - execErr: the terminal error to persist, or nil when none applies.
//   - eventType: the lifecycle event emitted for the transition.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the current state cannot reach status.
func (r *ExecutionRepository) finishStep(id engine.StepExecutionID, status engine.StepStatus, output json.RawMessage, execErr *engine.ExecutionError, eventType engine.ExecutionEventType) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
	if !ok {
		return engine.ErrNotFound
	}
	if s.Status != engine.StepRunning {
		return transitionError("step", string(s.Status), string(status))
	}
	now := time.Now().UTC()
	s.Status = status
	s.Output = cloneRawMessage(output)
	s.Error = cloneExecutionError(execErr)
	s.CompletedAt = &now
	r.steps[id] = s
	r.appendEventLocked(s.ExecutionID, engine.ExecutionEvent{ExecutionID: s.ExecutionID, Type: eventType, StepExecutionID: id, CreatedAt: now})
	return nil
}

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
func (r *ExecutionRepository) CreateStepAttempt(_ context.Context, in engine.NewStepAttempt) (engine.StepAttemptID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[in.StepExecutionID]
	if !ok {
		return 0, engine.ErrNotFound
	}
	if s.Status != engine.StepRunning {
		return 0, transitionError("step", string(s.Status), "create attempt")
	}
	r.nextAttempt++
	id := engine.StepAttemptID(r.nextAttempt)
	now := time.Now().UTC()
	r.attempts[id] = engine.StepAttempt{ID: id, StepExecutionID: in.StepExecutionID, Attempt: in.Attempt, Status: engine.StepAttemptRunning, StartedAt: now}
	r.appendEventLocked(s.ExecutionID, engine.ExecutionEvent{ExecutionID: s.ExecutionID, Type: engine.EventStepAttemptStarted, StepExecutionID: s.ID, StepAttemptID: id, CreatedAt: now})
	return id, nil
}

// CompleteStepAttempt transitions a running attempt to completed.
//
// Parameters:
//   - id: the running attempt to complete.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the attempt is not running.
func (r *ExecutionRepository) CompleteStepAttempt(_ context.Context, id engine.StepAttemptID) error {
	return r.finishAttempt(id, engine.StepAttemptCompleted, nil)
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
func (r *ExecutionRepository) FailStepAttempt(_ context.Context, id engine.StepAttemptID, execErr engine.ExecutionError) error {
	return r.finishAttempt(id, engine.StepAttemptFailed, &execErr)
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
func (r *ExecutionRepository) RecordStepRetryScheduled(_ context.Context, id engine.StepAttemptID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	attempt, ok := r.attempts[id]
	if !ok {
		return engine.ErrNotFound
	}
	if attempt.Status != engine.StepAttemptFailed {
		return transitionError("step attempt", string(attempt.Status), "retry scheduled")
	}

	step, ok := r.steps[attempt.StepExecutionID]
	if !ok {
		return engine.ErrNotFound
	}

	now := time.Now().UTC()
	r.appendEventLocked(step.ExecutionID, engine.ExecutionEvent{
		ExecutionID:     step.ExecutionID,
		Type:            engine.EventStepRetryScheduled,
		StepExecutionID: step.ID,
		StepAttemptID:   id,
		CreatedAt:       now,
	})
	return nil
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
func (r *ExecutionRepository) CancelStepAttempt(_ context.Context, id engine.StepAttemptID) error {
	return r.finishAttempt(id, engine.StepAttemptCancelled, nil)
}

// finishAttempt applies a terminal attempt transition. Failed attempts also emit
// step.attempt_failed while the repository lock is held.
//
// Parameters:
//   - id: the step attempt to transition.
//   - status: the terminal attempt status to assign.
//   - execErr: the attempt error to persist, or nil when none applies.
//
// Returns:
//   - error: nil on success, ErrNotFound when id does not exist, or
//     ErrInvalidTransition when the attempt is not running.
func (r *ExecutionRepository) finishAttempt(id engine.StepAttemptID, status engine.StepAttemptStatus, execErr *engine.ExecutionError) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.attempts[id]
	if !ok {
		return engine.ErrNotFound
	}
	if a.Status == status && status == engine.StepAttemptCancelled {
		return nil
	}
	if a.Status != engine.StepAttemptRunning {
		return transitionError("step attempt", string(a.Status), string(status))
	}
	s := r.steps[a.StepExecutionID]
	now := time.Now().UTC()
	a.Status = status
	a.Error = cloneExecutionError(execErr)
	a.CompletedAt = &now
	r.attempts[id] = a
	if status == engine.StepAttemptFailed {
		r.appendEventLocked(s.ExecutionID, engine.ExecutionEvent{ExecutionID: s.ExecutionID, Type: engine.EventStepAttemptFailed, StepExecutionID: s.ID, StepAttemptID: id, CreatedAt: now})
	}
	return nil
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
func (r *ExecutionRepository) ListStepAttempts(_ context.Context, stepID engine.StepExecutionID) ([]engine.StepAttempt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.steps[stepID]; !ok {
		return nil, engine.ErrNotFound
	}
	out := make([]engine.StepAttempt, 0)
	for _, a := range r.attempts {
		if a.StepExecutionID == stepID {
			out = append(out, cloneStepAttempt(a))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Attempt < out[j].Attempt
	})
	return out, nil
}

// ListEvents returns a copy of the chronological event history for executionID.
// It returns ErrNotFound when the execution does not exist.
//
// Parameters:
//   - executionID: the execution whose lifecycle history is requested.
//
// Returns:
//   - []ExecutionEvent: a copy of the execution's chronological event history.
//   - error: nil on success, or ErrNotFound when executionID does not exist.
func (r *ExecutionRepository) ListEvents(_ context.Context, executionID engine.ExecutionID) ([]engine.ExecutionEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.executions[executionID]; !ok {
		return nil, engine.ErrNotFound
	}
	return append([]engine.ExecutionEvent(nil), r.events[executionID]...), nil
}

// appendEventLocked appends an event while the caller holds r.mu for writing.
//
// Parameters:
//   - id: the execution whose event history receives event.
//   - event: the lifecycle event to append.
//
// Returns:
//   - none.
func (r *ExecutionRepository) appendEventLocked(id engine.ExecutionID, event engine.ExecutionEvent) {
	r.events[id] = append(r.events[id], event)
}

// transitionError wraps ErrInvalidTransition with the rejected lifecycle change.
//
// Parameters:
//   - entity: the kind of lifecycle entity whose transition was rejected.
//   - from: the entity's current state.
//   - to: the requested target state.
//
// Returns:
//   - error: ErrInvalidTransition wrapped with the rejected state change.
func transitionError(entity, from, to string) error {
	return fmt.Errorf("%w: %s %s -> %s", engine.ErrInvalidTransition, entity, from, to)
}

// cloneExecution returns an execution whose mutable byte slices and pointer fields
// do not alias repository-owned state.
//
// Parameters:
//   - e: the execution value to detach from repository-owned state.
//
// Returns:
//   - Execution: a copy whose mutable fields do not alias e's mutable storage.
func cloneExecution(e engine.Execution) engine.Execution {
	e.Input = cloneRawMessage(e.Input)
	e.Output = cloneRawMessage(e.Output)
	e.Error = cloneExecutionError(e.Error)
	e.StartedAt = cloneTime(e.StartedAt)
	e.CompletedAt = cloneTime(e.CompletedAt)
	return e
}

// cloneStepExecution returns a step execution detached from repository-owned state.
//
// Parameters:
//   - s: the step execution value to detach from repository-owned state.
//
// Returns:
//   - StepExecution: a copy whose mutable fields do not alias s's mutable storage.
func cloneStepExecution(s engine.StepExecution) engine.StepExecution {
	s.Output = cloneRawMessage(s.Output)
	s.Error = cloneExecutionError(s.Error)
	s.StartedAt = cloneTime(s.StartedAt)
	s.CompletedAt = cloneTime(s.CompletedAt)
	return s
}

// cloneStepAttempt returns a step attempt detached from repository-owned state.
//
// Parameters:
//   - a: the step attempt value to detach from repository-owned state.
//
// Returns:
//   - StepAttempt: a copy whose mutable fields do not alias a's mutable storage.
func cloneStepAttempt(a engine.StepAttempt) engine.StepAttempt {
	a.Error = cloneExecutionError(a.Error)
	a.CompletedAt = cloneTime(a.CompletedAt)
	return a
}

// cloneExecutionError returns a copy of e, preserving nil.
//
// Parameters:
//   - e: the execution error to copy.
//
// Returns:
//   - *ExecutionError: an independent copy of e, or nil when e is nil.
func cloneExecutionError(e *engine.ExecutionError) *engine.ExecutionError {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

// cloneTime returns a copy of t, preserving nil.
//
// Parameters:
//   - t: the time value to copy.
//
// Returns:
//   - *time.Time: an independent copy of t, or nil when t is nil.
func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

// cloneRawMessage returns an independent copy of value, preserving nil.
func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copyValue := make(json.RawMessage, len(value))
	copy(copyValue, value)
	return copyValue
}
