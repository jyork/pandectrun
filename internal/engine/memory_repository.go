package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryExecutionRepository is an in-memory ExecutionRepository implementation.
// It serializes mutations with a mutex so lifecycle state and emitted events are
// updated atomically, matching the transaction boundary expected from durable stores.
type MemoryExecutionRepository struct {
	mu sync.RWMutex

	executions map[ExecutionID]Execution
	steps      map[StepExecutionID]StepExecution
	attempts   map[StepAttemptID]StepAttempt
	events     map[ExecutionID][]ExecutionEvent

	nextExecution uint64
	nextStep      uint64
	nextAttempt   uint64
}

var _ ExecutionRepository = (*MemoryExecutionRepository)(nil)

// NewMemoryExecutionRepository returns an empty repository suitable for local
// execution and tests. Runtime IDs are generated within this repository instance.
func NewMemoryExecutionRepository() *MemoryExecutionRepository {
	return &MemoryExecutionRepository{
		executions: make(map[ExecutionID]Execution),
		steps:      make(map[StepExecutionID]StepExecution),
		attempts:   make(map[StepAttemptID]StepAttempt),
		events:     make(map[ExecutionID][]ExecutionEvent),
	}
}

func (r *MemoryExecutionRepository) CreateExecution(_ context.Context, in NewExecution) (ExecutionID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextExecution++
	id := ExecutionID(fmt.Sprintf("exec-%d", r.nextExecution))
	now := time.Now().UTC()
	r.executions[id] = Execution{ID: id, WorkflowID: in.WorkflowID, WorkflowVersion: in.WorkflowVersion, Status: ExecutionPending, Input: cloneRawMessage(in.Input), CreatedAt: now}
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionCreated, CreatedAt: now})
	return id, nil
}

func (r *MemoryExecutionRepository) GetExecution(_ context.Context, id ExecutionID) (Execution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.executions[id]
	if !ok {
		return Execution{}, ErrNotFound
	}
	return cloneExecution(e), nil
}

func (r *MemoryExecutionRepository) ListExecutions(_ context.Context, q ExecutionQuery) ([]Execution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	statuses := make(map[ExecutionStatus]struct{}, len(q.Status))
	for _, s := range q.Status {
		statuses[s] = struct{}{}
	}
	out := make([]Execution, 0)
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

func (r *MemoryExecutionRepository) StartExecution(_ context.Context, id ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]
	if !ok {
		return ErrNotFound
	}
	if e.Status != ExecutionPending {
		return transitionError("execution", string(e.Status), string(ExecutionRunning))
	}
	now := time.Now().UTC()
	e.Status = ExecutionRunning
	e.StartedAt = &now
	r.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionStarted, CreatedAt: now})
	return nil
}

func (r *MemoryExecutionRepository) CompleteExecution(_ context.Context, id ExecutionID, output json.RawMessage) error {
	return r.finishExecution(id, ExecutionCompleted, output, nil, EventExecutionCompleted)
}

func (r *MemoryExecutionRepository) FailExecution(_ context.Context, id ExecutionID, execErr ExecutionError) error {
	return r.finishExecution(id, ExecutionFailed, nil, &execErr, EventExecutionFailed)
}

func (r *MemoryExecutionRepository) finishExecution(id ExecutionID, status ExecutionStatus, output json.RawMessage, execErr *ExecutionError, eventType ExecutionEventType) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]; if !ok { return ErrNotFound }
	if e.Status != ExecutionRunning && e.Status != ExecutionCancelRequested {
		return transitionError("execution", string(e.Status), string(status))
	}
	now := time.Now().UTC()
	e.Status = status
	e.Output = cloneRawMessage(output)
	e.Error = cloneExecutionError(execErr)
	e.CompletedAt = &now
	r.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: eventType, CreatedAt: now})
	return nil
}

func (r *MemoryExecutionRepository) RequestCancellation(_ context.Context, id ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]; if !ok { return ErrNotFound }
	if e.Status == ExecutionCancelRequested || e.Status == ExecutionCancelled {
		return nil
	}
	if e.Status != ExecutionPending && e.Status != ExecutionRunning {
		return transitionError("execution", string(e.Status), string(ExecutionCancelRequested))
	}
	now := time.Now().UTC()
	e.Status = ExecutionCancelRequested
	r.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionCancelRequested, CreatedAt: now})
	return nil
}

func (r *MemoryExecutionRepository) CompleteCancellation(_ context.Context, id ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]; if !ok { return ErrNotFound }
	if e.Status == ExecutionCancelled {
		return nil
	}
	if e.Status != ExecutionCancelRequested {
		return transitionError("execution", string(e.Status), string(ExecutionCancelled))
	}
	now := time.Now().UTC()
	e.Status = ExecutionCancelled
	e.CompletedAt = &now
	r.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionCancelled, CreatedAt: now})
	return nil
}

func (r *MemoryExecutionRepository) CreateStepExecution(_ context.Context, in NewStepExecution) (StepExecutionID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.executions[in.ExecutionID]; !ok {
		return "", ErrNotFound
	}
	r.nextStep++
	id := StepExecutionID(fmt.Sprintf("step-%d", r.nextStep))
	r.steps[id] = StepExecution{ID: id, ExecutionID: in.ExecutionID, StepID: in.StepID, Status: StepPending}
	return id, nil
}

func (r *MemoryExecutionRepository) GetStepExecution(_ context.Context, id StepExecutionID) (StepExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.steps[id]
	if !ok {
		return StepExecution{}, ErrNotFound
	}
	return cloneStepExecution(s), nil
}

func (r *MemoryExecutionRepository) ListStepExecutions(_ context.Context, executionID ExecutionID) ([]StepExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.executions[executionID]; !ok {
		return nil, ErrNotFound
	}
	out := make([]StepExecution, 0)
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

func (r *MemoryExecutionRepository) StartStep(_ context.Context, id StepExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
	if !ok {
		return ErrNotFound
	}
	if s.Status != StepPending {
		return transitionError("step", string(s.Status), string(StepRunning))
	}
	now := time.Now().UTC()
	s.Status = StepRunning
	s.StartedAt = &now
	r.steps[id] = s
	r.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepStarted, StepExecutionID: id, CreatedAt: now})
	return nil
}

func (r *MemoryExecutionRepository) CompleteStep(_ context.Context, id StepExecutionID, output json.RawMessage) error {
	return r.finishStep(id, StepCompleted, output, nil, EventStepCompleted)
}
func (r *MemoryExecutionRepository) FailStep(_ context.Context, id StepExecutionID, execErr ExecutionError) error {
	return r.finishStep(id, StepFailed, nil, &execErr, EventStepFailed)
}
func (r *MemoryExecutionRepository) CancelStep(_ context.Context, id StepExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
	if !ok {
		return ErrNotFound
	}
	if s.Status == StepCancelled {
		return nil
	}
	if s.Status != StepPending && s.Status != StepRunning {
		return transitionError("step", string(s.Status), string(StepCancelled))
	}
	now := time.Now().UTC()
	s.Status = StepCancelled
	s.CompletedAt = &now
	r.steps[id] = s
	r.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepCancelled, StepExecutionID: id, CreatedAt: now})
	return nil
}
func (r *MemoryExecutionRepository) finishStep(id StepExecutionID, status StepStatus, output json.RawMessage, execErr *ExecutionError, eventType ExecutionEventType) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
	if !ok {
		return ErrNotFound
	}
	if s.Status != StepRunning {
		return transitionError("step", string(s.Status), string(status))
	}
	now := time.Now().UTC()
	s.Status = status
	s.Output = cloneRawMessage(output)
	s.Error = cloneExecutionError(execErr)
	s.CompletedAt = &now
	r.steps[id] = s
	r.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: eventType, StepExecutionID: id, CreatedAt: now})
	return nil
}

func (r *MemoryExecutionRepository) CreateStepAttempt(_ context.Context, in NewStepAttempt) (StepAttemptID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[in.StepExecutionID]
	if !ok {
		return "", ErrNotFound
	}
	if s.Status != StepRunning {
		return "", transitionError("step", string(s.Status), "create attempt")
	}
	r.nextAttempt++
	id := StepAttemptID(fmt.Sprintf("attempt-%d", r.nextAttempt))
	now := time.Now().UTC()
	r.attempts[id] = StepAttempt{ID: id, StepExecutionID: in.StepExecutionID, Attempt: in.Attempt, Status: StepAttemptRunning, StartedAt: now}
	r.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepAttemptStarted, StepExecutionID: s.ID, StepAttemptID: id, CreatedAt: now})
	return id, nil
}
func (r *MemoryExecutionRepository) CompleteStepAttempt(_ context.Context, id StepAttemptID) error {
	return r.finishAttempt(id, StepAttemptCompleted, nil)
}
func (r *MemoryExecutionRepository) FailStepAttempt(_ context.Context, id StepAttemptID, execErr ExecutionError) error {
	return r.finishAttempt(id, StepAttemptFailed, &execErr)
}
func (r *MemoryExecutionRepository) CancelStepAttempt(_ context.Context, id StepAttemptID) error {
	return r.finishAttempt(id, StepAttemptCancelled, nil)
}
func (r *MemoryExecutionRepository) finishAttempt(id StepAttemptID, status StepAttemptStatus, execErr *ExecutionError) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.attempts[id]
	if !ok {
		return ErrNotFound
	}
	if a.Status == status && status == StepAttemptCancelled {
		return nil
	}
	if a.Status != StepAttemptRunning {
		return transitionError("step attempt", string(a.Status), string(status))
	}
	s := r.steps[a.StepExecutionID]
	now := time.Now().UTC()
	a.Status = status
	a.Error = cloneExecutionError(execErr)
	a.CompletedAt = &now
	r.attempts[id] = a
	if status == StepAttemptFailed {
		r.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepAttemptFailed, StepExecutionID: s.ID, StepAttemptID: id, CreatedAt: now})
	}
	return nil
}
func (r *MemoryExecutionRepository) ListStepAttempts(_ context.Context, stepID StepExecutionID) ([]StepAttempt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.steps[stepID]; !ok {
		return nil, ErrNotFound
	}
	out := make([]StepAttempt, 0)
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
func (r *MemoryExecutionRepository) ListEvents(_ context.Context, executionID ExecutionID) ([]ExecutionEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.executions[executionID]; !ok {
		return nil, ErrNotFound
	}
	return append([]ExecutionEvent(nil), r.events[executionID]...), nil
}
func (r *MemoryExecutionRepository) appendEventLocked(id ExecutionID, event ExecutionEvent) {
	r.events[id] = append(r.events[id], event)
}
func transitionError(entity, from, to string) error {
	return fmt.Errorf("%w: %s %s -> %s", ErrInvalidTransition, entity, from, to)
}
func cloneExecution(e Execution) Execution {
	e.Input = cloneRawMessage(e.Input)
	e.Output = cloneRawMessage(e.Output)
	e.Error = cloneExecutionError(e.Error)
	e.StartedAt = cloneTime(e.StartedAt)
	e.CompletedAt = cloneTime(e.CompletedAt)
	return e
}
func cloneStepExecution(s StepExecution) StepExecution {
	s.Output = cloneRawMessage(s.Output)
	s.Error = cloneExecutionError(s.Error)
	s.StartedAt = cloneTime(s.StartedAt)
	s.CompletedAt = cloneTime(s.CompletedAt)
	return s
}
func cloneStepAttempt(a StepAttempt) StepAttempt {
	a.Error = cloneExecutionError(a.Error)
	a.CompletedAt = cloneTime(a.CompletedAt)
	return a
}
func cloneExecutionError(e *ExecutionError) *ExecutionError {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}
func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}
