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
\t\treturn Execution{}, ErrNotFound
\t}
	return cloneExecution(e), nil
}

func (r *MemoryExecutionRepository) ListExecutions(_ context.Context, q ExecutionQuery) ([]Execution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	statuses := make(map[ExecutionStatus]struct{}, len(q.Status))
	for _, s := range q.Status {
\t\tstatuses[s] = struct{}{}
\t}
	out := make([]Execution, 0)
	for _, e := range r.executions {
		if q.WorkflowID != "" && e.WorkflowID != q.WorkflowID {
\t\t\tcontinue
\t\t}
		if len(statuses) > 0 {
\t\t\tif _, ok := statuses[e.Status]; !ok {
\t\t\t\tcontinue
\t\t\t}
\t\t}
		out = append(out, cloneExecution(e))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
\t\t\treturn out[i].ID < out[j].ID
\t\t}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if q.Limit > 0 && len(out) > q.Limit {
\t\tout = out[:q.Limit]
\t}
	return out, nil
}

func (r *MemoryExecutionRepository) StartExecution(_ context.Context, id ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]
\tif !ok {
\t\treturn ErrNotFound
\t}
	if e.Status != ExecutionPending {
\t\treturn transitionError("execution", string(e.Status), string(ExecutionRunning))
\t}
	now := time.Now().UTC()
\te.Status = ExecutionRunning
\te.StartedAt = &now
\tr.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionStarted, CreatedAt: now})
\treturn nil
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
\t\treturn transitionError("execution", string(e.Status), string(status))
\t}
	now := time.Now().UTC()
\te.Status = status
\te.Output = cloneRawMessage(output)
\te.Error = cloneExecutionError(execErr)
\te.CompletedAt = &now
\tr.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: eventType, CreatedAt: now})
\treturn nil
}

func (r *MemoryExecutionRepository) RequestCancellation(_ context.Context, id ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]; if !ok { return ErrNotFound }
	if e.Status == ExecutionCancelRequested || e.Status == ExecutionCancelled {
\t\treturn nil
\t}
	if e.Status != ExecutionPending && e.Status != ExecutionRunning {
\t\treturn transitionError("execution", string(e.Status), string(ExecutionCancelRequested))
\t}
	now := time.Now().UTC()
\te.Status = ExecutionCancelRequested
\tr.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionCancelRequested, CreatedAt: now})
\treturn nil
}

func (r *MemoryExecutionRepository) CompleteCancellation(_ context.Context, id ExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.executions[id]; if !ok { return ErrNotFound }
	if e.Status == ExecutionCancelled {
\t\treturn nil
\t}
	if e.Status != ExecutionCancelRequested {
\t\treturn transitionError("execution", string(e.Status), string(ExecutionCancelled))
\t}
	now := time.Now().UTC()
\te.Status = ExecutionCancelled
\te.CompletedAt = &now
\tr.executions[id] = e
	r.appendEventLocked(id, ExecutionEvent{ExecutionID: id, Type: EventExecutionCancelled, CreatedAt: now})
\treturn nil
}

func (r *MemoryExecutionRepository) CreateStepExecution(_ context.Context, in NewStepExecution) (StepExecutionID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.executions[in.ExecutionID]; !ok {
\t\treturn "", ErrNotFound
\t}
	r.nextStep++
\tid := StepExecutionID(fmt.Sprintf("step-%d", r.nextStep))
	r.steps[id] = StepExecution{ID: id, ExecutionID: in.ExecutionID, StepID: in.StepID, Status: StepPending}
\treturn id, nil
}

func (r *MemoryExecutionRepository) GetStepExecution(_ context.Context, id StepExecutionID) (StepExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.steps[id]
\tif !ok {
\t\treturn StepExecution{}, ErrNotFound
\t}
\treturn cloneStepExecution(s), nil
}

func (r *MemoryExecutionRepository) ListStepExecutions(_ context.Context, executionID ExecutionID) ([]StepExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.executions[executionID]; !ok {
\t\treturn nil, ErrNotFound
\t}
	out := make([]StepExecution, 0)
\tfor _, s := range r.steps {
\t\tif s.ExecutionID == executionID {
\t\t\tout = append(out, cloneStepExecution(s))
\t\t}
\t}
	sort.Slice(out, func(i, j int) bool {
\t\treturn out[i].ID < out[j].ID
\t})
\treturn out, nil
}

func (r *MemoryExecutionRepository) StartStep(_ context.Context, id StepExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
\tif !ok {
\t\treturn ErrNotFound
\t}
\tif s.Status != StepPending {
\t\treturn transitionError("step", string(s.Status), string(StepRunning))
\t}
	now := time.Now().UTC()
\ts.Status = StepRunning
\ts.StartedAt = &now
\tr.steps[id] = s
\tr.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepStarted, StepExecutionID: id, CreatedAt: now})
\treturn nil
}

func (r *MemoryExecutionRepository) CompleteStep(_ context.Context, id StepExecutionID, output json.RawMessage) error {
\treturn r.finishStep(id, StepCompleted, output, nil, EventStepCompleted)
}
func (r *MemoryExecutionRepository) FailStep(_ context.Context, id StepExecutionID, execErr ExecutionError) error {
\treturn r.finishStep(id, StepFailed, nil, &execErr, EventStepFailed)
}
func (r *MemoryExecutionRepository) CancelStep(_ context.Context, id StepExecutionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
\tif !ok {
\t\treturn ErrNotFound
\t}
\tif s.Status == StepCancelled {
\t\treturn nil
\t}
\tif s.Status != StepPending && s.Status != StepRunning {
\t\treturn transitionError("step", string(s.Status), string(StepCancelled))
\t}
	now := time.Now().UTC()
\ts.Status = StepCancelled
\ts.CompletedAt = &now
\tr.steps[id] = s
\tr.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepCancelled, StepExecutionID: id, CreatedAt: now})
\treturn nil
}
func (r *MemoryExecutionRepository) finishStep(id StepExecutionID, status StepStatus, output json.RawMessage, execErr *ExecutionError, eventType ExecutionEventType) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[id]
\tif !ok {
\t\treturn ErrNotFound
\t}
\tif s.Status != StepRunning {
\t\treturn transitionError("step", string(s.Status), string(status))
\t}
	now := time.Now().UTC()
\ts.Status = status
\ts.Output = cloneRawMessage(output)
\ts.Error = cloneExecutionError(execErr)
\ts.CompletedAt = &now
\tr.steps[id] = s
\tr.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: eventType, StepExecutionID: id, CreatedAt: now})
\treturn nil
}

func (r *MemoryExecutionRepository) CreateStepAttempt(_ context.Context, in NewStepAttempt) (StepAttemptID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.steps[in.StepExecutionID]
\tif !ok {
\t\treturn "", ErrNotFound
\t}
\tif s.Status != StepRunning {
\t\treturn "", transitionError("step", string(s.Status), "create attempt")
\t}
	r.nextAttempt++
\tid := StepAttemptID(fmt.Sprintf("attempt-%d", r.nextAttempt))
\tnow := time.Now().UTC()
\tr.attempts[id] = StepAttempt{ID: id, StepExecutionID: in.StepExecutionID, Attempt: in.Attempt, Status: StepAttemptRunning, StartedAt: now}
\tr.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepAttemptStarted, StepExecutionID: s.ID, StepAttemptID: id, CreatedAt: now})
\treturn id, nil
}
func (r *MemoryExecutionRepository) CompleteStepAttempt(_ context.Context, id StepAttemptID) error {
\treturn r.finishAttempt(id, StepAttemptCompleted, nil)
}
func (r *MemoryExecutionRepository) FailStepAttempt(_ context.Context, id StepAttemptID, execErr ExecutionError) error {
\treturn r.finishAttempt(id, StepAttemptFailed, &execErr)
}
func (r *MemoryExecutionRepository) CancelStepAttempt(_ context.Context, id StepAttemptID) error {
\treturn r.finishAttempt(id, StepAttemptCancelled, nil)
}
func (r *MemoryExecutionRepository) finishAttempt(id StepAttemptID, status StepAttemptStatus, execErr *ExecutionError) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.attempts[id]
\tif !ok {
\t\treturn ErrNotFound
\t}
\tif a.Status == status && status == StepAttemptCancelled {
\t\treturn nil
\t}
\tif a.Status != StepAttemptRunning {
\t\treturn transitionError("step attempt", string(a.Status), string(status))
\t}
\ts := r.steps[a.StepExecutionID]
	now := time.Now().UTC()
\ta.Status = status
\ta.Error = cloneExecutionError(execErr)
\ta.CompletedAt = &now
\tr.attempts[id] = a
\tif status == StepAttemptFailed {
\t\tr.appendEventLocked(s.ExecutionID, ExecutionEvent{ExecutionID: s.ExecutionID, Type: EventStepAttemptFailed, StepExecutionID: s.ID, StepAttemptID: id, CreatedAt: now})
\t}
\treturn nil
}
func (r *MemoryExecutionRepository) ListStepAttempts(_ context.Context, stepID StepExecutionID) ([]StepAttempt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.steps[stepID]; !ok {
\t\treturn nil, ErrNotFound
\t}
\tout := make([]StepAttempt, 0)
\tfor _, a := range r.attempts {
\t\tif a.StepExecutionID == stepID {
\t\t\tout = append(out, cloneStepAttempt(a))
\t\t}
\t}
\tsort.Slice(out, func(i, j int) bool {
\t\treturn out[i].Attempt < out[j].Attempt
\t})
\treturn out, nil
}
func (r *MemoryExecutionRepository) ListEvents(_ context.Context, executionID ExecutionID) ([]ExecutionEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.executions[executionID]; !ok {
\t\treturn nil, ErrNotFound
\t}
\treturn append([]ExecutionEvent(nil), r.events[executionID]...), nil
}
func (r *MemoryExecutionRepository) appendEventLocked(id ExecutionID, event ExecutionEvent) {
\tr.events[id] = append(r.events[id], event)
}
func transitionError(entity, from, to string) error {
\treturn fmt.Errorf("%w: %s %s -> %s", ErrInvalidTransition, entity, from, to)
}
func cloneExecution(e Execution) Execution {
\te.Input = cloneRawMessage(e.Input)
\te.Output = cloneRawMessage(e.Output)
\te.Error = cloneExecutionError(e.Error)
\te.StartedAt = cloneTime(e.StartedAt)
\te.CompletedAt = cloneTime(e.CompletedAt)
\treturn e
}
func cloneStepExecution(s StepExecution) StepExecution {
\ts.Output = cloneRawMessage(s.Output)
\ts.Error = cloneExecutionError(s.Error)
\ts.StartedAt = cloneTime(s.StartedAt)
\ts.CompletedAt = cloneTime(s.CompletedAt)
\treturn s
}
func cloneStepAttempt(a StepAttempt) StepAttempt {
\ta.Error = cloneExecutionError(a.Error)
\ta.CompletedAt = cloneTime(a.CompletedAt)
\treturn a
}
func cloneExecutionError(e *ExecutionError) *ExecutionError {
\tif e == nil {
\t\treturn nil
\t}
\tc := *e
\treturn &c
}
func cloneTime(t *time.Time) *time.Time {
\tif t == nil {
\t\treturn nil
\t}
\tc := *t
\treturn &c
}
