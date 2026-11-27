package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jyork/pandectrun/internal/engine"
)

// integrationStep adapts a function to the engine's Step interface.
type integrationStep func(context.Context, engine.StepInput) (engine.StepResult, error)

func (s integrationStep) Execute(ctx context.Context, in engine.StepInput) (engine.StepResult, error) {
	return s(ctx, in)
}

// TestSchedulerPostgresRetryAndReconnect verifies that the scheduler retries a
// transient failure, passes workflow input and dependency output to the next
// step, and completes the workflow using PostgreSQL. It then replaces the
// connection pool and repository to verify that execution state, step outputs,
// attempt history, and ordered lifecycle events remain persisted.
// This tests client reconnection, not database restart or crash recovery.
func TestSchedulerPostgresRetryAndReconnect(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fail the first invocation and succeed on the second to exercise retry persistence.
	registry := engine.NewRegistry()
	calls := 0
	requireSuccess(t, registry.Register("retry", integrationStep(func(context.Context, engine.StepInput) (engine.StepResult, error) {
		calls++
		if calls == 1 {
			return engine.StepResult{}, errors.New("transient failure")
		}
		return engine.StepResult{Output: json.RawMessage("42")}, nil
	})))
	// Verify that the dependent step receives both the original input and prior output.
	requireSuccess(t, registry.Register("consume", integrationStep(func(_ context.Context, in engine.StepInput) (engine.StepResult, error) {
		if string(in.Context.Steps["first"]) != "42" || string(in.Context.Input) != "7" {
			return engine.StepResult{}, errors.New("missing workflow context")
		}
		return engine.StepResult{Output: json.RawMessage("49")}, nil
	})))
	def := engine.WorkflowDefinition{ID: "integration", Version: 1, Steps: []engine.StepDefinition{
		{ID: "first", Type: "retry", Retry: engine.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}},
		{ID: "second", Type: "consume", DependsOn: []string{"first"}},
	}}
	id, err := engine.NewScheduler(registry, r).Execute(ctx, def, json.RawMessage("7"))
	requireSuccess(t, err)
	if calls != 2 {
		t.Fatalf("retry calls = %d, want 2", calls)
	}

	// Reuse the isolated schema, not the original pool or repository.
	config := r.pool.Config()
	r.pool.Close()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	requireSuccess(t, err)
	t.Cleanup(pool.Close)
	reopened := NewExecutionRepository(pool)

	// Read persisted records through the replacement repository only.
	execution, err := reopened.GetExecution(ctx, id)
	requireSuccess(t, err)
	if execution.Status != engine.ExecutionCompleted || string(execution.Input) != "7" ||
		execution.StartedAt == nil || execution.CompletedAt == nil {
		t.Fatalf("unexpected execution after reconnect: %+v", execution)
	}
	steps, err := reopened.ListStepExecutions(ctx, id)
	requireSuccess(t, err)
	if len(steps) != 2 || steps[0].StepID != "first" || steps[1].StepID != "second" ||
		steps[0].Status != engine.StepCompleted || steps[1].Status != engine.StepCompleted ||
		string(steps[0].Output) != "42" || string(steps[1].Output) != "49" {
		t.Fatalf("unexpected steps after reconnect: %+v", steps)
	}
	// Verify that failed and successful attempts remain distinct, including error classification.
	first, err := reopened.ListStepAttempts(ctx, steps[0].ID)
	requireSuccess(t, err)
	second, err := reopened.ListStepAttempts(ctx, steps[1].ID)
	requireSuccess(t, err)
	if len(first) != 2 || first[0].Attempt != 1 || first[1].Attempt != 2 ||
		first[0].Status != engine.StepAttemptFailed || first[1].Status != engine.StepAttemptCompleted ||
		first[0].Error == nil || first[0].Error.Kind != engine.ErrorRetryable ||
		first[0].Error.Message != "transient failure" || len(second) != 1 ||
		second[0].Status != engine.StepAttemptCompleted {
		t.Fatalf("unexpected attempts after reconnect: %+v / %+v", first, second)
	}
	// Verify event ordering and the references linking events to their attempts.
	events, err := reopened.ListEvents(ctx, id)
	requireSuccess(t, err)
	want := []engine.ExecutionEventType{
		engine.EventExecutionCreated, engine.EventExecutionStarted,
		engine.EventStepStarted, engine.EventStepAttemptStarted,
		engine.EventStepAttemptFailed, engine.EventStepRetryScheduled,
		engine.EventStepAttemptStarted, engine.EventStepCompleted,
		engine.EventStepStarted, engine.EventStepAttemptStarted,
		engine.EventStepCompleted, engine.EventExecutionCompleted,
	}
	got := make([]engine.ExecutionEventType, 0, len(events))
	for _, event := range events {
		got = append(got, event.Type)
		if event.ExecutionID != id || event.CreatedAt.IsZero() {
			t.Fatalf("invalid event: %+v", event)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if events[5].StepExecutionID != steps[0].ID || events[5].StepAttemptID != first[0].ID ||
		events[6].StepAttemptID != first[1].ID || events[9].StepAttemptID != second[0].ID {
		t.Fatal("incorrect persisted attempt event references")
	}
}

// TestSchedulerPostgresTerminalFailure verifies that an explicitly permanent
// step error suppresses configured retries, persists a terminal failure on the
// attempt and execution, and prevents dependent steps from running. It also
// verifies that the event history contains no retry-scheduled event.
func TestSchedulerPostgresTerminalFailure(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := engine.NewRegistry()
	calls := 0
	// Configure a permanent error even though the workflow allows three attempts.
	requireSuccess(t, registry.Register("fail", integrationStep(func(context.Context, engine.StepInput) (engine.StepResult, error) {
		calls++
		return engine.StepResult{}, engine.Permanent(errors.New("terminal failure"))
	})))
	def := engine.WorkflowDefinition{ID: "failure", Version: 1, Steps: []engine.StepDefinition{
		{ID: "first", Type: "fail", Retry: engine.RetryPolicy{MaxAttempts: 3}},
		{ID: "second", Type: "fail", DependsOn: []string{"first"}},
	}}
	id, err := engine.NewScheduler(registry, r).Execute(ctx, def, nil)
	if err == nil || id == "" || calls != 1 {
		t.Fatalf("unexpected failure outcome: id=%s err=%v calls=%d", id, err, calls)
	}
	execution, err := r.GetExecution(ctx, id)
	requireSuccess(t, err)
	if execution.Status != engine.ExecutionFailed || execution.Error == nil || execution.Error.Message != "terminal failure" {
		t.Fatalf("unexpected failed execution: %+v", execution)
	}
	steps, err := r.ListStepExecutions(ctx, id)
	requireSuccess(t, err)
	if len(steps) != 2 || steps[0].Status != engine.StepFailed || steps[1].Status != engine.StepPending {
		t.Fatalf("unexpected steps: %+v", steps)
	}
	attempts, err := r.ListStepAttempts(ctx, steps[0].ID)
	requireSuccess(t, err)
	if len(attempts) != 1 || attempts[0].Status != engine.StepAttemptFailed ||
		attempts[0].Error == nil || attempts[0].Error.Kind != engine.ErrorTerminal {
		t.Fatalf("unexpected attempts: %+v", attempts)
	}
	assertExecutionEvents(t, r, id, "execution.created", "execution.started", "step.started",
		"step.attempt_started", "step.attempt_failed", "step.failed", "execution.failed")
}

// TestSchedulerPostgresCancellation verifies that a persisted cancellation
// request is observed by the scheduler between steps. The active step finishes,
// the pending dependent step is cancelled without an attempt, and the execution
// reaches the cancelled state with the expected lifecycle events.
// This does not test interruption of an actively executing step.
func TestSchedulerPostgresCancellation(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := engine.NewRegistry()
	calls := 0
	// Request cancellation from the active step without cancelling its Go context.
	requireSuccess(t, registry.Register("cancel", integrationStep(func(ctx context.Context, in engine.StepInput) (engine.StepResult, error) {
		calls++
		if err := r.RequestCancellation(ctx, in.ExecutionID); err != nil {
			return engine.StepResult{}, err
		}
		return engine.StepResult{Output: json.RawMessage("true")}, nil
	})))
	def := engine.WorkflowDefinition{ID: "cancel", Version: 1, Steps: []engine.StepDefinition{
		{ID: "first", Type: "cancel"},
		{ID: "second", Type: "cancel", DependsOn: []string{"first"}},
	}}
	id, err := engine.NewScheduler(registry, r).Execute(ctx, def, nil)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("unexpected cancellation: id=%s err=%v calls=%d", id, err, calls)
	}
	execution, err := r.GetExecution(ctx, id)
	requireSuccess(t, err)
	if execution.Status != engine.ExecutionCancelled || execution.CompletedAt == nil {
		t.Fatalf("unexpected cancelled execution: %+v", execution)
	}
	steps, err := r.ListStepExecutions(ctx, id)
	requireSuccess(t, err)
	if len(steps) != 2 || steps[0].Status != engine.StepCompleted || steps[1].Status != engine.StepCancelled {
		t.Fatalf("unexpected steps: %+v", steps)
	}
	// The dependent step must never have started or created an attempt.
	attempts, err := r.ListStepAttempts(ctx, steps[1].ID)
	requireSuccess(t, err)
	if len(attempts) != 0 {
		t.Fatalf("cancelled pending step has attempts: %+v", attempts)
	}
	assertExecutionEvents(t, r, id, "execution.created", "execution.started", "step.started",
		"step.attempt_started", "execution.cancel_requested", "step.completed", "step.cancelled", "execution.cancelled")
}
