package repositorytest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"sync"
	"testing"

	"github.com/jyork/pandectrun/internal/engine"
)

// Factory creates an empty, isolated repository for one contract subtest.
// Register any resource cleanup using t.Cleanup.
//
// Parameters:
//   - t: the subtest that owns the repository.
//
// Returns:
//   - engine.ExecutionRepository: a fresh repository implementation.
type Factory func(t *testing.T) engine.ExecutionRepository

// Run executes backend-independent repository contract tests.
//
// Parameters:
//   - t: the parent test.
//   - factory: creates an isolated repository for each subtest.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	tests := []struct {
		name string
		run  func(*testing.T, engine.ExecutionRepository)
	}{
		{"ExecutionLifecycleAndEvents", testExecutionLifecycleAndEvents},
		{"CancellationIsIdempotent", testCancellationIsIdempotent},
		{"ListExecutionsFiltersAndLimits", testListExecutionsFiltersAndLimits},
		{"CompletedStepOutputIsImmutable", testCompletedStepOutputIsImmutable},
		{"IDTypes", testIDTypes},
		{"AttemptHistory", testAttemptHistory},
		{"MissingRecords", testMissingRecords},
		{"ConcurrentStart", testConcurrentStart},
		{"CreationConstraints", testCreationConstraints},
		{"CancellationRaceOutcomes", testCancellationRaceOutcomes},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := factory(t)
			if repo == nil {
				t.Fatal("factory returned nil repository")
			}
			tc.run(t, repo)
		})
	}
}

func testExecutionLifecycleAndEvents(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()

	id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 2, Input: json.RawMessage(`{"x":1}`)})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, id))
	requireNoError(t, repo.CompleteExecution(ctx, id, json.RawMessage(`{"ok":true}`)))

	execution, err := repo.GetExecution(ctx, id)
	requireNoError(t, err)
	if execution.Status != engine.ExecutionCompleted {
		t.Fatalf("status = %q", execution.Status)
	}
	if execution.StartedAt == nil || execution.CompletedAt == nil {
		t.Fatal("lifecycle timestamps were not persisted")
	}

	events, err := repo.ListEvents(ctx, id)
	requireNoError(t, err)
	want := []engine.ExecutionEventType{engine.EventExecutionCreated, engine.EventExecutionStarted, engine.EventExecutionCompleted}
	if len(events) != len(want) {
		t.Fatalf("events = %d, want %d", len(events), len(want))
	}
	for i := range want {
		if events[i].Type != want[i] {
			t.Fatalf("event[%d] = %q, want %q", i, events[i].Type, want[i])
		}
	}

	if err := repo.FailExecution(ctx, id, engine.ExecutionError{Kind: engine.ErrorTerminal, Message: "late"}); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("late failure error = %v, want ErrInvalidTransition", err)
	}
}

func testCancellationIsIdempotent(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, id))
	requireNoError(t, repo.RequestCancellation(ctx, id))
	requireNoError(t, repo.RequestCancellation(ctx, id))
	requireNoError(t, repo.CompleteCancellation(ctx, id))
	requireNoError(t, repo.CompleteCancellation(ctx, id))
	e, err := repo.GetExecution(ctx, id)
	requireNoError(t, err)
	if e.Status != engine.ExecutionCancelled {
		t.Fatalf("status = %q", e.Status)
	}
	events, err := repo.ListEvents(ctx, id)
	requireNoError(t, err)
	if len(events) != 4 || events[2].Type != engine.EventExecutionCancelRequested || events[3].Type != engine.EventExecutionCancelled {
		t.Fatalf("unexpected cancellation events: %+v", events)
	}
}

func testListExecutionsFiltersAndLimits(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	one, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "one", WorkflowVersion: 1})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, one))
	two, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "two", WorkflowVersion: 1})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, two))
	requireNoError(t, repo.FailExecution(ctx, two, engine.ExecutionError{Kind: engine.ErrorTerminal, Message: "boom"}))
	three, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "one", WorkflowVersion: 2})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, three))

	running, err := repo.ListExecutions(ctx, engine.ExecutionQuery{Status: []engine.ExecutionStatus{engine.ExecutionRunning}})
	requireNoError(t, err)
	if len(running) != 2 || running[0].ID != one || running[1].ID != three {
		// Equal timestamps may be ordered by UUID rather than insertion order.
		if len(running) != 2 || running[0].ID != three || running[1].ID != one {
			t.Fatalf("unexpected running executions: %+v", running)
		}
	}
	workflowOne, err := repo.ListExecutions(ctx, engine.ExecutionQuery{WorkflowID: "one", Limit: 1})
	requireNoError(t, err)
	if len(workflowOne) != 1 || workflowOne[0].WorkflowID != "one" {
		t.Fatalf("workflow one query = %+v", workflowOne)
	}
}

func testCompletedStepOutputIsImmutable(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, id))
	stepID, err := repo.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: id, StepID: "s"})
	requireNoError(t, err)
	requireNoError(t, repo.StartStep(ctx, stepID))
	attemptID, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: stepID, Attempt: 1})
	requireNoError(t, err)
	requireNoError(t, repo.CompleteStepAttempt(ctx, attemptID))
	output := json.RawMessage(`{"value":1}`)
	requireNoError(t, repo.CompleteStep(ctx, stepID, output))
	output[9] = '9'
	step, err := repo.GetStepExecution(ctx, stepID)
	requireNoError(t, err)
	if !equalJSON(step.Output, json.RawMessage(`{"value":1}`)) {
		t.Fatalf("stored output = %s", step.Output)
	}
	if err := repo.CompleteStep(ctx, stepID, json.RawMessage(`{"value":2}`)); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("second completion error = %v", err)
	}
	step, err = repo.GetStepExecution(ctx, stepID)
	requireNoError(t, err)
	if !equalJSON(step.Output, json.RawMessage(`{"value":1}`)) {
		t.Fatalf("output changed after rejected transition: %s", step.Output)
	}
}

func testIDTypes(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()

	executionID, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	if err != nil {
		t.Fatal(err)
	}

	uuidV4 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidV4.MatchString(string(executionID)) {
		t.Fatalf("execution ID %q is not a version 4 UUID", executionID)
	}

	secondExecutionID, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if executionID == secondExecutionID {
		t.Fatal("execution IDs must be unique")
	}

	stepID, err := repo.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: executionID, StepID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if stepID <= 0 {
		t.Fatalf("step ID = %d, want positive ID", stepID)
	}

	if err := repo.StartExecution(ctx, executionID); err != nil {
		t.Fatal(err)
	}
	if err := repo.StartStep(ctx, stepID); err != nil {
		t.Fatal(err)
	}

	attemptID, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: stepID, Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if attemptID <= 0 {
		t.Fatalf("attempt ID = %d, want positive ID", attemptID)
	}

	events, err := repo.ListEvents(ctx, executionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[len(events)-1].StepAttemptID != attemptID {
		t.Fatalf("last event does not reference attempt %d", attemptID)
	}
}

func equalJSON(a, b json.RawMessage) bool {
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}

// requireNoError stops the current test when an operation fails.
func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func testAttemptHistory(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, id))
	step, err := repo.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: id, StepID: "retry"})
	requireNoError(t, err)
	requireNoError(t, repo.StartStep(ctx, step))
	first, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: step, Attempt: 1})
	requireNoError(t, err)
	failure := engine.ExecutionError{Kind: engine.ErrorRetryable, Code: "temporary", Message: "retry"}
	requireNoError(t, repo.FailStepAttempt(ctx, first, failure))
	requireNoError(t, repo.RecordStepRetryScheduled(ctx, first))
	second, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: step, Attempt: 2})
	requireNoError(t, err)
	if second == first {
		t.Fatal("attempt IDs must be distinct")
	}
	requireNoError(t, repo.CompleteStepAttempt(ctx, second))
	requireNoError(t, repo.CompleteStep(ctx, step, json.RawMessage(`{"ok":true}`)))
	attempts, err := repo.ListStepAttempts(ctx, step)
	requireNoError(t, err)
	if len(attempts) != 2 {
		t.Fatalf("attempt count = %d, want 2", len(attempts))
	}
	if attempts[0].ID != first || attempts[0].Attempt != 1 || attempts[0].Status != engine.StepAttemptFailed ||
		attempts[0].Error == nil || *attempts[0].Error != failure ||
		attempts[1].ID != second || attempts[1].Attempt != 2 || attempts[1].Status != engine.StepAttemptCompleted {
		t.Fatalf("unexpected attempt history: %+v", attempts)
	}
	for _, attempt := range attempts {
		if attempt.StartedAt.IsZero() || attempt.CompletedAt == nil || attempt.CompletedAt.Before(attempt.StartedAt) {
			t.Fatalf("invalid attempt timestamps: %+v", attempt)
		}
	}
	events, err := repo.ListEvents(ctx, id)
	requireNoError(t, err)
	want := []engine.ExecutionEventType{
		engine.EventExecutionCreated, engine.EventExecutionStarted, engine.EventStepStarted,
		engine.EventStepAttemptStarted, engine.EventStepAttemptFailed, engine.EventStepRetryScheduled,
		engine.EventStepAttemptStarted, engine.EventStepCompleted,
	}
	if len(events) != len(want) {
		t.Fatalf("event count = %d, want %d", len(events), len(want))
	}
	for i, event := range events {
		if event.Type != want[i] || event.ExecutionID != id || event.CreatedAt.IsZero() {
			t.Fatalf("unexpected event %d: %+v", i, event)
		}
	}
	if events[4].StepAttemptID != first || events[5].StepAttemptID != first || events[6].StepAttemptID != second {
		t.Fatal("attempt event references do not match history")
	}
}

func testMissingRecords(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	id := engine.ExecutionID("00000000-0000-4000-8000-000000000001")
	_, getErr := repo.GetExecution(ctx, id)
	_, stepsErr := repo.ListStepExecutions(ctx, id)
	_, eventsErr := repo.ListEvents(ctx, id)
	_, stepErr := repo.GetStepExecution(ctx, 1)
	_, attemptsErr := repo.ListStepAttempts(ctx, 1)
	_, createStepErr := repo.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: id, StepID: "missing"})
	_, createAttemptErr := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: 1, Attempt: 1})
	for name, err := range map[string]error{
		"GetExecution": getErr, "ListStepExecutions": stepsErr, "ListEvents": eventsErr,
		"GetStepExecution": stepErr, "ListStepAttempts": attemptsErr,
		"CreateStepExecution": createStepErr, "CreateStepAttempt": createAttemptErr,
		"StartExecution": repo.StartExecution(ctx, id), "StartStep": repo.StartStep(ctx, 1),
		"CompleteStepAttempt": repo.CompleteStepAttempt(ctx, 1),
	} {
		if !errors.Is(err, engine.ErrNotFound) {
			t.Errorf("%s: got %v, want ErrNotFound", name, err)
		}
	}
}

func testConcurrentStart(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireNoError(t, err)
	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- repo.StartExecution(ctx, id)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, engine.ErrInvalidTransition) {
			t.Errorf("unexpected start error: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successful starts = %d, want 1", success)
	}
	events, err := repo.ListEvents(ctx, id)
	requireNoError(t, err)
	if len(events) != 2 || events[1].Type != engine.EventExecutionStarted {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func testCancellationRaceOutcomes(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	for _, status := range []engine.ExecutionStatus{engine.ExecutionCompleted, engine.ExecutionFailed, engine.ExecutionCancelled} {
		id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
		requireNoError(t, err)
		requireNoError(t, repo.StartExecution(ctx, id))
		requireNoError(t, repo.RequestCancellation(ctx, id))
		switch status {
		case engine.ExecutionCompleted:
			requireNoError(t, repo.CompleteExecution(ctx, id, json.RawMessage(`{}`)))
		case engine.ExecutionFailed:
			requireNoError(t, repo.FailExecution(ctx, id, engine.ExecutionError{Kind: engine.ErrorTerminal, Message: "failure"}))
		case engine.ExecutionCancelled:
			requireNoError(t, repo.CompleteCancellation(ctx, id))
		}
		execution, err := repo.GetExecution(ctx, id)
		requireNoError(t, err)
		if execution.Status != status || execution.CompletedAt == nil {
			t.Fatalf("execution = %+v, want terminal status %s", execution, status)
		}
	}
}

func testCreationConstraints(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background()
	for _, version := range []int{-1, 0} {
		id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: version})
		if id != "" || !errors.Is(err, engine.ErrInvalidInput) {
			t.Fatalf("invalid version: id=%s err=%v", id, err)
		}
	}
	executions, err := repo.ListExecutions(ctx, engine.ExecutionQuery{})
	requireNoError(t, err)
	if len(executions) != 0 {
		t.Fatal("invalid executions persisted")
	}
	parent, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireNoError(t, err)
	requireNoError(t, repo.StartExecution(ctx, parent))
	stepInput := engine.NewStepExecution{ExecutionID: parent, StepID: "step"}
	step, err := repo.CreateStepExecution(ctx, stepInput)
	requireNoError(t, err)
	if id, err := repo.CreateStepExecution(ctx, stepInput); id != 0 || !errors.Is(err, engine.ErrAlreadyExists) {
		t.Fatalf("duplicate step: id=%d err=%v", id, err)
	}
	requireNoError(t, repo.StartStep(ctx, step))
	for _, number := range []uint{0, 2147483648} {
		id, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: step, Attempt: number})
		if id != 0 || !errors.Is(err, engine.ErrInvalidInput) {
			t.Fatalf("invalid attempt: id=%d err=%v", id, err)
		}
	}
	before, err := repo.ListEvents(ctx, parent)
	requireNoError(t, err)
	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			<-start
			_, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: step, Attempt: 1})
			results <- err
		}()
	}
	close(start)
	success := 0
	for i := 0; i < workers; i++ {
		err := <-results
		if err == nil {
			success++
		} else if !errors.Is(err, engine.ErrAlreadyExists) {
			t.Errorf("unexpected attempt creation error: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successful attempts = %d, want 1", success)
	}
	attempts, err := repo.ListStepAttempts(ctx, step)
	requireNoError(t, err)
	steps, err := repo.ListStepExecutions(ctx, parent)
	requireNoError(t, err)
	events, err := repo.ListEvents(ctx, parent)
	requireNoError(t, err)
	if len(steps) != 1 || len(attempts) != 1 || len(events) != len(before)+1 ||
		!reflect.DeepEqual(events[:len(before)], before) ||
		events[len(before)].Type != engine.EventStepAttemptStarted {
		t.Fatal("unexpected persisted state or event history")
	}
}
