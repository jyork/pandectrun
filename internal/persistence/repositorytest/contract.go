package repositorytest

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"reflect"
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
		run func(*testing.T, engine.ExecutionRepository)
	}{
		{"ExecutionLifecycleAndEvents", testExecutionLifecycleAndEvents},
		{"CancellationIsIdempotent", testCancellationIsIdempotent},
		{"ListExecutionsFiltersAndLimits", testListExecutionsFiltersAndLimits},
		{"CompletedStepOutputIsImmutable", testCompletedStepOutputIsImmutable},
		{"IDTypes", testIDTypes},
		{"AttemptHistory", testAttemptHistory},
		{"MissingRecords", testMissingRecords},
		{"ConcurrentStart", testConcurrentStart},
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
	if err != nil { t.Fatal(err) }
	if err := repo.StartExecution(ctx, id); err != nil { t.Fatal(err) }
	if err := repo.CompleteExecution(ctx, id, json.RawMessage(`{"ok":true}`)); err != nil { t.Fatal(err) }

	execution, err := repo.GetExecution(ctx, id)
	if err != nil { t.Fatal(err) }
	if execution.Status != engine.ExecutionCompleted { t.Fatalf("status = %q", execution.Status) }
	if execution.StartedAt == nil || execution.CompletedAt == nil { t.Fatal("lifecycle timestamps were not persisted") }

	events, err := repo.ListEvents(ctx, id)
	if err != nil { t.Fatal(err) }
	want := []engine.ExecutionEventType{engine.EventExecutionCreated, engine.EventExecutionStarted, engine.EventExecutionCompleted}
	if len(events) != len(want) { t.Fatalf("events = %d, want %d", len(events), len(want)) }
	for i := range want { if events[i].Type != want[i] { t.Fatalf("event[%d] = %q, want %q", i, events[i].Type, want[i]) } }

	if err := repo.FailExecution(ctx, id, engine.ExecutionError{Kind: engine.ErrorTerminal, Message: "late"}); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("late failure error = %v, want ErrInvalidTransition", err)
	}
}

func testCancellationIsIdempotent(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background();
	id, _ := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID:"wf", WorkflowVersion:1})
	if err := repo.StartExecution(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.RequestCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.RequestCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.CompleteCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.CompleteCancellation(ctx,id); err != nil { t.Fatal(err) }
	e, _ := repo.GetExecution(ctx,id); if e.Status != engine.ExecutionCancelled { t.Fatalf("status = %q",e.Status) }
}

func testListExecutionsFiltersAndLimits(t *testing.T, repo engine.ExecutionRepository) {
	ctx := context.Background();
	one,_ := repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"one",WorkflowVersion:1}); _ = repo.StartExecution(ctx,one)
	two,_ := repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"two",WorkflowVersion:1}); _ = repo.StartExecution(ctx,two); _ = repo.FailExecution(ctx,two,engine.ExecutionError{Kind:engine.ErrorTerminal,Message:"boom"})
	three,_ := repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"one",WorkflowVersion:2}); _ = repo.StartExecution(ctx,three)

	running,err := repo.ListExecutions(ctx,engine.ExecutionQuery{Status:[]engine.ExecutionStatus{engine.ExecutionRunning}}); if err != nil { t.Fatal(err) }
	if len(running)!=2 { t.Fatalf("running executions = %d, want 2",len(running)) }
	workflowOne,err := repo.ListExecutions(ctx,engine.ExecutionQuery{WorkflowID:"one",Limit:1}); if err != nil { t.Fatal(err) }
	if len(workflowOne)!=1 || workflowOne[0].WorkflowID!="one" { t.Fatalf("workflow one query = %+v",workflowOne) }
}

func testCompletedStepOutputIsImmutable(t *testing.T, repo engine.ExecutionRepository) {
	ctx:=context.Background(); executionID,_:=repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"wf",WorkflowVersion:1}); _=repo.StartExecution(ctx,executionID)
	stepID,_:=repo.CreateStepExecution(ctx,engine.NewStepExecution{ExecutionID:executionID,StepID:"s"}); _=repo.StartStep(ctx,stepID)
	attemptID,_:=repo.CreateStepAttempt(ctx,engine.NewStepAttempt{StepExecutionID:stepID,Attempt:1}); _=repo.CompleteStepAttempt(ctx,attemptID)
	output:=json.RawMessage(`{"value":1}`); if err:=repo.CompleteStep(ctx,stepID,output); err!=nil { t.Fatal(err) }
	output[10]='9'
	step,_:=repo.GetStepExecution(ctx,stepID); if !equalJSON(step.Output, json.RawMessage(`{"value":1}`)) { t.Fatalf("stored output = %s",step.Output) }
	if err:=repo.CompleteStep(ctx,stepID,json.RawMessage(`{"value":2}`)); !errors.Is(err,engine.ErrInvalidTransition) { t.Fatalf("second completion error = %v",err) }
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
