package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type recordingStep struct {
	order *[]string
}

func (s recordingStep) Execute(_ context.Context, input StepInput) (StepResult, error) {
	*s.order = append(*s.order, input.Step.ID)
	return StepResult{Output: json.RawMessage(fmt.Sprintf(`{"step":%q}`, input.Step.ID))}, nil
}

type failingStep struct{}

type retryThenSucceedStep struct {
	attempts int
}

func (s *retryThenSucceedStep) Execute(context.Context, StepInput) (StepResult, error) {
	s.attempts++
	if s.attempts < 3 {
		return StepResult{}, fmt.Errorf("transient failure %d", s.attempts)
	}
	return StepResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

type permanentFailingStep struct {
	attempts int
}

func (s *permanentFailingStep) Execute(context.Context, StepInput) (StepResult, error) {
	s.attempts++
	return StepResult{}, Permanent(fmt.Errorf("invalid request"))
}

// IsRetryable deliberately returns true to verify that explicit permanent
// classification takes precedence over a step-specific classifier.
func (s *permanentFailingStep) IsRetryable(error) bool {
	return true
}

type classifyingStep struct {
	attempts int
	retry    bool
}

func (s *classifyingStep) Execute(context.Context, StepInput) (StepResult, error) {
	s.attempts++
	return StepResult{}, fmt.Errorf("classified failure")
}

func (s *classifyingStep) IsRetryable(error) bool {
	return s.retry
}


func (failingStep) Execute(context.Context, StepInput) (StepResult, error) {
	return StepResult{}, fmt.Errorf("boom")
}

func TestSchedulerExecutesDAGSequentiallyAndPersistsHistory(t *testing.T) {
	var order []string
	registry := NewRegistry()
	if err := registry.Register("record", recordingStep{order: &order}); err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryExecutionRepository()

	def := WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
		{ID: "validate", Type: "record"},
		{ID: "fetch-a", Type: "record", DependsOn: []string{"validate"}},
		{ID: "fetch-b", Type: "record", DependsOn: []string{"validate"}},
		{ID: "analyze", Type: "record", DependsOn: []string{"fetch-a", "fetch-b"}},
	}}

	executionID, err := NewScheduler(registry, repo).Execute(context.Background(), def, json.RawMessage(`{"service":"checkout"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	wantOrder := []string{"validate", "fetch-a", "fetch-b", "analyze"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("execution order = %v, want %v", order, wantOrder)
	}

	execution, err := repo.GetExecution(context.Background(), executionID)
	if err != nil {
\t\tt.Fatal(err)
\t}
	if execution.Status != ExecutionCompleted {
		t.Fatalf("execution status = %q, want %q", execution.Status, ExecutionCompleted)
	}
	steps, err := repo.ListStepExecutions(context.Background(), executionID)
	if err != nil {
\t\tt.Fatal(err)
\t}
	if len(steps) != 4 {
\t\tt.Fatalf("steps = %d, want 4", len(steps))
\t}
	for _, step := range steps {
		if step.Status != StepCompleted {
\t\t\tt.Fatalf("step %q status = %q", step.StepID, step.Status)
\t\t}
		attempts, err := repo.ListStepAttempts(context.Background(), step.ID)
		if err != nil {
\t\tt.Fatal(err)
\t}
		if len(attempts) != 1 || attempts[0].Status != StepAttemptCompleted {
			t.Fatalf("step %q attempts = %+v, want one completed attempt", step.StepID, attempts)
		}
	}
}

func TestSchedulerFailsFastAndPersistsFailure(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("fail", failingStep{}); err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryExecutionRepository()

	def := WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
		{ID: "bad", Type: "fail"},
		{ID: "later", Type: "fail", DependsOn: []string{"bad"}},
	}}

	executionID, err := NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
\t\tt.Fatal("Execute() error = nil, want failure")
\t}

	execution, getErr := repo.GetExecution(context.Background(), executionID)
	if getErr != nil {
\t\tt.Fatal(getErr)
\t}
	if execution.Status != ExecutionFailed {
\t\tt.Fatalf("status = %q, want %q", execution.Status, ExecutionFailed)
\t}
	if execution.Error == nil || execution.Error.Message != "boom" {
\t\tt.Fatalf("execution error = %+v", execution.Error)
\t}

	steps, listErr := repo.ListStepExecutions(context.Background(), executionID)
	if listErr != nil {
\t\tt.Fatal(listErr)
\t}
	byStep := make(map[string]StepExecution)
	for _, step := range steps {
\t\tbyStep[step.StepID] = step
\t}
	if byStep["bad"].Status != StepFailed {
\t\tt.Fatalf("bad status = %q", byStep["bad"].Status)
\t}
	if byStep["later"].Status != StepPending {
\t\tt.Fatalf("later status = %q", byStep["later"].Status)
\t}
}


func TestSchedulerRetriesConfiguredStepAndPersistsAttempts(t *testing.T) {
	implementation := &retryThenSucceedStep{}
	registry := NewRegistry()
	if err := registry.Register("retry", implementation); err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryExecutionRepository()

	def := WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps: []StepDefinition{
			{
				ID:   "retry-me",
				Type: "retry",
				Retry: RetryPolicy{
					MaxAttempts: 3,
					BaseDelay:   time.Nanosecond,
					MaxDelay:    time.Nanosecond,
				},
			},
		},
	}

	executionID, err := NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if implementation.attempts != 3 {
		t.Fatalf("step attempts = %d, want 3", implementation.attempts)
	}

	steps, err := repo.ListStepExecutions(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(steps))
	}

	attempts, err := repo.ListStepAttempts(context.Background(), steps[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 3 {
		t.Fatalf("persisted attempts = %d, want 3", len(attempts))
	}
	if attempts[0].Status != StepAttemptFailed || attempts[1].Status != StepAttemptFailed || attempts[2].Status != StepAttemptCompleted {
		t.Fatalf("attempt statuses = [%q %q %q], want [failed failed completed]", attempts[0].Status, attempts[1].Status, attempts[2].Status)
	}
	if attempts[0].Error == nil || attempts[0].Error.Kind != ErrorRetryable {
		t.Fatalf("first attempt error = %+v, want retryable", attempts[0].Error)
	}

	events, err := repo.ListEvents(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	var retryScheduled int
	for _, event := range events {
		if event.Type == EventStepRetryScheduled {
			retryScheduled++
		}
	}
	if retryScheduled != 2 {
		t.Fatalf("retry scheduled events = %d, want 2", retryScheduled)
	}
}

func TestSchedulerDoesNotRetryWithoutPolicy(t *testing.T) {
	implementation := &retryThenSucceedStep{}
	registry := NewRegistry()
	if err := registry.Register("retry", implementation); err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryExecutionRepository()

	def := WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps:   []StepDefinition{{ID: "once", Type: "retry"}},
	}

	_, err := NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if implementation.attempts != 1 {
		t.Fatalf("step attempts = %d, want 1", implementation.attempts)
	}
}

func TestSchedulerPermanentErrorStopsConfiguredRetries(t *testing.T) {
	implementation := &permanentFailingStep{}
	registry := NewRegistry()
	if err := registry.Register("permanent", implementation); err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryExecutionRepository()

	def := WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps: []StepDefinition{
			{
				ID:    "permanent",
				Type:  "permanent",
				Retry: RetryPolicy{MaxAttempts: 3},
			},
		},
	}

	_, err := NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if implementation.attempts != 1 {
		t.Fatalf("step attempts = %d, want 1", implementation.attempts)
	}
}

func TestSchedulerUsesStepRetryClassifier(t *testing.T) {
	implementation := &classifyingStep{retry: false}
	registry := NewRegistry()
	if err := registry.Register("classified", implementation); err != nil {
		t.Fatal(err)
	}
	repo := NewMemoryExecutionRepository()

	def := WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps: []StepDefinition{
			{
				ID:    "classified",
				Type:  "classified",
				Retry: RetryPolicy{MaxAttempts: 3},
			},
		},
	}

	_, err := NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if implementation.attempts != 1 {
		t.Fatalf("step attempts = %d, want classifier to stop after 1", implementation.attempts)
	}
}
