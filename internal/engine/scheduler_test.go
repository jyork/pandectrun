package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jyork/pandectrun/internal/engine"
	"github.com/jyork/pandectrun/internal/persistence/memory"
)

type recordingStep struct {
	order *[]string
}

func (s recordingStep) Execute(_ context.Context, input engine.StepInput) (engine.StepResult, error) {
	*s.order = append(*s.order, input.Step.ID)
	return engine.StepResult{Output: json.RawMessage(fmt.Sprintf(`{"step":%q}`, input.Step.ID))}, nil
}

type failingStep struct{}

type retryThenSucceedStep struct {
	attempts int
}

func (s *retryThenSucceedStep) Execute(context.Context, engine.StepInput) (engine.StepResult, error) {
	s.attempts++
	if s.attempts < 3 {
		return engine.StepResult{}, fmt.Errorf("transient failure %d", s.attempts)
	}
	return engine.StepResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

type permanentFailingStep struct {
	attempts int
}

func (s *permanentFailingStep) Execute(context.Context, engine.StepInput) (engine.StepResult, error) {
	s.attempts++
	return engine.StepResult{}, engine.Permanent(fmt.Errorf("invalid request"))
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

func (s *classifyingStep) Execute(context.Context, engine.StepInput) (engine.StepResult, error) {
	s.attempts++
	return engine.StepResult{}, fmt.Errorf("classified failure")
}

func (s *classifyingStep) IsRetryable(error) bool {
	return s.retry
}

func (failingStep) Execute(context.Context, engine.StepInput) (engine.StepResult, error) {
	return engine.StepResult{}, fmt.Errorf("boom")
}

func TestSchedulerExecutesDAGSequentiallyAndPersistsHistory(t *testing.T) {
	var order []string
	registry := engine.NewRegistry()
	if err := registry.Register("record", recordingStep{order: &order}); err != nil {
		t.Fatal(err)
	}
	repo := memory.NewExecutionRepository()

	def := engine.WorkflowDefinition{ID: "wf", Version: 1, Steps: []engine.StepDefinition{
		{ID: "validate", Type: "record"},
		{ID: "fetch-a", Type: "record", DependsOn: []string{"validate"}},
		{ID: "fetch-b", Type: "record", DependsOn: []string{"validate"}},
		{ID: "analyze", Type: "record", DependsOn: []string{"fetch-a", "fetch-b"}},
	}}

	executionID, err := engine.NewScheduler(registry, repo).Execute(context.Background(), def, json.RawMessage(`{"service":"checkout"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	wantOrder := []string{"validate", "fetch-a", "fetch-b", "analyze"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("execution order = %v, want %v", order, wantOrder)
	}

	execution, err := repo.GetExecution(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != engine.ExecutionCompleted {
		t.Fatalf("execution status = %q, want %q", execution.Status, engine.ExecutionCompleted)
	}
	steps, err := repo.ListStepExecutions(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 4 {
		t.Fatalf("steps = %d, want 4", len(steps))
	}
	for _, step := range steps {
		if step.Status != engine.StepCompleted {
			t.Fatalf("step %q status = %q", step.StepID, step.Status)
		}
		attempts, err := repo.ListStepAttempts(context.Background(), step.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(attempts) != 1 || attempts[0].Status != engine.StepAttemptCompleted {
			t.Fatalf("step %q attempts = %+v, want one completed attempt", step.StepID, attempts)
		}
	}
}

func TestSchedulerFailsFastAndPersistsFailure(t *testing.T) {
	registry := engine.NewRegistry()
	if err := registry.Register("fail", failingStep{}); err != nil {
		t.Fatal(err)
	}
	repo := memory.NewExecutionRepository()

	def := engine.WorkflowDefinition{ID: "wf", Version: 1, Steps: []engine.StepDefinition{
		{ID: "bad", Type: "fail"},
		{ID: "later", Type: "fail", DependsOn: []string{"bad"}},
	}}

	executionID, err := engine.NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}

	execution, getErr := repo.GetExecution(context.Background(), executionID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if execution.Status != engine.ExecutionFailed {
		t.Fatalf("status = %q, want %q", execution.Status, engine.ExecutionFailed)
	}
	if execution.Error == nil || execution.Error.Message != "boom" {
		t.Fatalf("execution error = %+v", execution.Error)
	}

	steps, listErr := repo.ListStepExecutions(context.Background(), executionID)
	if listErr != nil {
		t.Fatal(listErr)
	}
	byStep := make(map[string]engine.StepExecution)
	for _, step := range steps {
		byStep[step.StepID] = step
	}
	if byStep["bad"].Status != engine.StepFailed {
		t.Fatalf("bad status = %q", byStep["bad"].Status)
	}
	if byStep["later"].Status != engine.StepPending {
		t.Fatalf("later status = %q", byStep["later"].Status)
	}
}

func TestSchedulerRetriesConfiguredStepAndPersistsAttempts(t *testing.T) {
	implementation := &retryThenSucceedStep{}
	registry := engine.NewRegistry()
	if err := registry.Register("retry", implementation); err != nil {
		t.Fatal(err)
	}
	repo := memory.NewExecutionRepository()

	def := engine.WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps: []engine.StepDefinition{
			{
				ID:   "retry-me",
				Type: "retry",
				Retry: engine.RetryPolicy{
					MaxAttempts: 3,
					BaseDelay:   time.Nanosecond,
					MaxDelay:    time.Nanosecond,
				},
			},
		},
	}

	executionID, err := engine.NewScheduler(registry, repo).Execute(context.Background(), def, nil)
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
	if attempts[0].Status != engine.StepAttemptFailed || attempts[1].Status != engine.StepAttemptFailed || attempts[2].Status != engine.StepAttemptCompleted {
		t.Fatalf("attempt statuses = [%q %q %q], want [failed failed completed]", attempts[0].Status, attempts[1].Status, attempts[2].Status)
	}
	if attempts[0].Error == nil || attempts[0].Error.Kind != engine.ErrorRetryable {
		t.Fatalf("first attempt error = %+v, want retryable", attempts[0].Error)
	}

	events, err := repo.ListEvents(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	var retryScheduled int
	for _, event := range events {
		if event.Type == engine.EventStepRetryScheduled {
			retryScheduled++
		}
	}
	if retryScheduled != 2 {
		t.Fatalf("retry scheduled events = %d, want 2", retryScheduled)
	}
}

func TestSchedulerDoesNotRetryWithoutPolicy(t *testing.T) {
	implementation := &retryThenSucceedStep{}
	registry := engine.NewRegistry()
	if err := registry.Register("retry", implementation); err != nil {
		t.Fatal(err)
	}
	repo := memory.NewExecutionRepository()

	def := engine.WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps:   []engine.StepDefinition{{ID: "once", Type: "retry"}},
	}

	_, err := engine.NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if implementation.attempts != 1 {
		t.Fatalf("step attempts = %d, want 1", implementation.attempts)
	}
}

func TestSchedulerPermanentErrorStopsConfiguredRetries(t *testing.T) {
	implementation := &permanentFailingStep{}
	registry := engine.NewRegistry()
	if err := registry.Register("permanent", implementation); err != nil {
		t.Fatal(err)
	}
	repo := memory.NewExecutionRepository()

	def := engine.WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps: []engine.StepDefinition{
			{
				ID:    "permanent",
				Type:  "permanent",
				Retry: engine.RetryPolicy{MaxAttempts: 3},
			},
		},
	}

	_, err := engine.NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if implementation.attempts != 1 {
		t.Fatalf("step attempts = %d, want 1", implementation.attempts)
	}
}

func TestSchedulerUsesStepRetryClassifier(t *testing.T) {
	implementation := &classifyingStep{retry: false}
	registry := engine.NewRegistry()
	if err := registry.Register("classified", implementation); err != nil {
		t.Fatal(err)
	}
	repo := memory.NewExecutionRepository()

	def := engine.WorkflowDefinition{
		ID:      "wf",
		Version: 1,
		Steps: []engine.StepDefinition{
			{
				ID:    "classified",
				Type:  "classified",
				Retry: engine.RetryPolicy{MaxAttempts: 3},
			},
		},
	}

	_, err := engine.NewScheduler(registry, repo).Execute(context.Background(), def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if implementation.attempts != 1 {
		t.Fatalf("step attempts = %d, want classifier to stop after 1", implementation.attempts)
	}
}
