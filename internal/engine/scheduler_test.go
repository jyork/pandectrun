package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

type recordingStep struct {
	order *[]string
}

func (s recordingStep) Execute(_ context.Context, input StepInput) (StepResult, error) {
	*s.order = append(*s.order, input.Step.ID)
	return StepResult{Output: json.RawMessage(fmt.Sprintf(`{"step":%q}`, input.Step.ID))}, nil
}

type failingStep struct{}

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
	if err != nil { t.Fatal(err) }
	if execution.Status != ExecutionCompleted {
		t.Fatalf("execution status = %q, want %q", execution.Status, ExecutionCompleted)
	}
	steps, err := repo.ListStepExecutions(context.Background(), executionID)
	if err != nil { t.Fatal(err) }
	if len(steps) != 4 { t.Fatalf("steps = %d, want 4", len(steps)) }
	for _, step := range steps {
		if step.Status != StepCompleted { t.Fatalf("step %q status = %q", step.StepID, step.Status) }
		attempts, err := repo.ListStepAttempts(context.Background(), step.ID)
		if err != nil { t.Fatal(err) }
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
	if err == nil { t.Fatal("Execute() error = nil, want failure") }

	execution, getErr := repo.GetExecution(context.Background(), executionID)
	if getErr != nil { t.Fatal(getErr) }
	if execution.Status != ExecutionFailed { t.Fatalf("status = %q, want %q", execution.Status, ExecutionFailed) }
	if execution.Error == nil || execution.Error.Message != "boom" { t.Fatalf("execution error = %+v", execution.Error) }

	steps, listErr := repo.ListStepExecutions(context.Background(), executionID)
	if listErr != nil { t.Fatal(listErr) }
	byStep := make(map[string]StepExecution)
	for _, step := range steps { byStep[step.StepID] = step }
	if byStep["bad"].Status != StepFailed { t.Fatalf("bad status = %q", byStep["bad"].Status) }
	if byStep["later"].Status != StepPending { t.Fatalf("later status = %q", byStep["later"].Status) }
}
