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

func TestSchedulerExecutesDAGSequentiallyInDefinitionOrder(t *testing.T) {
	var order []string
	registry := NewRegistry()
	if err := registry.Register("record", recordingStep{order: &order}); err != nil {
		t.Fatal(err)
	}

	def := WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
		{ID: "validate", Type: "record"},
		{ID: "fetch-a", Type: "record", DependsOn: []string{"validate"}},
		{ID: "fetch-b", Type: "record", DependsOn: []string{"validate"}},
		{ID: "analyze", Type: "record", DependsOn: []string{"fetch-a", "fetch-b"}},
	}}

	got, err := NewScheduler(registry).Execute(context.Background(), "exec-1", def, json.RawMessage(`{"service":"checkout"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	wantOrder := []string{"validate", "fetch-a", "fetch-b", "analyze"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("execution order = %v, want %v", order, wantOrder)
	}
	if len(got.Steps) != 4 {
		t.Fatalf("completed outputs = %d, want 4", len(got.Steps))
	}
}

func TestSchedulerFailsFast(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("fail", failingStep{}); err != nil {
		t.Fatal(err)
	}

	def := WorkflowDefinition{ID: "wf", Version: 1, Steps: []StepDefinition{
		{ID: "bad", Type: "fail"},
		{ID: "later", Type: "fail", DependsOn: []string{"bad"}},
	}}

	got, err := NewScheduler(registry).Execute(context.Background(), "exec-1", def, nil)
	if err == nil {
		t.Fatal("Execute() error = nil, want failure")
	}
	if len(got.Steps) != 0 {
		t.Fatalf("completed outputs = %d, want 0", len(got.Steps))
	}
}
