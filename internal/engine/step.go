package engine

import (
	"context"
	"encoding/json"
	"fmt"
)

type StepInput struct {
	ExecutionID string
	Step        StepDefinition
	Context     WorkflowContext
}

type StepResult struct {
	Output json.RawMessage
}

// Step executes one workflow step. Implementations receive constructed
// dependencies when they are registered; workflow context is data, not a
// service locator.
type Step interface {
	Execute(context.Context, StepInput) (StepResult, error)
}

type Registry struct {
	steps map[string]Step
}

func NewRegistry() *Registry {
	return &Registry{steps: make(map[string]Step)}
}

func (r *Registry) Register(stepType string, implementation Step) error {
	if stepType == "" {
		return fmt.Errorf("step type is required")
	}
	if implementation == nil {
		return fmt.Errorf("step implementation for %q is nil", stepType)
	}
	if _, exists := r.steps[stepType]; exists {
		return fmt.Errorf("step type %q is already registered", stepType)
	}
	r.steps[stepType] = implementation
	return nil
}

func (r *Registry) Get(stepType string) (Step, bool) {
	step, ok := r.steps[stepType]
	return step, ok
}
