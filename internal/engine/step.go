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

// Registry maps workflow step type names to their executable implementations.
//
// Workflow definitions refer to implementations indirectly through
// StepDefinition.Type values such as "validation", "http", or "llm". The
// scheduler uses the registry to resolve those names at execution time rather
// than depending directly on concrete step implementations.
//
// A registry owns at most one implementation for a given step type. Duplicate
// registrations are rejected so application startup fails explicitly instead
// of silently replacing one implementation with another.
//
// Registry is intentionally small and currently assumes registrations happen
// during application construction, before workflow execution begins. It does
// not provide synchronization for concurrent mutation. Once constructed, it is
// safe for concurrent readers as long as no goroutine is registering new step
// implementations at the same time.
type Registry struct {
	steps map[string]Step
}

// NewRegistry returns an empty Registry ready to accept step implementations.
// Applications normally construct one registry during startup, register all
// supported step types, and then share that registry with the scheduler.
func NewRegistry() *Registry {
	return &Registry{steps: make(map[string]Step)}
}

// Register associates stepType with implementation.
//
// stepType is the value workflow definitions place in StepDefinition.Type.
// Register returns an error when the type name is empty, the implementation is
// nil, or the type has already been registered. Existing registrations are
// never overwritten.
//
// Register is intended for application initialization rather than dynamic
// runtime mutation. Callers should complete registration before workflows are
// executed or the registry is shared between goroutines.
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

// Get resolves a registered implementation for stepType.
//
// The returned boolean is false when no implementation has been registered for
// the requested type. The scheduler can use that distinction to report an
// unsupported step type as an execution/configuration error rather than
// invoking a nil implementation.
//
// Get does not modify registry state and may be called concurrently provided
// the registry is no longer being mutated through Register.
func (r *Registry) Get(stepType string) (Step, bool) {
	step, ok := r.steps[stepType]
	return step, ok
}
