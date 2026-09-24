package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// StepInput contains the execution identity, definition, and accumulated workflow
// context supplied to a step implementation.
type StepInput struct {
	ExecutionID ExecutionID
	Step        StepDefinition
	Context     WorkflowContext
}

// StepResult contains the durable output produced by a successful step attempt.
type StepResult struct {
	Output json.RawMessage
}


// PermanentError marks an error as non-retryable. The scheduler fails the
// logical step immediately even when its definition has a retry policy.
type PermanentError struct {
	Err error
}

// Error returns the wrapped error message.
//
// Returns:
//   - string: the wrapped error message, or "permanent error" when the receiver
//     or wrapped error is nil.
func (e *PermanentError) Error() string {
	if e == nil || e.Err == nil {
		return "permanent error"
	}
	return e.Err.Error()
}

// Unwrap exposes the underlying error for errors.Is and errors.As.
//
// Returns:
//   - error: the wrapped error, or nil when the receiver is nil.
func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Permanent wraps err so the scheduler treats it as non-retryable.
// A nil error remains nil.
//
// Parameters:
//   - err: the error to classify as permanent.
//
// Returns:
//   - error: err wrapped as a PermanentError, the original error when it is
//     already permanent, or nil when err is nil.
func Permanent(err error) error {
	if err == nil || IsPermanent(err) {
		return err
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err or any wrapped error is explicitly permanent.
//
// Parameters:
//   - err: the error chain to inspect.
//
// Returns:
//   - bool: true when err contains a PermanentError; otherwise false.
func IsPermanent(err error) bool {
	var permanentErr *PermanentError
	return errors.As(err, &permanentErr)
}

// RetryClassifier is an optional capability implemented by steps that need
// domain-specific retry decisions. It is consulted only when a retry policy is
// configured and the error has not already been marked permanent.
type RetryClassifier interface {
	IsRetryable(error) bool
}

// Step executes one workflow step. Implementations receive constructed
// dependencies when they are registered; workflow context is data, not a
// service locator. Implementations should honor context cancellation and return
// promptly when the underlying operation can be cancelled.
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
//
// Returns:
//   - *Registry: a newly initialized, empty registry.
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
//
// Parameters:
//   - stepType: the StepDefinition.Type value used to resolve the implementation.
//   - implementation: the Step implementation associated with stepType.
//
// Returns:
//   - error: nil on success; otherwise an error for an empty type, nil
//     implementation, or duplicate registration.
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
//
// Parameters:
//   - stepType: the StepDefinition.Type value to resolve.
//
// Returns:
//   - Step: the registered implementation, or nil when stepType is not registered.
//   - bool: true when an implementation was found; otherwise false.
func (r *Registry) Get(stepType string) (Step, bool) {
	step, ok := r.steps[stepType]
	return step, ok
}
