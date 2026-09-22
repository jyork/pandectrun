package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrInvalidTransition = errors.New("invalid lifecycle transition")
)

// ExecutionEventType identifies a chronological lifecycle event emitted while an execution runs.
type ExecutionEventType string

const (
	EventExecutionCreated         ExecutionEventType = "execution.created"
	EventExecutionStarted         ExecutionEventType = "execution.started"
	EventExecutionCancelRequested ExecutionEventType = "execution.cancel_requested"
	EventExecutionCompleted       ExecutionEventType = "execution.completed"
	EventExecutionFailed          ExecutionEventType = "execution.failed"
	EventExecutionCancelled       ExecutionEventType = "execution.cancelled"
	EventStepStarted              ExecutionEventType = "step.started"
	EventStepAttemptStarted       ExecutionEventType = "step.attempt_started"
	EventStepAttemptFailed        ExecutionEventType = "step.attempt_failed"
	EventStepCompleted            ExecutionEventType = "step.completed"
	EventStepFailed               ExecutionEventType = "step.failed"
	EventStepCancelled            ExecutionEventType = "step.cancelled"
)

// ExecutionEvent records a durable point in an execution's lifecycle history.
// StepExecutionID and StepAttemptID are populated only for events at those scopes.
type ExecutionEvent struct {
	ExecutionID     ExecutionID        `json:"execution_id"`
	Type            ExecutionEventType `json:"type"`
	StepExecutionID StepExecutionID    `json:"step_execution_id,omitempty"`
	StepAttemptID   StepAttemptID      `json:"step_attempt_id,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
}

// ExecutionRepository persists execution, step, attempt, and event state through
// lifecycle-oriented operations. Implementations own runtime IDs and lifecycle
// timestamps and must keep each state transition and its corresponding event atomic.
type ExecutionRepository interface {
	CreateExecution(context.Context, NewExecution) (ExecutionID, error)
	GetExecution(context.Context, ExecutionID) (Execution, error)
	ListExecutions(context.Context, ExecutionQuery) ([]Execution, error)

	StartExecution(context.Context, ExecutionID) error
	CompleteExecution(context.Context, ExecutionID, json.RawMessage) error
	FailExecution(context.Context, ExecutionID, ExecutionError) error
	RequestCancellation(context.Context, ExecutionID) error
	CompleteCancellation(context.Context, ExecutionID) error

	CreateStepExecution(context.Context, NewStepExecution) (StepExecutionID, error)
	GetStepExecution(context.Context, StepExecutionID) (StepExecution, error)
	ListStepExecutions(context.Context, ExecutionID) ([]StepExecution, error)
	StartStep(context.Context, StepExecutionID) error
	CompleteStep(context.Context, StepExecutionID, json.RawMessage) error
	FailStep(context.Context, StepExecutionID, ExecutionError) error
	CancelStep(context.Context, StepExecutionID) error

	CreateStepAttempt(context.Context, NewStepAttempt) (StepAttemptID, error)
	CompleteStepAttempt(context.Context, StepAttemptID) error
	FailStepAttempt(context.Context, StepAttemptID, ExecutionError) error
	CancelStepAttempt(context.Context, StepAttemptID) error
	ListStepAttempts(context.Context, StepExecutionID) ([]StepAttempt, error)

	ListEvents(context.Context, ExecutionID) ([]ExecutionEvent, error)
}
