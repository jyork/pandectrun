package engine

import (
	"encoding/json"
	"time"
)

type ExecutionID string
type StepExecutionID string
type StepAttemptID string

// ExecutionStatus describes the lifecycle state of a workflow execution.
type ExecutionStatus string

const (
	ExecutionPending         ExecutionStatus = "pending"
	ExecutionRunning         ExecutionStatus = "running"
	ExecutionCancelRequested ExecutionStatus = "cancel_requested"
	ExecutionCompleted       ExecutionStatus = "completed"
	ExecutionFailed          ExecutionStatus = "failed"
	ExecutionCancelled       ExecutionStatus = "cancelled"
)

// StepStatus describes the lifecycle state of a logical step within an execution.
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	StepCancelled StepStatus = "cancelled"
)

// StepAttemptStatus describes the outcome or current state of one step attempt.
type StepAttemptStatus string

const (
	StepAttemptRunning   StepAttemptStatus = "running"
	StepAttemptCompleted StepAttemptStatus = "completed"
	StepAttemptFailed    StepAttemptStatus = "failed"
	StepAttemptCancelled StepAttemptStatus = "cancelled"
)

// ErrorKind classifies an execution error for orchestration decisions such as retry or cancellation.
type ErrorKind string

const (
	ErrorRetryable ErrorKind = "retryable"
	ErrorTerminal  ErrorKind = "terminal"
	ErrorCancelled ErrorKind = "cancelled"
)

// ExecutionError is the durable, structured representation of an execution or step failure.
type ExecutionError struct {
	Kind    ErrorKind `json:"kind"`
	Code    string    `json:"code,omitempty"`
	Message string    `json:"message"`
}

// RetryPolicy defines the engine-owned retry limits and backoff parameters for a step.
type RetryPolicy struct {
	MaxAttempts uint          `json:"max_attempts,omitempty"`
	BaseDelay   time.Duration `json:"base_delay,omitempty"`
	MaxDelay    time.Duration `json:"max_delay,omitempty"`
}

// WorkflowDefinition is an immutable, versioned description of a workflow DAG.
type WorkflowDefinition struct {
	ID      string           `json:"id"`
	Name    string           `json:"name"`
	Version int              `json:"version"`
	Steps   []StepDefinition `json:"steps"`
}

// StepDefinition describes one logical node in a workflow DAG and its implementation-specific configuration.
type StepDefinition struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	DependsOn []string        `json:"depends_on,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
	Retry     RetryPolicy     `json:"retry,omitempty"`
}

// NewExecution contains caller-supplied data required to create an Execution.
// The repository assigns the execution ID, initial status, and lifecycle timestamps.
type NewExecution struct {
	WorkflowID      string
	WorkflowVersion int
	Input           json.RawMessage
}

// NewStepExecution contains caller-supplied data required to create a StepExecution.
// The repository assigns the step-execution ID and initial lifecycle state.
type NewStepExecution struct {
	ExecutionID ExecutionID
	StepID      string
}

// NewStepAttempt contains caller-supplied data required to create one attempt for a StepExecution.
// Attempt is the one-based ordinal of the attempt within that step execution.
type NewStepAttempt struct {
	StepExecutionID StepExecutionID
	Attempt         uint
}

// ExecutionQuery filters persisted executions. Empty WorkflowID and Status values
// match all workflows and statuses; Limit <= 0 leaves the result unbounded.
type ExecutionQuery struct {
	WorkflowID string
	Status     []ExecutionStatus
	Limit      int
}

// Execution is the durable runtime record for one invocation of a workflow definition.
type Execution struct {
	ID              ExecutionID     `json:"id"`
	WorkflowID      string          `json:"workflow_id"`
	WorkflowVersion int             `json:"workflow_version"`
	Status          ExecutionStatus `json:"status"`
	Input           json.RawMessage `json:"input"`
	Output          json.RawMessage `json:"output,omitempty"`
	Error           *ExecutionError `json:"error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
}

// StepExecution is the durable runtime record for one logical step within an Execution.
type StepExecution struct {
	ID          StepExecutionID `json:"id"`
	ExecutionID ExecutionID     `json:"execution_id"`
	StepID      string          `json:"step_id"`
	Status      StepStatus      `json:"status"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       *ExecutionError `json:"error,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

// StepAttempt is the durable record of one invocation attempt for a StepExecution.
type StepAttempt struct {
	ID              StepAttemptID     `json:"id"`
	StepExecutionID StepExecutionID   `json:"step_execution_id"`
	Attempt         uint              `json:"attempt"`
	Status          StepAttemptStatus `json:"status"`
	StartedAt       time.Time         `json:"started_at"`
	CompletedAt     *time.Time        `json:"completed_at,omitempty"`
	Error           *ExecutionError   `json:"error,omitempty"`
}

// WorkflowContext is the step-facing view constructed from workflow input and completed step outputs.
type WorkflowContext struct {
	Input json.RawMessage            `json:"input"`
	Steps map[string]json.RawMessage `json:"steps"`
}
