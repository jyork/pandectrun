package engine

import (
	"encoding/json"
	"time"
)

type ExecutionID string
type StepExecutionID string
type StepAttemptID string

type ExecutionStatus string

const (
	ExecutionPending         ExecutionStatus = "pending"
	ExecutionRunning         ExecutionStatus = "running"
	ExecutionCancelRequested ExecutionStatus = "cancel_requested"
	ExecutionCompleted       ExecutionStatus = "completed"
	ExecutionFailed          ExecutionStatus = "failed"
	ExecutionCancelled       ExecutionStatus = "cancelled"
)

type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	StepCancelled StepStatus = "cancelled"
)

type StepAttemptStatus string

const (
	StepAttemptRunning   StepAttemptStatus = "running"
	StepAttemptCompleted StepAttemptStatus = "completed"
	StepAttemptFailed    StepAttemptStatus = "failed"
	StepAttemptCancelled StepAttemptStatus = "cancelled"
)

type ErrorKind string

const (
	ErrorRetryable ErrorKind = "retryable"
	ErrorTerminal  ErrorKind = "terminal"
	ErrorCancelled ErrorKind = "cancelled"
)

type ExecutionError struct {
	Kind    ErrorKind `json:"kind"`
	Code    string    `json:"code,omitempty"`
	Message string    `json:"message"`
}

type RetryPolicy struct {
	MaxAttempts uint          `json:"max_attempts,omitempty"`
	BaseDelay   time.Duration `json:"base_delay,omitempty"`
	MaxDelay    time.Duration `json:"max_delay,omitempty"`
}

type WorkflowDefinition struct {
	ID      string           `json:"id"`
	Name    string           `json:"name"`
	Version int              `json:"version"`
	Steps   []StepDefinition `json:"steps"`
}

type StepDefinition struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	DependsOn []string        `json:"depends_on,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
	Retry     RetryPolicy     `json:"retry,omitempty"`
}

type NewExecution struct {
	WorkflowID      string
	WorkflowVersion int
	Input           json.RawMessage
}

type NewStepExecution struct {
	ExecutionID ExecutionID
	StepID      string
}

type NewStepAttempt struct {
	StepExecutionID StepExecutionID
	Attempt         uint
}

type ExecutionQuery struct {
	WorkflowID string
	Status     []ExecutionStatus
	Limit      int
}

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

type StepAttempt struct {
	ID              StepAttemptID     `json:"id"`
	StepExecutionID StepExecutionID   `json:"step_execution_id"`
	Attempt         uint              `json:"attempt"`
	Status          StepAttemptStatus `json:"status"`
	StartedAt       time.Time         `json:"started_at"`
	CompletedAt     *time.Time        `json:"completed_at,omitempty"`
	Error           *ExecutionError   `json:"error,omitempty"`
}

type WorkflowContext struct {
	Input json.RawMessage            `json:"input"`
	Steps map[string]json.RawMessage `json:"steps"`
}
