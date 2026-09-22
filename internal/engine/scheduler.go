package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cinar/resile"
)

// Scheduler executes validated workflow definitions sequentially and persists
// execution, step, attempt, and event history through an ExecutionRepository.
type Scheduler struct {
	registry   *Registry
	repository ExecutionRepository
}

// NewScheduler constructs a scheduler using registry to resolve step
// implementations and repository as the authoritative execution store.
func NewScheduler(registry *Registry, repository ExecutionRepository) *Scheduler {
	return &Scheduler{registry: registry, repository: repository}
}

// Execute runs one workflow execution and returns its repository-assigned ID.
// Step execution is sequential in v1; configured retry policies may cause a
// logical step to have multiple persisted attempts before it reaches a terminal state.
func (s *Scheduler) Execute(ctx context.Context, def WorkflowDefinition, input json.RawMessage) (ExecutionID, error) {
	if err := ValidateWorkflow(def); err != nil {
		return "", err
	}
	if s.registry == nil {
		return "", fmt.Errorf("step registry is required")
	}
	if s.repository == nil {
		return "", fmt.Errorf("execution repository is required")
	}

	executionID, err := s.repository.CreateExecution(ctx, NewExecution{
		WorkflowID:      def.ID,
		WorkflowVersion: def.Version,
		Input:           input,
	})
	if err != nil {
		return "", fmt.Errorf("create execution: %w", err)
	}

	stepIDs := make(map[string]StepExecutionID, len(def.Steps))
	statuses := make(map[string]StepStatus, len(def.Steps))
	for _, step := range def.Steps {
		stepID, createErr := s.repository.CreateStepExecution(ctx, NewStepExecution{
			ExecutionID: executionID,
			StepID:      step.ID,
		})
		if createErr != nil {
			return executionID, fmt.Errorf("create step %q: %w", step.ID, createErr)
		}
		stepIDs[step.ID] = stepID
		statuses[step.ID] = StepPending
	}

	if err := s.repository.StartExecution(ctx, executionID); err != nil {
		return executionID, fmt.Errorf("start execution: %w", err)
	}

	workflowContext := WorkflowContext{
		Input: cloneRawMessage(input),
		Steps: make(map[string]json.RawMessage, len(def.Steps)),
	}

	completed := 0
	for completed < len(def.Steps) {
		if err := ctx.Err(); err != nil {
			execErr := ExecutionError{Kind: ErrorCancelled, Message: err.Error()}
			_ = s.repository.FailExecution(context.Background(), executionID, execErr)
			return executionID, err
		}

		execution, err := s.repository.GetExecution(ctx, executionID)
		if err != nil {
			return executionID, fmt.Errorf("load execution: %w", err)
		}
		if execution.Status == ExecutionCancelRequested {
			for stepID, status := range statuses {
				if status == StepPending {
					if cancelErr := s.repository.CancelStep(ctx, stepIDs[stepID]); cancelErr != nil {
						return executionID, fmt.Errorf("cancel step %q: %w", stepID, cancelErr)
					}
					statuses[stepID] = StepCancelled
				}
			}
			if err := s.repository.CompleteCancellation(ctx, executionID); err != nil {
				return executionID, fmt.Errorf("complete cancellation: %w", err)
			}
			return executionID, context.Canceled
		}

		runnable := RunnableSteps(def, statuses)
		if len(runnable) == 0 {
			err := fmt.Errorf("workflow has no runnable steps before completion")
			_ = s.repository.FailExecution(ctx, executionID, ExecutionError{Kind: ErrorTerminal, Message: err.Error()})
			return executionID, err
		}

		// v1 intentionally runs one eligible step at a time. RunnableSteps
		// exposes future parallelism while preserving deterministic behavior now.
		stepDef := runnable[0]
		implementation, ok := s.registry.Get(stepDef.Type)
		if !ok {
			err := fmt.Errorf("step type %q is not registered", stepDef.Type)
			_ = s.repository.FailExecution(ctx, executionID, ExecutionError{Kind: ErrorTerminal, Message: err.Error()})
			return executionID, err
		}

		stepExecutionID := stepIDs[stepDef.ID]
		if err := s.repository.StartStep(ctx, stepExecutionID); err != nil {
			return executionID, fmt.Errorf("start step %q: %w", stepDef.ID, err)
		}
		statuses[stepDef.ID] = StepRunning

		result, stepErr := s.executeStep(ctx, executionID, stepExecutionID, stepDef, implementation, workflowContext)
		if stepErr != nil {
			execErr := ExecutionError{Kind: ErrorTerminal, Message: stepErr.Error()}
			if err := s.repository.FailStep(ctx, stepExecutionID, execErr); err != nil {
				return executionID, fmt.Errorf("fail step %q: %w", stepDef.ID, err)
			}
			statuses[stepDef.ID] = StepFailed
			if err := s.repository.FailExecution(ctx, executionID, execErr); err != nil {
				return executionID, fmt.Errorf("fail execution after step %q: %w", stepDef.ID, err)
			}
			return executionID, fmt.Errorf("step %q failed: %w", stepDef.ID, stepErr)
		}

		if err := s.repository.CompleteStep(ctx, stepExecutionID, result.Output); err != nil {
			return executionID, fmt.Errorf("complete step %q: %w", stepDef.ID, err)
		}

		workflowContext.Steps[stepDef.ID] = cloneRawMessage(result.Output)
		statuses[stepDef.ID] = StepCompleted
		completed++
	}

	if err := s.repository.CompleteExecution(ctx, executionID, nil); err != nil {
		return executionID, fmt.Errorf("complete execution: %w", err)
	}
	return executionID, nil
}

// executeStep runs one logical step and persists every invocation as a distinct
// StepAttempt. Resile owns retry timing; PandectRun owns retry eligibility,
// persistence, and lifecycle events.
func (s *Scheduler) executeStep(
	ctx context.Context,
	executionID ExecutionID,
	stepExecutionID StepExecutionID,
	stepDef StepDefinition,
	implementation Step,
	workflowContext WorkflowContext,
) (StepResult, error) {
	maxAttempts := stepDef.Retry.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 1
	}

	attemptNumber := uint(0)
	var lastStepErr error
	action := func(attemptCtx context.Context) (StepResult, error) {
		attemptNumber++

		attemptID, err := s.repository.CreateStepAttempt(attemptCtx, NewStepAttempt{
			StepExecutionID: stepExecutionID,
			Attempt:         attemptNumber,
		})
		if err != nil {
			return StepResult{}, fmt.Errorf("create attempt for step %q: %w", stepDef.ID, err)
		}

		result, stepErr := implementation.Execute(attemptCtx, StepInput{
			ExecutionID: executionID,
			Step:        stepDef,
			Context:     workflowContext,
		})
		if stepErr == nil {
			if err := s.repository.CompleteStepAttempt(attemptCtx, attemptID); err != nil {
				return StepResult{}, fmt.Errorf("complete attempt for step %q: %w", stepDef.ID, err)
			}
			return result, nil
		}

		lastStepErr = stepErr

		if errors.Is(attemptCtx.Err(), context.Canceled) {
			if err := s.repository.CancelStepAttempt(context.Background(), attemptID); err != nil {
				return StepResult{}, fmt.Errorf("cancel attempt for step %q: %w", stepDef.ID, err)
			}
			return StepResult{}, stepErr
		}

		retryable := s.isRetryable(stepDef.Retry, implementation, stepErr)
		kind := ErrorTerminal
		if retryable {
			kind = ErrorRetryable
		}
		execErr := ExecutionError{Kind: kind, Message: stepErr.Error()}
		if err := s.repository.FailStepAttempt(attemptCtx, attemptID, execErr); err != nil {
			return StepResult{}, fmt.Errorf("fail attempt for step %q: %w", stepDef.ID, err)
		}

		if retryable && attemptNumber < maxAttempts {
			if err := s.repository.RecordStepRetryScheduled(attemptCtx, attemptID); err != nil {
				return StepResult{}, fmt.Errorf("record retry for step %q: %w", stepDef.ID, err)
			}
		}

		if !retryable {
			return StepResult{}, Permanent(stepErr)
		}
		return StepResult{}, stepErr
	}

	if maxAttempts == 1 {
		result, err := action(ctx)
		return result, unwrapPermanent(err)
	}

	options := []resile.Option{
		resile.WithMaxAttempts(maxAttempts),
		resile.WithRetryIfFunc(func(err error) bool {
			return !IsPermanent(err)
		}),
	}
	if stepDef.Retry.BaseDelay > 0 {
		options = append(options, resile.WithBaseDelay(stepDef.Retry.BaseDelay))
	}
	if stepDef.Retry.MaxDelay > 0 {
		options = append(options, resile.WithMaxDelay(stepDef.Retry.MaxDelay))
	}

	result, err := resile.Do(ctx, action, options...)
	if err != nil {
		if ctx.Err() != nil {
			return StepResult{}, ctx.Err()
		}
		if lastStepErr != nil {
			return StepResult{}, unwrapPermanent(lastStepErr)
		}
		return StepResult{}, unwrapPermanent(err)
	}
	return result, nil
}

// isRetryable applies PandectRun's retry precedence. Retries require more than
// one configured attempt, explicit permanent errors always stop retries, and an
// optional step classifier decides otherwise-unclassified errors.
func (s *Scheduler) isRetryable(policy RetryPolicy, implementation Step, err error) bool {
	if policy.MaxAttempts <= 1 {
		return false
	}
	if IsPermanent(err) {
		return false
	}
	if classifier, ok := implementation.(RetryClassifier); ok {
		return classifier.IsRetryable(err)
	}
	return true
}

// unwrapPermanent returns the underlying step error when retry classification
// wrapped it as permanent, keeping PandectRun's control marker out of user-facing errors.
func unwrapPermanent(err error) error {
	if err == nil {
		return nil
	}

	var permanentErr *PermanentError
	if errors.As(err, &permanentErr) && permanentErr.Err != nil {
		return permanentErr.Err
	}
	return err
}

// cloneRawMessage returns a copy of value so callers cannot mutate persisted or
// accumulated workflow state through a shared byte slice.
func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copyValue := make(json.RawMessage, len(value))
	copy(copyValue, value)
	return copyValue
}
