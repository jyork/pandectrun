package engine

import (
	"context"
	"encoding/json"
	"fmt"
)

// Scheduler executes validated workflow definitions sequentially and persists
// execution, step, attempt, and event history through an ExecutionRepository.
type Scheduler struct {
	registry   *Registry
	repository ExecutionRepository
}

func NewScheduler(registry *Registry, repository ExecutionRepository) *Scheduler {
	return &Scheduler{registry: registry, repository: repository}
}

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

		attemptID, err := s.repository.CreateStepAttempt(ctx, NewStepAttempt{
			StepExecutionID: stepExecutionID,
			Attempt:         1,
		})
		if err != nil {
			return executionID, fmt.Errorf("create attempt for step %q: %w", stepDef.ID, err)
		}

		result, stepErr := implementation.Execute(ctx, StepInput{
			ExecutionID: executionID,
			Step:        stepDef,
			Context:     workflowContext,
		})
		if stepErr != nil {
			execErr := ExecutionError{Kind: ErrorTerminal, Message: stepErr.Error()}
			if err := s.repository.FailStepAttempt(ctx, attemptID, execErr); err != nil {
				return executionID, fmt.Errorf("fail attempt for step %q: %w", stepDef.ID, err)
			}
			if err := s.repository.FailStep(ctx, stepExecutionID, execErr); err != nil {
				return executionID, fmt.Errorf("fail step %q: %w", stepDef.ID, err)
			}
			statuses[stepDef.ID] = StepFailed
			if err := s.repository.FailExecution(ctx, executionID, execErr); err != nil {
				return executionID, fmt.Errorf("fail execution after step %q: %w", stepDef.ID, err)
			}
			return executionID, fmt.Errorf("step %q failed: %w", stepDef.ID, stepErr)
		}

		if err := s.repository.CompleteStepAttempt(ctx, attemptID); err != nil {
			return executionID, fmt.Errorf("complete attempt for step %q: %w", stepDef.ID, err)
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

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copyValue := make(json.RawMessage, len(value))
	copy(copyValue, value)
	return copyValue
}
