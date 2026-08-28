package engine

import (
	"context"
	"encoding/json"
	"fmt"
)

// Scheduler executes validated workflow definitions sequentially.
// Persistence, retries, and distributed workers will be layered around this
// execution contract without changing workflow-definition semantics.
type Scheduler struct {
	registry *Registry
}

func NewScheduler(registry *Registry) *Scheduler {
	return &Scheduler{registry: registry}
}

func (s *Scheduler) Execute(ctx context.Context, executionID string, def WorkflowDefinition, input json.RawMessage) (WorkflowContext, error) {
	if err := ValidateWorkflow(def); err != nil {
		return WorkflowContext{}, err
	}
	if s.registry == nil {
		return WorkflowContext{}, fmt.Errorf("step registry is required")
	}

	statuses := make(map[string]StepStatus, len(def.Steps))
	workflowContext := WorkflowContext{
		Input: input,
		Steps: make(map[string]json.RawMessage, len(def.Steps)),
	}
	for _, step := range def.Steps {
		statuses[step.ID] = StepPending
	}

	completed := 0
	for completed < len(def.Steps) {
		if err := ctx.Err(); err != nil {
			return workflowContext, err
		}

		runnable := RunnableSteps(def, statuses)
		if len(runnable) == 0 {
			return workflowContext, fmt.Errorf("workflow has no runnable steps before completion")
		}

		// v1 intentionally runs one eligible step at a time. RunnableSteps
		// exposes future parallelism while preserving deterministic behavior now.
		stepDef := runnable[0]
		implementation, ok := s.registry.Get(stepDef.Type)
		if !ok {
			return workflowContext, fmt.Errorf("step type %q is not registered", stepDef.Type)
		}

		statuses[stepDef.ID] = StepRunning
		result, err := implementation.Execute(ctx, StepInput{
			ExecutionID: executionID,
			Step:        stepDef,
			Context:     workflowContext,
		})
		if err != nil {
			statuses[stepDef.ID] = StepFailed
			return workflowContext, fmt.Errorf("step %q failed: %w", stepDef.ID, err)
		}

		// Completed outputs are immutable within this execution. A step is
		// written exactly once before becoming visible to downstream steps.
		workflowContext.Steps[stepDef.ID] = cloneRawMessage(result.Output)
		statuses[stepDef.ID] = StepCompleted
		completed++
	}

	return workflowContext, nil
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copyValue := make(json.RawMessage, len(value))
	copy(copyValue, value)
	return copyValue
}
