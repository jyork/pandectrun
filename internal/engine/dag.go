package engine

import "fmt"

// ValidateWorkflow checks structural invariants required by the scheduler.
func ValidateWorkflow(def WorkflowDefinition) error {
	if def.ID == "" {
		return fmt.Errorf("workflow id is required")
	}
	if def.Version <= 0 {
		return fmt.Errorf("workflow version must be positive")
	}
	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow must contain at least one step")
	}

	steps := make(map[string]StepDefinition, len(def.Steps))
	for _, step := range def.Steps {
		if step.ID == "" {
			return fmt.Errorf("step id is required")
		}
		if step.Type == "" {
			return fmt.Errorf("step %q type is required", step.ID)
		}
		if _, exists := steps[step.ID]; exists {
			return fmt.Errorf("duplicate step id %q", step.ID)
		}
		steps[step.ID] = step
	}

	for _, step := range def.Steps {
		for _, dependency := range step.DependsOn {
			if dependency == step.ID {
				return fmt.Errorf("step %q cannot depend on itself", step.ID)
			}
			if _, exists := steps[dependency]; !exists {
				return fmt.Errorf("step %q depends on unknown step %q", step.ID, dependency)
			}
		}
	}

	// Three-color depth-first traversal: 0 = unseen, 1 = visiting, 2 = done.
	state := make(map[string]uint8, len(def.Steps))
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return fmt.Errorf("workflow contains a dependency cycle involving step %q", id)
		case 2:
			return nil
		}

		state[id] = 1
		for _, dependency := range steps[id].DependsOn {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}

	for _, step := range def.Steps {
		if err := visit(step.ID); err != nil {
			return err
		}
	}
	return nil
}

// RunnableSteps returns pending steps whose dependencies have completed.
// The definition order is preserved, giving the initial sequential scheduler
// deterministic execution ordering when multiple steps are runnable.
func RunnableSteps(def WorkflowDefinition, statuses map[string]StepStatus) []StepDefinition {
	var runnable []StepDefinition
	for _, step := range def.Steps {
		if statuses[step.ID] != StepPending {
			continue
		}

		ready := true
		for _, dependency := range step.DependsOn {
			if statuses[dependency] != StepCompleted {
				ready = false
				break
			}
		}
		if ready {
			runnable = append(runnable, step)
		}
	}
	return runnable
}
