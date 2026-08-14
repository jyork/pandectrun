package engine

import "fmt"

// ValidateWorkflow verifies that a workflow definition is structurally valid
// before the scheduler attempts to execute it.
//
// Validation establishes the invariants the execution engine relies on:
//   - the workflow has a non-empty ID and a positive version;
//   - the workflow contains at least one step;
//   - every step has a non-empty ID and type;
//   - step IDs are unique within the workflow;
//   - every dependency references another step in the same workflow;
//   - a step cannot depend on itself; and
//   - the dependency graph is acyclic.
//
// The final check treats the workflow as a directed acyclic graph (DAG) and
// performs a three-color depth-first traversal. Encountering a step that is
// already in the current traversal path identifies a dependency cycle. Cyclic
// workflows are rejected because no valid execution order can satisfy their
// dependencies.
//
// ValidateWorkflow validates only workflow structure. It does not verify that
// a step type is registered with a scheduler, interpret step-specific Config,
// validate retry policy values, or perform any external I/O. Those checks
// belong to the components that own those concerns.
//
// A nil return means the definition is safe for the scheduler to reason about;
// it does not guarantee that execution will succeed.
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
