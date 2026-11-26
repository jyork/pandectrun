package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/jyork/pandectrun/internal/engine"
)

func TestStepPersistence(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parent, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	other, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "other", WorkflowVersion: 1})
	requireSuccess(t, err)
	steps, err := r.ListStepExecutions(ctx, parent)
	requireSuccess(t, err)
	if len(steps) != 0 {
		t.Fatalf("expected empty step list: %+v", steps)
	}
	first, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "first"})
	requireSuccess(t, err)
	second, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "second"})
	requireSuccess(t, err)
	third, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "third"})
	requireSuccess(t, err)
	fourth, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "fourth"})
	requireSuccess(t, err)
	_, err = r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: other, StepID: "first"})
	requireSuccess(t, err)
	if first <= 0 || second <= first || third <= second || fourth <= third {
		t.Fatal("expected increasing positive step IDs")
	}
	step, err := r.GetStepExecution(ctx, first)
	requireSuccess(t, err)
	if step.ID != first || step.ExecutionID != parent || step.StepID != "first" || step.Status != engine.StepPending ||
		step.StartedAt != nil || step.CompletedAt != nil || step.Output != nil || step.Error != nil {
		t.Fatalf("unexpected pending step: %+v", step)
	}
	if err := r.CompleteStep(ctx, first, nil); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("pending completion: %v", err)
	}
	requireSuccess(t, r.StartExecution(ctx, parent))
	requireSuccess(t, r.StartStep(ctx, first))
	output := json.RawMessage("null")
	requireSuccess(t, r.CompleteStep(ctx, first, output))
	output[0] = 'X'
	if err := r.CompleteStep(ctx, first, json.RawMessage(`{}`)); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("repeated completion: %v", err)
	}
	if err := r.CancelStep(ctx, first); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("cancel completed step: %v", err)
	}
	step, err = r.GetStepExecution(ctx, first)
	requireSuccess(t, err)
	if step.Status != engine.StepCompleted || string(step.Output) != "null" || step.StartedAt == nil ||
		step.CompletedAt == nil || step.CompletedAt.Before(*step.StartedAt) || step.Error != nil {
		t.Fatalf("unexpected completed step: %+v", step)
	}

	requireSuccess(t, r.StartStep(ctx, second))
	failure := engine.ExecutionError{Kind: engine.ErrorTerminal, Code: "test", Message: "failure"}
	requireSuccess(t, r.FailStep(ctx, second, failure))
	step, err = r.GetStepExecution(ctx, second)
	requireSuccess(t, err)
	if step.Status != engine.StepFailed || step.Error == nil || *step.Error != failure || step.Output != nil ||
		step.StartedAt == nil || step.CompletedAt == nil {
		t.Fatalf("unexpected failed step: %+v", step)
	}
	requireSuccess(t, r.CancelStep(ctx, third))
	requireSuccess(t, r.CancelStep(ctx, third))
	step, err = r.GetStepExecution(ctx, third)
	requireSuccess(t, err)
	if step.Status != engine.StepCancelled || step.StartedAt != nil || step.CompletedAt == nil {
		t.Fatalf("unexpected cancelled pending step: %+v", step)
	}
	requireSuccess(t, r.StartStep(ctx, fourth))
	requireSuccess(t, r.CancelStep(ctx, fourth))
	step, err = r.GetStepExecution(ctx, fourth)
	requireSuccess(t, err)
	if step.Status != engine.StepCancelled || step.StartedAt == nil || step.CompletedAt == nil {
		t.Fatalf("unexpected cancelled running step: %+v", step)
	}
	steps, err = r.ListStepExecutions(ctx, parent)
	requireSuccess(t, err)
	if len(steps) != 4 || steps[0].ID != first || steps[1].ID != second || steps[2].ID != third || steps[3].ID != fourth {
		t.Fatalf("unexpected step list: %+v", steps)
	}
	assertExecutionEvents(t, r, parent, "execution.created", "execution.started", "step.started", "step.completed",
		"step.started", "step.failed", "step.cancelled", "step.started", "step.cancelled")
	var count int
	err = r.pool.QueryRow(ctx, `SELECT count(*) FROM execution_events
		WHERE execution_id = $1::text::uuid AND step_execution_id = $2 AND step_attempt_id IS NULL`, string(parent), int64(first)).Scan(&count)
	requireSuccess(t, err)
	if count != 2 {
		t.Fatalf("first step event references = %d, want 2", count)
	}
}

func TestStepMissingRecordsAndDuplicate(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	missing := engine.ExecutionID(uuid.NewV4().String())
	_, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: missing, StepID: "missing"})
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing parent: %v", err)
	}
	_, err = r.ListStepExecutions(ctx, missing)
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing parent list: %v", err)
	}
	_, err = r.GetStepExecution(ctx, 1)
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing step: %v", err)
	}
	if err := r.StartStep(ctx, 1); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing step transition: %v", err)
	}
	parent, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	in := engine.NewStepExecution{ExecutionID: parent, StepID: "unique"}
	_, err = r.CreateStepExecution(ctx, in)
	requireSuccess(t, err)
	_, err = r.CreateStepExecution(ctx, in)
	if !errors.Is(err, engine.ErrAlreadyExists) {
		t.Fatalf("duplicate step: %v", err)
	}
}

func TestStepConcurrentCompletion(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parent, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	id, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "step"})
	requireSuccess(t, err)
	requireSuccess(t, r.StartExecution(ctx, parent))
	requireSuccess(t, r.StartStep(ctx, id))
	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- r.CompleteStep(ctx, id, json.RawMessage(`{"ok":true}`))
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, engine.ErrInvalidTransition) {
			t.Errorf("unexpected completion error: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successful completions = %d, want 1", success)
	}
	assertExecutionEvents(t, r, parent, "execution.created", "execution.started", "step.started", "step.completed")
}

func TestStepEventFailureRollsBackState(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parent, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	id, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "step"})
	requireSuccess(t, err)
	requireSuccess(t, r.StartExecution(ctx, parent))
	requireSuccess(t, r.StartStep(ctx, id))
	_, err = r.pool.Exec(ctx, "ALTER TABLE execution_events ADD CHECK (type <> 'step.completed')")
	requireSuccess(t, err)
	if err := r.CompleteStep(ctx, id, json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected event insertion failure")
	}
	step, err := r.GetStepExecution(ctx, id)
	requireSuccess(t, err)
	if step.Status != engine.StepRunning || step.CompletedAt != nil || step.Output != nil {
		t.Fatalf("step changed despite event failure: %+v", step)
	}
	assertExecutionEvents(t, r, parent, "execution.created", "execution.started", "step.started")
}
