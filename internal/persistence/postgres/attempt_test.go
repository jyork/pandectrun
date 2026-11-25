package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jyork/pandectrun/internal/engine"
	"github.com/jyork/pandectrun/internal/persistence/repositorytest"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.Run(t, func(t *testing.T) engine.ExecutionRepository {
		return migratedRepository(t)
	})
}

func TestAttemptCancellationAndRollback(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parent, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	requireSuccess(t, r.StartExecution(ctx, parent))
	step, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "step"})
	requireSuccess(t, err)
	in := engine.NewStepAttempt{StepExecutionID: step, Attempt: 1}
	if _, err := r.CreateStepAttempt(ctx, in); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("attempt on pending step: %v", err)
	}
	requireSuccess(t, r.StartStep(ctx, step))
	id, err := r.CreateStepAttempt(ctx, in)
	requireSuccess(t, err)
	if err := r.RecordStepRetryScheduled(ctx, id); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("retry running attempt: %v", err)
	}
	_, err = r.pool.Exec(ctx, "ALTER TABLE execution_events ADD CHECK (type <> 'step.attempt_failed')")
	requireSuccess(t, err)
	if err := r.FailStepAttempt(ctx, id, engine.ExecutionError{Kind: engine.ErrorRetryable, Message: "failure"}); err == nil {
		t.Fatal("expected event insertion failure")
	}
	attempts, err := r.ListStepAttempts(ctx, step)
	requireSuccess(t, err)
	if len(attempts) != 1 || attempts[0].Status != engine.StepAttemptRunning || attempts[0].CompletedAt != nil || attempts[0].Error != nil {
		t.Fatalf("attempt changed despite event failure: %+v", attempts)
	}
	requireSuccess(t, r.CancelStepAttempt(ctx, id))
	requireSuccess(t, r.CancelStepAttempt(ctx, id))
	if err := r.CompleteStepAttempt(ctx, id); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("complete cancelled attempt: %v", err)
	}
	attempts, err = r.ListStepAttempts(ctx, step)
	requireSuccess(t, err)
	if len(attempts) != 1 || attempts[0].Status != engine.StepAttemptCancelled || attempts[0].CompletedAt == nil {
		t.Fatalf("unexpected cancelled attempt: %+v", attempts)
	}
	events, err := r.ListEvents(ctx, parent)
	requireSuccess(t, err)
	if len(events) != 4 || events[3].Type != engine.EventStepAttemptStarted ||
		events[3].StepExecutionID != step || events[3].StepAttemptID != id ||
		events[0].StepExecutionID != 0 || events[0].StepAttemptID != 0 {
		t.Fatalf("unexpected event history: %+v", events)
	}
}

func TestAttemptCreationRollback(t *testing.T) {
	r := migratedRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parent, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	requireSuccess(t, r.StartExecution(ctx, parent))
	step, err := r.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: parent, StepID: "step"})
	requireSuccess(t, err)
	requireSuccess(t, r.StartStep(ctx, step))
	_, err = r.pool.Exec(ctx, "ALTER TABLE execution_events ADD CHECK (type <> 'step.attempt_started')")
	requireSuccess(t, err)
	id, err := r.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: step, Attempt: 1})
	if err == nil || id != 0 {
		t.Fatalf("expected failed creation: id=%d err=%v", id, err)
	}
	attempts, err := r.ListStepAttempts(ctx, step)
	requireSuccess(t, err)
	if len(attempts) != 0 {
		t.Fatalf("attempt persisted despite event failure: %+v", attempts)
	}
	assertExecutionEvents(t, r, parent, "execution.created", "execution.started", "step.started")
}
