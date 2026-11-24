package memory

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"

	"github.com/jyork/pandectrun/internal/engine"
)

func TestMemoryRepositoryExecutionLifecycleAndEvents(t *testing.T) {
	ctx := context.Background()
	repo := NewExecutionRepository()
	id, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 2, Input: json.RawMessage(`{"x":1}`)})
	if err != nil { t.Fatal(err) }
	if err := repo.StartExecution(ctx, id); err != nil { t.Fatal(err) }
	if err := repo.CompleteExecution(ctx, id, json.RawMessage(`{"ok":true}`)); err != nil { t.Fatal(err) }

	execution, err := repo.GetExecution(ctx, id)
	if err != nil { t.Fatal(err) }
	if execution.Status != engine.ExecutionCompleted { t.Fatalf("status = %q", execution.Status) }
	if execution.StartedAt == nil || execution.CompletedAt == nil { t.Fatal("lifecycle timestamps were not persisted") }

	events, err := repo.ListEvents(ctx, id)
	if err != nil { t.Fatal(err) }
	want := []engine.ExecutionEventType{engine.EventExecutionCreated, engine.EventExecutionStarted, engine.EventExecutionCompleted}
	if len(events) != len(want) { t.Fatalf("events = %d, want %d", len(events), len(want)) }
	for i := range want { if events[i].Type != want[i] { t.Fatalf("event[%d] = %q, want %q", i, events[i].Type, want[i]) } }

	if err := repo.FailExecution(ctx, id, engine.ExecutionError{Kind: engine.ErrorTerminal, Message: "late"}); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("late failure error = %v, want ErrInvalidTransition", err)
	}
}

func TestMemoryRepositoryCancellationIsIdempotent(t *testing.T) {
	ctx := context.Background(); repo := NewExecutionRepository()
	id, _ := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID:"wf", WorkflowVersion:1})
	if err := repo.StartExecution(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.RequestCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.RequestCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.CompleteCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.CompleteCancellation(ctx,id); err != nil { t.Fatal(err) }
	e, _ := repo.GetExecution(ctx,id); if e.Status != engine.ExecutionCancelled { t.Fatalf("status = %q",e.Status) }
}

func TestMemoryRepositoryListExecutionsFiltersAndLimits(t *testing.T) {
	ctx := context.Background(); repo := NewExecutionRepository()
	one,_ := repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"one",WorkflowVersion:1}); _ = repo.StartExecution(ctx,one)
	two,_ := repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"two",WorkflowVersion:1}); _ = repo.StartExecution(ctx,two); _ = repo.FailExecution(ctx,two,engine.ExecutionError{Kind:engine.ErrorTerminal,Message:"boom"})
	three,_ := repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"one",WorkflowVersion:2}); _ = repo.StartExecution(ctx,three)

	running,err := repo.ListExecutions(ctx,engine.ExecutionQuery{Status:[]engine.ExecutionStatus{engine.ExecutionRunning}}); if err != nil { t.Fatal(err) }
	if len(running)!=2 { t.Fatalf("running executions = %d, want 2",len(running)) }
	workflowOne,err := repo.ListExecutions(ctx,engine.ExecutionQuery{WorkflowID:"one",Limit:1}); if err != nil { t.Fatal(err) }
	if len(workflowOne)!=1 || workflowOne[0].WorkflowID!="one" { t.Fatalf("workflow one query = %+v",workflowOne) }
}

func TestMemoryRepositoryCompletedStepOutputIsImmutable(t *testing.T) {
	ctx:=context.Background(); repo:=NewExecutionRepository(); executionID,_:=repo.CreateExecution(ctx,engine.NewExecution{WorkflowID:"wf",WorkflowVersion:1}); _=repo.StartExecution(ctx,executionID)
	stepID,_:=repo.CreateStepExecution(ctx,engine.NewStepExecution{ExecutionID:executionID,StepID:"s"}); _=repo.StartStep(ctx,stepID)
	attemptID,_:=repo.CreateStepAttempt(ctx,engine.NewStepAttempt{StepExecutionID:stepID,Attempt:1}); _=repo.CompleteStepAttempt(ctx,attemptID)
	output:=json.RawMessage(`{"value":1}`); if err:=repo.CompleteStep(ctx,stepID,output); err!=nil { t.Fatal(err) }
	output[10]='9'
	step,_:=repo.GetStepExecution(ctx,stepID); if string(step.Output)!=`{"value":1}` { t.Fatalf("stored output = %s",step.Output) }
	if err:=repo.CompleteStep(ctx,stepID,json.RawMessage(`{"value":2}`)); !errors.Is(err,engine.ErrInvalidTransition) { t.Fatalf("second completion error = %v",err) }
}

func TestMemoryRepositoryIDTypes(t *testing.T) {
	ctx := context.Background()
	repo := NewExecutionRepository()

	executionID, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	if err != nil {
		t.Fatal(err)
	}

	uuidV4 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidV4.MatchString(string(executionID)) {
		t.Fatalf("execution ID %q is not a version 4 UUID", executionID)
	}

	secondExecutionID, err := repo.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if executionID == secondExecutionID {
		t.Fatal("execution IDs must be unique")
	}

	stepID, err := repo.CreateStepExecution(ctx, engine.NewStepExecution{ExecutionID: executionID, StepID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if stepID != 1 {
		t.Fatalf("first step ID = %d, want 1", stepID)
	}

	if err := repo.StartExecution(ctx, executionID); err != nil {
		t.Fatal(err)
	}
	if err := repo.StartStep(ctx, stepID); err != nil {
		t.Fatal(err)
	}

	attemptID, err := repo.CreateStepAttempt(ctx, engine.NewStepAttempt{StepExecutionID: stepID, Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if attemptID != 1 {
		t.Fatalf("first attempt ID = %d, want 1", attemptID)
	}

	events, err := repo.ListEvents(ctx, executionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[len(events)-1].StepAttemptID != attemptID {
		t.Fatalf("last event does not reference attempt %d", attemptID)
	}
}
