package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestMemoryRepositoryExecutionLifecycleAndEvents(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryExecutionRepository()
	id, err := repo.CreateExecution(ctx, NewExecution{WorkflowID: "wf", WorkflowVersion: 2, Input: json.RawMessage(`{"x":1}`)})
	if err != nil { t.Fatal(err) }
	if err := repo.StartExecution(ctx, id); err != nil { t.Fatal(err) }
	if err := repo.CompleteExecution(ctx, id, json.RawMessage(`{"ok":true}`)); err != nil { t.Fatal(err) }

	execution, err := repo.GetExecution(ctx, id)
	if err != nil { t.Fatal(err) }
	if execution.Status != ExecutionCompleted { t.Fatalf("status = %q", execution.Status) }
	if execution.StartedAt == nil || execution.CompletedAt == nil { t.Fatal("lifecycle timestamps were not persisted") }

	events, err := repo.ListEvents(ctx, id)
	if err != nil { t.Fatal(err) }
	want := []ExecutionEventType{EventExecutionCreated, EventExecutionStarted, EventExecutionCompleted}
	if len(events) != len(want) { t.Fatalf("events = %d, want %d", len(events), len(want)) }
	for i := range want { if events[i].Type != want[i] { t.Fatalf("event[%d] = %q, want %q", i, events[i].Type, want[i]) } }

	if err := repo.FailExecution(ctx, id, ExecutionError{Kind: ErrorTerminal, Message: "late"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("late failure error = %v, want ErrInvalidTransition", err)
	}
}

func TestMemoryRepositoryCancellationIsIdempotent(t *testing.T) {
	ctx := context.Background(); repo := NewMemoryExecutionRepository()
	id, _ := repo.CreateExecution(ctx, NewExecution{WorkflowID:"wf", WorkflowVersion:1})
	if err := repo.StartExecution(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.RequestCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.RequestCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.CompleteCancellation(ctx,id); err != nil { t.Fatal(err) }
	if err := repo.CompleteCancellation(ctx,id); err != nil { t.Fatal(err) }
	e, _ := repo.GetExecution(ctx,id); if e.Status != ExecutionCancelled { t.Fatalf("status = %q",e.Status) }
}

func TestMemoryRepositoryListExecutionsFiltersAndLimits(t *testing.T) {
	ctx := context.Background(); repo := NewMemoryExecutionRepository()
	one,_ := repo.CreateExecution(ctx,NewExecution{WorkflowID:"one",WorkflowVersion:1}); _ = repo.StartExecution(ctx,one)
	two,_ := repo.CreateExecution(ctx,NewExecution{WorkflowID:"two",WorkflowVersion:1}); _ = repo.StartExecution(ctx,two); _ = repo.FailExecution(ctx,two,ExecutionError{Kind:ErrorTerminal,Message:"boom"})
	three,_ := repo.CreateExecution(ctx,NewExecution{WorkflowID:"one",WorkflowVersion:2}); _ = repo.StartExecution(ctx,three)

	running,err := repo.ListExecutions(ctx,ExecutionQuery{Status:[]ExecutionStatus{ExecutionRunning}}); if err != nil { t.Fatal(err) }
	if len(running)!=2 { t.Fatalf("running executions = %d, want 2",len(running)) }
	workflowOne,err := repo.ListExecutions(ctx,ExecutionQuery{WorkflowID:"one",Limit:1}); if err != nil { t.Fatal(err) }
	if len(workflowOne)!=1 || workflowOne[0].WorkflowID!="one" { t.Fatalf("workflow one query = %+v",workflowOne) }
}

func TestMemoryRepositoryCompletedStepOutputIsImmutable(t *testing.T) {
	ctx:=context.Background(); repo:=NewMemoryExecutionRepository(); executionID,_:=repo.CreateExecution(ctx,NewExecution{WorkflowID:"wf",WorkflowVersion:1}); _=repo.StartExecution(ctx,executionID)
	stepID,_:=repo.CreateStepExecution(ctx,NewStepExecution{ExecutionID:executionID,StepID:"s"}); _=repo.StartStep(ctx,stepID)
	attemptID,_:=repo.CreateStepAttempt(ctx,NewStepAttempt{StepExecutionID:stepID,Attempt:1}); _=repo.CompleteStepAttempt(ctx,attemptID)
	output:=json.RawMessage(`{"value":1}`); if err:=repo.CompleteStep(ctx,stepID,output); err!=nil { t.Fatal(err) }
	output[10]='9'
	step,_:=repo.GetStepExecution(ctx,stepID); if string(step.Output)!=`{"value":1}` { t.Fatalf("stored output = %s",step.Output) }
	if err:=repo.CompleteStep(ctx,stepID,json.RawMessage(`{"value":2}`)); !errors.Is(err,ErrInvalidTransition) { t.Fatalf("second completion error = %v",err) }
}
