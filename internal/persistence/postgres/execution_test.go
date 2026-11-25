package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jyork/pandectrun/internal/engine"
)

// migratedRepository creates an isolated schema in the configured test database.
// Cleanup drops only the schema generated for this test.
func migratedRepository(t *testing.T) *ExecutionRepository {
	t.Helper()
	dsn := os.Getenv("PANDECTRUN_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("PANDECTRUN_POSTGRES_TEST_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	requireSuccess(t, err)
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{"test_" + uuid.NewV4().String()}.Sanitize()
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	requireSuccess(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	requireSuccess(t, err)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 8
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	requireSuccess(t, err)
	t.Cleanup(pool.Close)
	migration, err := os.ReadFile("../../../migrations/postgres/000001_execution_schema.up.sql")
	requireSuccess(t, err)
	_, err = pool.Exec(ctx, string(migration), pgx.QueryExecModeSimpleProtocol)
	requireSuccess(t, err)
	return NewExecutionRepository(pool)
}

func requireSuccess(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecutionPersistence(t *testing.T) {
	r := migratedRepository(t)
	ctx := context.Background()
	id, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 2, Input: json.RawMessage("null")})
	requireSuccess(t, err)
	e, err := r.GetExecution(ctx, id)
	requireSuccess(t, err)
	if e.ID != id || e.WorkflowID != "wf" || e.WorkflowVersion != 2 || e.Status != engine.ExecutionPending ||
		string(e.Input) != "null" || e.Output != nil || e.Error != nil || e.CreatedAt.IsZero() || e.StartedAt != nil || e.CompletedAt != nil {
		t.Fatalf("unexpected pending execution: %+v", e)
	}
	requireSuccess(t, r.StartExecution(ctx, id))
	requireSuccess(t, r.RequestCancellation(ctx, id))
	requireSuccess(t, r.RequestCancellation(ctx, id))
	requireSuccess(t, r.CompleteExecution(ctx, id, json.RawMessage("null")))
	e, err = r.GetExecution(ctx, id)
	requireSuccess(t, err)
	if e.Status != engine.ExecutionCompleted || string(e.Output) != "null" || e.StartedAt == nil || e.CompletedAt == nil ||
		e.StartedAt.Before(e.CreatedAt) || e.CompletedAt.Before(*e.StartedAt) {
		t.Fatalf("unexpected completed execution: %+v", e)
	}
	if err := r.FailExecution(ctx, id, engine.ExecutionError{Kind: engine.ErrorTerminal}); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("late failure: %v", err)
	}
	assertExecutionEvents(t, r, id, "execution.created", "execution.started", "execution.cancel_requested", "execution.completed")

	failed, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	requireSuccess(t, r.StartExecution(ctx, failed))
	failure := engine.ExecutionError{Kind: engine.ErrorTerminal, Code: "test", Message: "failure"}
	requireSuccess(t, r.FailExecution(ctx, failed, failure))
	e, err = r.GetExecution(ctx, failed)
	requireSuccess(t, err)
	if e.Status != engine.ExecutionFailed || e.Error == nil || *e.Error != failure || e.Input != nil || e.Output != nil {
		t.Fatalf("unexpected failed execution: %+v", e)
	}
	cancelled, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "other", WorkflowVersion: 1})
	requireSuccess(t, err)
	requireSuccess(t, r.RequestCancellation(ctx, cancelled))
	requireSuccess(t, r.CompleteCancellation(ctx, cancelled))
	requireSuccess(t, r.CompleteCancellation(ctx, cancelled))
	assertExecutionEvents(t, r, cancelled, "execution.created", "execution.cancel_requested", "execution.cancelled")

	list, err := r.ListExecutions(ctx, engine.ExecutionQuery{WorkflowID: "wf", Status: []engine.ExecutionStatus{engine.ExecutionFailed, engine.ExecutionCompleted}})
	requireSuccess(t, err)
	if len(list) != 2 || list[0].ID != id || list[1].ID != failed {
		t.Fatalf("unexpected filtered list: %+v", list)
	}
	list, err = r.ListExecutions(ctx, engine.ExecutionQuery{Limit: 1})
	requireSuccess(t, err)
	if len(list) != 1 || list[0].ID != id {
		t.Fatalf("unexpected limited list: %+v", list)
	}
	missing := engine.ExecutionID(uuid.NewV4().String())
	if _, err := r.GetExecution(ctx, missing); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing execution: %v", err)
	}
	if err := r.StartExecution(ctx, missing); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("missing transition: %v", err)
	}
}

func assertExecutionEvents(t *testing.T, r *ExecutionRepository, id engine.ExecutionID, want ...string) {
	t.Helper()
	rows, err := r.pool.Query(context.Background(), "SELECT type FROM execution_events WHERE execution_id = $1::text::uuid ORDER BY id", string(id))
	requireSuccess(t, err)
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	requireSuccess(t, err)
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestExecutionConcurrentStart(t *testing.T) {
	r := migratedRepository(t)
	ctx := context.Background()
	id, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- r.StartExecution(ctx, id)
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
			t.Errorf("unexpected error: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successful starts = %d, want 1", success)
	}
	assertExecutionEvents(t, r, id, "execution.created", "execution.started")
}

func TestExecutionEventFailureRollsBackState(t *testing.T) {
	r := migratedRepository(t)
	ctx := context.Background()
	id, err := r.CreateExecution(ctx, engine.NewExecution{WorkflowID: "wf", WorkflowVersion: 1})
	requireSuccess(t, err)
	_, err = r.pool.Exec(ctx, "ALTER TABLE execution_events ADD CHECK (type <> 'execution.started')")
	requireSuccess(t, err)
	if err := r.StartExecution(ctx, id); err == nil {
		t.Fatal("expected event insertion failure")
	}
	e, err := r.GetExecution(ctx, id)
	requireSuccess(t, err)
	if e.Status != engine.ExecutionPending || e.StartedAt != nil {
		t.Fatalf("state changed despite event failure: %+v", e)
	}
	assertExecutionEvents(t, r, id, "execution.created")
}
