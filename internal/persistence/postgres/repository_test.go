package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testRepository connects to an explicitly configured test database.
// The tests use session-local temporary tables and never modify the
// application's execution tables.
func testRepository(t *testing.T) *ExecutionRepository {
	t.Helper()
	dsn := os.Getenv("PANDECTRUN_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("PANDECTRUN_POSTGRES_TEST_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return NewExecutionRepository(pool)
}

func TestTransactionCommitAndRollback(t *testing.T) {
	repo := testRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// A single-connection test pool keeps the temporary table visible.
	pool := repo.pool

	if _, err := pool.Exec(ctx, "CREATE TEMP TABLE pandectrun_tx_test (value integer NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := repo.withTransaction(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO pandectrun_tx_test VALUES (1)")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("rollback requested")
	err := repo.withTransaction(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO pandectrun_tx_test VALUES (2)"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("transaction error = %v, want %v", err, sentinel)
	}

	var count, sum int
	if err := pool.QueryRow(ctx, "SELECT count(*), sum(value) FROM pandectrun_tx_test").Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sum != 1 {
		t.Fatalf("count = %d, sum = %d; want 1, 1", count, sum)
	}
}
