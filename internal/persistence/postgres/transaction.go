package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// withTransaction executes fn in a read-committed transaction. pgx.BeginTxFunc
// rolls back on callback failure or panic and commits only on success.
// Commit errors are returned without retrying an uncertain outcome.
//
// Parameters:
//   - ctx: context for transaction operations.
//   - fn: callback performing all related state and event writes.
//
// Returns:
//   - error: the callback, transaction, or commit error, if any.
func (r *ExecutionRepository) withTransaction(ctx context.Context, fn func(pgx.Tx) error) error {
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
	if err != nil {
		return fmt.Errorf("postgres transaction: %w", err)
	}
	return nil
}
