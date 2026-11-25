// Package postgres implements PostgreSQL-backed execution persistence.
package postgres

import "github.com/jackc/pgx/v5/pgxpool"

// ExecutionRepository holds the connection pool used for execution persistence.
// The caller owns the pool and is responsible for closing it.
// Lifecycle operations will be added before this type implements the engine contract.
type ExecutionRepository struct {
	pool *pgxpool.Pool
}

// NewExecutionRepository constructs a PostgreSQL repository.
//
// Parameters:
//   - pool: an initialized, caller-owned connection pool.
//
// Returns:
//   - *ExecutionRepository: the repository using pool.
func NewExecutionRepository(pool *pgxpool.Pool) *ExecutionRepository {
	return &ExecutionRepository{pool: pool}
}
