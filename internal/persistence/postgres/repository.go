// Package postgres implements PostgreSQL-backed execution persistence.
package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jyork/pandectrun/internal/engine"
)

// ExecutionRepository holds the connection pool used for execution persistence.
// The caller owns the pool and is responsible for closing it.
type ExecutionRepository struct {
	pool *pgxpool.Pool
}

// Verify at compile time that the PostgreSQL repository implements the engine interface.
var _ engine.ExecutionRepository = (*ExecutionRepository)(nil)

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
