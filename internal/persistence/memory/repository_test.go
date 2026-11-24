package memory

import (
	"testing"

	"github.com/jyork/pandectrun/internal/engine"
	"github.com/jyork/pandectrun/internal/persistence/repositorytest"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.Run(t, func(t *testing.T) engine.ExecutionRepository {
		return NewExecutionRepository()
	})
}
