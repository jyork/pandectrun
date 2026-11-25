# PostgreSQL persistence

This package is the foundation for the PostgreSQL execution repository. It
does not yet implement the engine's complete repository interface. The caller
owns the pgx connection pool. Transactions use READ COMMITTED isolation and
do not automatically retry commit failures.

## Integration tests

With the development Compose service running, create a separate test database
once from the repository root:

```bash
docker compose exec -T postgres createdb -U pandectrun pandectrun_test
```

Run the integration tests:

```bash
PANDECTRUN_POSTGRES_TEST_DSN='postgresql://pandectrun:pandectrun_dev@localhost:5432/pandectrun_test?sslmode=disable' go test -v ./internal/persistence/postgres/...
```

Without the environment variable, database integration tests skip. CI provides
a dedicated PostgreSQL 18 service and sets the variable. The foundation tests
use temporary tables; no migration is required yet. Future repository contract
tests will require isolated migrated databases.
