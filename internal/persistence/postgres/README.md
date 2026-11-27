# PostgreSQL persistence

This package implements the engine's complete execution repository interface. The caller
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
a dedicated PostgreSQL 18 service and sets the variable. Foundation tests use temporary tables. Execution tests create a unique schema,
apply the checked-in migration, and drop only that schema on cleanup. The test
role must have CREATE permission on the test database. No manual migration
is required. Do not point the test DSN at a production database.

Execution creation, retrieval, filtering and lifecycle transitions are now
implemented. State transitions lock the execution row before validation and
write their events in the same transaction. SQL NULL and JSON null remain
distinct.

Step execution operations are implemented. Mutations acquire the parent execution
lock before the step lock, serializing event writes per execution. The existing
schema enforces unique logical step IDs within an execution; duplicate inserts
return engine.ErrAlreadyExists in both adapters.

Attempt mutations acquire locks in execution, step, attempt order. Attempt
creation and failure write their events atomically; completion and cancellation
have no separate attempt event, matching the memory implementation. Retry
scheduling records an event for a failed attempt. Event retrieval orders by
identity ID and maps absent child references to zero values.

The shared repository contract suite runs against isolated migrated schemas.
Both adapters enforce positive signed 32-bit workflow versions and attempt
numbers, and unique attempt numbers per step. Invalid numbers return
engine.ErrInvalidInput; duplicate logical keys return engine.ErrAlreadyExists.

Integration tests exercise the scheduler with PostgreSQL for retry followed by
success, terminal failure, and persisted cancellation requests observed between
steps. The successful workflow test replaces the connection pool and repository
before reading execution state, step outputs, attempts, and events. This verifies
persistence across client reconnection, not database restart or automatic crash
recovery. Application configuration remains separate work.
