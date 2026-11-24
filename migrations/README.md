# PostgreSQL schema and migrations

This directory contains ordered SQL migrations for the execution repository.

## Local development with Docker Compose

From the repository root, with Docker Desktop or Docker Engine running:

```bash
docker compose up -d --wait
```

The PostgreSQL 18 service listens on localhost port 5432 and stores data in a
persistent Compose-managed volume. The development-only connection URL is:

```text
postgresql://pandectrun:pandectrun_dev@localhost:5432/pandectrun?sslmode=disable
```

If a standalone PostgreSQL container is already using port 5432, stop it first
(e.g. `docker stop pandectrun-postgres`). Compose creates a separate volume;
it does not import the standalone container's data.

Apply the schema once and run the smoke test without installing psql locally:

```bash
docker compose exec -T postgres psql -U pandectrun -d pandectrun -v ON_ERROR_STOP=1 -1 < migrations/000001_execution_schema.up.sql
docker compose exec -T postgres psql -U pandectrun -d pandectrun -v ON_ERROR_STOP=1 < migrations/test_execution_schema.sql
```

Stop and remove the service with `docker compose down`. The database volume
is retained. To permanently delete the Compose-managed development database,
use `docker compose down -v`.

These credentials and settings are for local development only. Migrations
are intentionally applied explicitly rather than automatically on startup.

## Applying migrations

Version 1 is a single transactional migration. From the repository root:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -1 -f migrations/000001_execution_schema.up.sql
```

For a disposable development database, the rollback is:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -1 -f migrations/000001_execution_schema.down.sql
```

The SQL is deliberately plain PostgreSQL, without a migration library. The
operator must apply each version exactly once, in order, and record the applied
version in deployment tooling. Do not run the down migration against production
data. Automated migration version tracking can be added when deployment is built.

## Contract and crash recovery

The tables mirror `Execution`, `StepExecution`, `StepAttempt`, and
`ExecutionEvent`. Workflow definitions and execution relations are deferred.
The `execution_events.id` identity provides a stable ordering key for event
retrieval; `created_at` alone does not guarantee a total order.

The database constrains status vocabularies, basic terminal timestamp consistency,
attempt ordinals, uniqueness, and parent references. Legal lifecycle transitions,
immutability of completed step outputs, and state/event atomicity remain the
responsibility of the future PostgreSQL repository implementation. Each lifecycle
transition and its event insertion must share a database transaction, using
conditional updates or row locks to reject concurrent invalid transitions.
The event foreign keys do not independently guarantee that a referenced step or
attempt belongs to the same execution; the repository must preserve that invariant.

**Interrupted work is not automatically retried or resumed.** If the process
crashes while an execution or attempt is running, persisted records remain in
their last observed state. On restart, a separate recovery/inspection operation
may identify these records as *potentially interrupted*, using a deployment
restart boundary or future ownership/heartbeat metadata. It must not silently
replay external side effects or rewrite history. There is intentionally no
`interrupted` lifecycle status in this migration: such a status would require
a coordinated lifecycle and event contract, not merely a database column.

The v1 schema stores JSON as `jsonb`. SQL NULL represents an absent value;
the future repository must explicitly handle JSON null versus SQL NULL.
Execution IDs use PostgreSQL `UUID`, corresponding to Go `ExecutionID string`.
Step and attempt primary keys use `BIGINT GENERATED ALWAYS AS IDENTITY`,
corresponding to distinct Go `int64` ID types. The repository must return
generated integer IDs using `RETURNING id`.

## Verification

Apply the up migration in a fresh PostgreSQL database, run the smoke-test SQL
in `migrations/test_execution_schema.sql` in a rollback-only transaction, and
verify the down migration in a disposable database.
