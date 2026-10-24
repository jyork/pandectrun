# PostgreSQL schema and migrations

This directory contains ordered SQL migrations for the execution repository.

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
Repository-generated IDs remain `TEXT` to match the existing Go types;
a future PostgreSQL implementation may generate UUID strings internally.

## Verification

Apply the up migration in a fresh PostgreSQL database, run the smoke-test SQL
in `migrations/test_execution_schema.sql` in a rollback-only transaction, and
verify the down migration in a disposable database.
