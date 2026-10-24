-- PandectRun execution persistence schema, version 1.
-- Runtime identifiers are repository-owned strings, matching the Go ID types.
-- Workflow definitions remain outside this migration's scope.

CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    workflow_version INTEGER NOT NULL CHECK (workflow_version > 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'cancel_requested', 'completed', 'failed', 'cancelled')),
    input JSONB,
    output JSONB,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT executions_terminal_time CHECK (
        (status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL)
        OR (status NOT IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL)
    ),
    CONSTRAINT executions_error_shape CHECK (
        error IS NULL OR (jsonb_typeof(error) = 'object' AND error ? 'kind' AND error ? 'message')
    )
);

CREATE TABLE step_executions (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE RESTRICT,
    step_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped', 'cancelled')),
    output JSONB,
    error JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    UNIQUE (execution_id, step_id),
    CONSTRAINT steps_terminal_time CHECK (
        (status IN ('completed', 'failed', 'skipped', 'cancelled') AND completed_at IS NOT NULL)
        OR (status NOT IN ('completed', 'failed', 'skipped', 'cancelled') AND completed_at IS NULL)
    ),
    CONSTRAINT steps_error_shape CHECK (
        error IS NULL OR (jsonb_typeof(error) = 'object' AND error ? 'kind' AND error ? 'message')
    )
);

CREATE TABLE step_attempts (
    id TEXT PRIMARY KEY,
    step_execution_id TEXT NOT NULL REFERENCES step_executions(id) ON DELETE RESTRICT,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    error JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (step_execution_id, attempt),
    CONSTRAINT attempts_terminal_time CHECK (
        (status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL)
        OR (status = 'running' AND completed_at IS NULL)
    ),
    CONSTRAINT attempts_error_shape CHECK (
        error IS NULL OR (jsonb_typeof(error) = 'object' AND error ? 'kind' AND error ? 'message')
    )
);

CREATE TABLE execution_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE RESTRICT,
    type TEXT NOT NULL CHECK (type IN (
        'execution.created', 'execution.started', 'execution.cancel_requested',
        'execution.completed', 'execution.failed', 'execution.cancelled',
        'step.started', 'step.attempt_started', 'step.attempt_failed',
        'step.retry_scheduled', 'step.completed', 'step.failed', 'step.cancelled'
    )),
    step_execution_id TEXT REFERENCES step_executions(id) ON DELETE RESTRICT,
    step_attempt_id TEXT REFERENCES step_attempts(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_attempt_requires_step CHECK (
        step_attempt_id IS NULL OR step_execution_id IS NOT NULL
    )
);

CREATE INDEX executions_workflow_created_idx
    ON executions (workflow_id, created_at, id);
CREATE INDEX executions_status_created_idx
    ON executions (status, created_at, id);
CREATE INDEX step_executions_execution_idx
    ON step_executions (execution_id, id);
CREATE INDEX step_attempts_step_attempt_idx
    ON step_attempts (step_execution_id, attempt);
CREATE INDEX execution_events_execution_order_idx
    ON execution_events (execution_id, id);

-- Application repository operations must lock/check the current state and
-- insert their corresponding event within the SAME transaction. CHECK constraints
-- do not by themselves enforce legal transitions, output immutability, or
-- the relationship between event references and their parent execution.
-- Crash recovery does not automatically replay running attempts.
