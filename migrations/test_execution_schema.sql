-- Run after the up migration. ROLLBACK leaves no test data behind.
BEGIN;

INSERT INTO executions (id, workflow_id, workflow_version, status)
VALUES ('smoke-exec', 'smoke-workflow', 1, 'pending');

INSERT INTO step_executions (id, execution_id, step_id)
VALUES ('smoke-step', 'smoke-exec', 'validate');

INSERT INTO step_attempts (id, step_execution_id, attempt)
VALUES ('smoke-attempt', 'smoke-step', 1);

INSERT INTO execution_events (execution_id, type, step_execution_id, step_attempt_id)
VALUES ('smoke-exec', 'step.attempt_started', 'smoke-step', 'smoke-attempt');

DO $$
BEGIN
    IF (SELECT count(*) FROM execution_events WHERE execution_id = 'smoke-exec') <> 1 THEN
        RAISE EXCEPTION 'execution event insert failed';
    END IF;
END
$$;

ROLLBACK;
