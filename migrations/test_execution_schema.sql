-- Run after the up migration. ROLLBACK leaves no test data behind.
BEGIN;

INSERT INTO executions (id, workflow_id, workflow_version, status)
VALUES ('00000000-0000-4000-8000-000000000001', 'smoke-workflow', 1, 'pending');

WITH new_step AS (
    INSERT INTO step_executions (execution_id, step_id)
    VALUES ('00000000-0000-4000-8000-000000000001', 'validate')
    RETURNING id
), new_attempt AS (
    INSERT INTO step_attempts (step_execution_id, attempt)
    SELECT id, 1 FROM new_step
    RETURNING id, step_execution_id
)
INSERT INTO execution_events (execution_id, type, step_execution_id, step_attempt_id)
SELECT '00000000-0000-4000-8000-000000000001', 'step.attempt_started',
       step_execution_id, id
FROM new_attempt;

DO $$
BEGIN
    IF (SELECT count(*) FROM execution_events WHERE execution_id = '00000000-0000-4000-8000-000000000001') <> 1 THEN
        RAISE EXCEPTION 'execution event insert failed';
    END IF;
END
$$;

ROLLBACK;
