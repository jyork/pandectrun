# Execution Lifecycle Diagrams

This document provides Mermaid diagrams for the execution-persistence design in [`execution-persistence.md`](execution-persistence.md). The diagrams are intended to make lifecycle transitions, retries, cancellation races, and execution relationships easier to review independently of the repository API details.

## Execution lifecycle

Cancellation is best-effort. `cancel_requested` records intent, while the terminal state records the actual outcome.

```mermaid
stateDiagram-v2
    [*] --> pending

    pending --> running: execution starts
    pending --> cancel_requested: cancellation requested

    running --> completed: workflow completes
    running --> failed: terminal failure
    running --> cancel_requested: cancellation requested

    cancel_requested --> cancelled: cancellation takes effect
    cancel_requested --> completed: work completes first
    cancel_requested --> failed: independent terminal failure

    completed --> [*]
    failed --> [*]
    cancelled --> [*]
```

The three transitions out of `cancel_requested` are deliberate. A cancellation request does not guarantee that cancellation wins a race with successful completion or an independent terminal failure.

## Step lifecycle

A logical step may have multiple attempts. Attempt failures are not terminal step failures while retry remains possible.

```mermaid
stateDiagram-v2
    [*] --> pending

    pending --> running: step starts
    pending --> cancelled: execution cancelled
    pending --> skipped: workflow does not require step

    running --> completed: attempt succeeds
    running --> running: retryable attempt failure / retry
    running --> failed: terminal failure or retries exhausted
    running --> cancelled: cancellation takes effect

    completed --> [*]
    failed --> [*]
    skipped --> [*]
    cancelled --> [*]
```

`skipped` and `cancelled` are intentionally distinct. A skipped step is omitted because of workflow semantics; a cancelled step would otherwise have run or completed but execution was terminated.

## Retry sequence

Retries belong to a `StepExecution`. They do not create new workflow executions.

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant R as ExecutionRepository
    participant I as Step Implementation

    S->>R: StartStep(step)
    S->>R: CreateStepAttempt(attempt 1)
    S->>I: Execute(ctx, input)
    I-->>S: retryable error
    S->>R: FailStepAttempt(attempt 1, error)
    Note over S,R: step.attempt_failed
    Note over S,R: step.retry_scheduled

    S->>R: CreateStepAttempt(attempt 2)
    S->>I: Execute(ctx, input)
    I-->>S: retryable error
    S->>R: FailStepAttempt(attempt 2, error)
    Note over S,R: step.attempt_failed
    Note over S,R: step.retry_scheduled

    S->>R: CreateStepAttempt(attempt 3)
    S->>I: Execute(ctx, input)
    I-->>S: success + output
    S->>R: CompleteStepAttempt(attempt 3)
    S->>R: CompleteStep(step, output)
    Note over S,R: StepExecution is completed
```

The repository operations shown here are conceptual. The implementation may combine operations where doing so is necessary to preserve atomic state and event semantics.

## Cancellation sequence

A cancellation request is persisted before the scheduler attempts to stop active work.

```mermaid
sequenceDiagram
    actor C as API / UI / Agent
    participant R as ExecutionRepository
    participant S as Scheduler
    participant I as Running Step

    C->>R: RequestCancellation(execution)
    Note over R: status = cancel_requested
    Note over R: append execution.cancel_requested

    S->>R: observe cancel_requested
    S->>S: cancel execution context
    S-->>I: context cancellation

    alt cancellation takes effect
        I-->>S: context.Canceled
        S->>R: CancelStep(step)
        S->>R: CompleteCancellation(execution)
        Note over R: execution.cancelled
    else work completes first
        I-->>S: success
        S->>R: CompleteStep(step, output)
        S->>R: CompleteExecution(execution, output)
        Note over R: execution.completed
    else independent terminal failure wins
        I-->>S: terminal error
        S->>R: FailStep(step, error)
        S->>R: FailExecution(execution, error)
        Note over R: execution.failed
    end
```

The scheduler must distinguish cancellation-caused `context.Canceled` from an independent failure so cancellation does not incorrectly trigger retry/failure behavior.

## Execution relationships

Retrying an entire workflow or selecting a different workflow creates a new execution. Existing execution history is not reset or rewritten.

```mermaid
flowchart LR
    A[Execution A<br/>incident-analysis v3<br/>failed]
    B[Execution B<br/>incident-analysis v3]
    C[Execution C<br/>deep-diagnostics v2]
    D[Execution D<br/>child workflow]

    A -->|retry| B
    A -->|fallback / agent decision| C
    C -->|child workflow invocation| D
```

The relationship between executions and the trigger that initiated an execution are separate concepts. For example, Execution C may be related to Execution A as a fallback while its trigger identifies an agent as the initiator.

## Persistence boundary

The scheduler depends on repository semantics, not a particular storage technology.

```mermaid
flowchart TB
    API[API / UI / Agent]
    S[Scheduler]
    REG[Step Registry]
    REPO[ExecutionRepository]
    MEM[MemoryExecutionRepository]
    PG[PostgreSQL Repository<br/>later]
    STEP[Step Implementation]

    API --> S
    API -->|cancellation request| REPO
    S --> REG
    REG --> STEP
    S --> REPO
    S --> STEP

    REPO -. implemented by .-> MEM
    REPO -. implemented by .-> PG
```

The in-memory repository is the first implementation. It should enforce the same lifecycle, immutability, event-ordering, and atomic-operation semantics expected from a later PostgreSQL implementation.

## Atomic state and event transition

State and its corresponding audit event are one logical repository operation.

```mermaid
flowchart LR
    S[Scheduler]
    OP[CompleteStep]

    subgraph TX[Atomic repository operation]
        V[Validate transition]
        ST[Set status = completed]
        OUT[Persist immutable output]
        EVT[Append step.completed]
        V --> ST --> OUT --> EVT
    end

    S --> OP --> V
```

For the in-memory repository, the atomic boundary can be protected by one mutex. A PostgreSQL implementation can later provide the same semantic boundary with a transaction.
