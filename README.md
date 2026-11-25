# PandectRun

**AI workflow orchestration for deterministic logic, APIs, and LLMs.**

[pandectrun.com](https://pandectrun.com) · [Architecture](docs/architecture.md)

PandectRun is a production-oriented workflow orchestration platform for building reliable workflows that combine deterministic application logic, external services, and AI models.

The name **PandectRun** comes from *pandect*: a comprehensive treatise or compendium covering a subject as a whole. The name reflects the project's goal of bringing the many pieces of an AI-assisted workflow—deterministic logic, APIs, models, state, reliability, and observability—together into one coherent execution system.

The goal is not to build another AI chatbot. PandectRun explores the infrastructure required to run AI-assisted workflows as dependable software: explicit execution graphs, retries, persistence, idempotency, observability, structured model output, and auditable execution state.

## Current Capabilities

PandectRun currently provides a Go execution engine and persistence layer. It
is a development-stage project, not yet a standalone workflow service.

- Directed acyclic graph (DAG) validation and sequential dependency-aware execution.
- Step registration, accumulated execution context, and structured JSON results.
- Execution, step, and attempt lifecycle management, including cancellation.
- Resile-backed retries with configurable policies and retryable versus terminal errors.
- Execution history and lifecycle events.
- Interchangeable in-memory and PostgreSQL execution repositories.
- PostgreSQL schema migrations, local Docker Compose environment, and shared repository contract tests.
- GitHub Actions testing with the Go race detector and a PostgreSQL service.

The PostgreSQL adapter implements all execution repository methods. Integration
verification of the complete adapter is in progress. Automatic crash recovery,
distributed scheduling, persistent workflow definitions, HTTP APIs, and AI
provider integrations are not yet implemented.

## Project Goals

- **Workflows, not prompts** — AI calls are individual steps inside a larger deterministic execution model.
- **Durable execution** — workflow state and intermediate results can survive process or service failures.
- **Explicit dependencies** — steps declare what must complete before they can execute.
- **Provider independence** — AI providers sit behind a common abstraction.
- **Structured results** — steps exchange schema-validated data rather than unstructured conversations.
- **Operational visibility** — executions produce logs, metrics, traces, and audit events.
- **Recoverable failure** — retries, backoff, timeouts, cancellation, and idempotency are first-class concerns.

## Reference Workflows

These are planned reference workflows and illustrative payloads.

### Incident Analysis

The primary reference workflow analyzes operational incidents using alerts, recent deployments, log samples, runbooks, deterministic heuristics, and an LLM.

A typical execution:

1. Validates the incident payload.
2. Correlates alerts with recent deployments.
3. Applies deterministic diagnostic heuristics.
4. Analyzes logs and symptoms with an LLM.
5. Retrieves an applicable runbook.
6. Generates recommended diagnostic actions.
7. Assigns confidence scores to likely causes.
8. Persists the result and execution history.

Example input:

```json
{
  "service": "checkout-api",
  "alerts": [
    "p95 latency exceeded 2 seconds",
    "error rate increased to 8%"
  ],
  "recent_deployments": [
    {
      "version": "2.14.0",
      "deployed_at": "2026-08-02T18:20:00Z"
    }
  ],
  "log_samples": [
    "database connection pool exhausted",
    "request deadline exceeded"
  ]
}
```

Example result:

```json
{
  "severity": "high",
  "likely_causes": [
    {"cause": "Database connection pool exhaustion", "confidence": 0.87},
    {"cause": "Regression introduced in version 2.14.0", "confidence": 0.66}
  ],
  "recommended_actions": [
    "Inspect active and waiting database connections",
    "Compare connection-pool configuration with the previous release",
    "Consider rolling back version 2.14.0"
  ],
  "runbook": "database-pool-exhaustion"
}
```

### Support Ticket Triage

The second reference workflow demonstrates a business-oriented orchestration use case: validation, customer lookup, sensitive-data redaction, LLM classification, deterministic priority rules, summary generation, queue selection, persistence, and event publication.

## Architecture

See the [architecture document and Mermaid diagram](docs/architecture.md).

The engine owns the execution repository interface and is independent of
database-specific implementation details. Repositories are injected into the
engine: the in-memory adapter supports development and testing, while PostgreSQL
provides durable execution state. Additional database adapters can implement the
same interface without introducing a generic SQL abstraction.

PostgreSQL lifecycle mutations use transactions and consistent parent-to-child
row locking. State changes and their associated events are persisted atomically.
Redis and Kafka remain possible future infrastructure for distributed execution.

See [execution lifecycle](docs/execution-lifecycle.md),
[execution persistence](docs/execution-persistence.md), and
[PostgreSQL persistence](internal/persistence/postgres/README.md) for details.

Workflow definitions describe reusable steps and dependencies instead of hard-coding a single application into the engine:

```json
{
  "name": "support-ticket-triage",
  "steps": [
    {"id": "validate", "type": "validation"},
    {"id": "lookup-customer", "type": "http", "depends_on": ["validate"]},
    {"id": "classify", "type": "llm", "depends_on": ["lookup-customer"]},
    {"id": "apply-priority-rules", "type": "rules", "depends_on": ["classify"]},
    {"id": "publish-result", "type": "event", "depends_on": ["apply-priority-rules"]}
  ]
}
```

Each step receives the accumulated execution context and contributes structured output to it.

## Planned Step Types

| Step | Purpose |
|---|---|
| `validation` | Validate input or intermediate state |
| `http` | Call external REST services |
| `llm` | Invoke a configured AI model |
| `rules` | Apply deterministic business or diagnostic rules |
| `event` | Publish an event to downstream consumers |

## API Direction

The following endpoints are proposed, not implemented.

```text
POST /workflows
GET  /workflows/{workflow_id}
POST /workflows/{workflow_id}/executions
GET  /executions/{execution_id}
POST /executions/{execution_id}/cancel
POST /executions/{execution_id}/retry
GET  /executions/{execution_id}/events
```

Example execution request:

```json
{
  "input": {
    "subject": "Unable to access account",
    "message": "Password reset emails are not arriving."
  }
}
```

## Technology Direction

- **Application:** Go 1.27, REST APIs, gRPC where appropriate
- **Persistence:** PostgreSQL; Redis for caching and coordination
- **Messaging:** Kafka as distributed/event-driven execution is introduced
- **AI:** provider abstraction, structured output, retries, timeouts, telemetry
- **Observability:** OpenTelemetry, Prometheus, Grafana, structured logging
- **Deployment:** Docker, Kubernetes, AWS, Terraform, GitHub Actions

PandectRun targets **Go 1.27**. Older Go versions are not supported; the project will use the current language and standard-library capabilities available at this baseline rather than carrying compatibility code for earlier releases.

Local development should remain lightweight even as the target production architecture moves toward Kubernetes.

## Dependencies

PandectRun aims to keep its dependency surface small. The standard library is preferred where it provides a clear, maintainable solution. External dependencies are added deliberately when they provide substantial production behavior that would otherwise require significant custom infrastructure code or recreate well-understood edge cases.

### Resile

[`github.com/cinar/resile`](https://github.com/cinar/resile) provides the retry and execution-resilience primitives used by PandectRun. The engine uses Resile for configured retry policies, bounded attempts, backoff, and cancellation-aware execution. Additional resilience and telemetry capabilities remain future work.

PandectRun will keep its own retry-policy abstraction at the workflow-engine boundary rather than exposing Resile-specific configuration in persisted workflow definitions. This keeps workflow semantics owned by PandectRun while allowing the underlying resilience implementation to evolve independently.

More advanced Resile features—including circuit breakers, adaptive retries, bulkheads, hedged requests, rate limiting, and fallback strategies—will be introduced only when a concrete workflow or operational requirement justifies them.

The module is pinned in `go.mod`; dependency upgrades should be intentional and reviewed like other runtime behavior changes.

### pgx

[`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx) provides the
PostgreSQL driver, connection pooling, and transaction support. The dependency
is confined to the PostgreSQL persistence adapter; the engine does not depend
on a particular database driver.

## Execution Model

A workflow execution maintains accumulated context:

```json
{
  "input": {},
  "steps": {
    "validate": {},
    "lookup-customer": {},
    "classify": {}
  }
}
```

A step reads the context it depends on, performs its operation, produces structured output, stores that output under its step ID, and emits telemetry and audit information. The engine validates the DAG and executes eligible steps sequentially. Each step execution can have multiple attempts; retry policies determine whether another attempt is scheduled after a failure. Execution, step, and attempt state are recorded separately from the workflow definition.

## Reliability

External APIs and AI providers are treated as unreliable dependencies. PandectRun owns the workflow-level semantics for retryable versus terminal failures, persisted execution state, cancellation, and execution history. Request idempotency remains a future design concern. Resile provides the underlying retry/resilience mechanics, including backoff, jitter, attempt limits, context-aware cancellation, and related execution policies.

This separation keeps dependency-specific behavior out of persisted workflow definitions and prevents the orchestrator API from becoming coupled to a particular retry library.

## Observability

Workflow executions should be traceable from the incoming API request through individual workflow steps and external dependencies. Planned telemetry includes execution and step latency, failures, retries, LLM latency and token usage, external API latency, queue depth, and execution status transitions.

## Repository Structure

```text
pandectrun/
├── .github/workflows/       # CI
├── docs/                    # Architecture and execution design
├── internal/
│   ├── engine/              # DAG, scheduler, models and repository contract
│   └── persistence/
│       ├── memory/          # In-memory repository
│       ├── postgres/        # PostgreSQL repository
│       └── repositorytest/  # Shared repository contract tests
├── migrations/postgres/     # PostgreSQL schema and smoke test
├── compose.yaml             # Local PostgreSQL development service
├── go.mod
├── go.sum
└── README.md
```

## Getting Started

Requirements: Go 1.27, Git, and Docker with Compose for PostgreSQL integration
tests. Run commands from the repository root.

Run unit tests and static analysis:

```bash
go test ./...
go vet ./...
```

Start the local PostgreSQL 18 service:

```bash
docker compose up -d --wait
```

To initialize a fresh development database, apply the migration once:

```bash
docker compose exec -T postgres psql -U pandectrun -d pandectrun -v ON_ERROR_STOP=1 -1 < migrations/postgres/000001_execution_schema.up.sql
```

Create a separate integration test database once:

```bash
docker compose exec -T postgres createdb -U pandectrun pandectrun_test
```

Run the PostgreSQL integration tests:

```bash
PANDECTRUN_POSTGRES_TEST_DSN='postgresql://pandectrun:pandectrun_dev@localhost:5432/pandectrun_test?sslmode=disable' go test -v ./internal/persistence/postgres/...
```

Integration tests create isolated schemas and apply the migration themselves.
They skip when the test DSN is absent. Never use a production database for
these tests. See the [PostgreSQL adapter README](internal/persistence/postgres/README.md)
for additional details.

## Roadmap

### Phase 1 — Execution Core

- [x] Workflow definition and DAG validation
- [x] Execution state model and step interface
- [x] Sequential workflow execution
- [x] In-memory execution repository
- [x] PostgreSQL repository implementation
- [ ] Complete PostgreSQL integration verification and backend contract harmonization
- [ ] Application configuration and repository selection
- [ ] REST API and persistent workflow definitions

### Phase 2 — Production Behavior

- [x] Resile-backed retry policies
- [x] Cancellation and execution event history
- [ ] Request idempotency and timeout policy design
- [ ] Recovery after process failure
- [ ] Structured logging and operational telemetry

### Phase 3 — AI and Reference Workflows

- [ ] LLM provider abstraction and structured output
- [ ] Incident-analysis workflow
- [ ] Support-ticket-triage workflow
- [ ] LLM telemetry and error handling

### Phase 4 — Distributed Execution

- [ ] Distributed scheduling and worker processes
- [ ] Parallel execution of independent steps
- [ ] Evaluate Redis and Kafka for coordination and messaging
- [ ] Distributed tracing

### Phase 5 — Deployment and Operations

- [x] Local Docker Compose environment
- [x] GitHub Actions test workflow
- [ ] Application containers and Kubernetes deployment
- [ ] Terraform and AWS deployment
- [ ] Prometheus metrics and Grafana dashboards
- [ ] Load testing

## Status

PandectRun is under active development. The execution engine and repository
implementations are the current foundation. The reference workflows, service
APIs, AI integrations, and distributed architecture describe planned
capabilities rather than available application features.

## License

See the [`LICENSE`](LICENSE) file for licensing terms.
