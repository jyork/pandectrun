# PandectRun

**AI workflow orchestration for deterministic logic, APIs, and LLMs.**

[pandectrun.com](https://pandectrun.com) · [Architecture](docs/architecture.md)

PandectRun is a production-oriented workflow orchestration platform for building reliable workflows that combine deterministic application logic, external services, and AI models.

The goal is not to build another AI chatbot. PandectRun explores the infrastructure required to run AI-assisted workflows as dependable software: explicit execution graphs, retries, persistence, idempotency, observability, structured model output, and auditable execution state.

## Project Goals

- **Workflows, not prompts** — AI calls are individual steps inside a larger deterministic execution model.
- **Durable execution** — workflow state and intermediate results can survive process or service failures.
- **Explicit dependencies** — steps declare what must complete before they can execute.
- **Provider independence** — AI providers sit behind a common abstraction.
- **Structured results** — steps exchange schema-validated data rather than unstructured conversations.
- **Operational visibility** — executions produce logs, metrics, traces, and audit events.
- **Recoverable failure** — retries, backoff, timeouts, cancellation, and idempotency are first-class concerns.

## Reference Workflows

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

At a high level, PandectRun separates workflow execution from the infrastructure and integrations used by individual steps. PostgreSQL is the durable system of record; Redis and Kafka are introduced as the project moves toward distributed execution rather than being prerequisites for the first runnable version.

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
  },
  "idempotency_key": "ticket-4451-v1"
}
```

## Technology Direction

- **Application:** Go 1.26, REST APIs, gRPC where appropriate
- **Persistence:** PostgreSQL; Redis for caching and coordination
- **Messaging:** Kafka as distributed/event-driven execution is introduced
- **AI:** provider abstraction, structured output, retries, timeouts, telemetry
- **Observability:** OpenTelemetry, Prometheus, Grafana, structured logging
- **Deployment:** Docker, Kubernetes, AWS, Terraform, GitHub Actions

PandectRun targets **Go 1.26**. Older Go versions are not supported; the project will use the current language and standard-library capabilities available at this baseline rather than carrying compatibility code for earlier releases.

Local development should remain lightweight even as the target production architecture moves toward Kubernetes.

## Dependencies

PandectRun aims to keep its dependency surface small. The standard library is preferred where it provides a clear, maintainable solution. External dependencies are added deliberately when they provide substantial production behavior that would otherwise require significant custom infrastructure code or recreate well-understood edge cases.

### Resile

[`github.com/cinar/resile`](https://github.com/cinar/resile) provides the retry and execution-resilience primitives used by PandectRun. The initial integration will focus on retry behavior such as bounded attempts, exponential backoff with jitter, context cancellation and deadlines, terminal/fatal error signaling, and observability hooks.

PandectRun will keep its own retry-policy abstraction at the workflow-engine boundary rather than exposing Resile-specific configuration in persisted workflow definitions. This keeps workflow semantics owned by PandectRun while allowing the underlying resilience implementation to evolve independently.

More advanced Resile features—including circuit breakers, adaptive retries, bulkheads, hedged requests, rate limiting, and fallback strategies—will be introduced only when a concrete workflow or operational requirement justifies them.

The module is pinned in `go.mod`; dependency upgrades should be intentional and reviewed like other runtime behavior changes.

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

A step reads the context it depends on, performs its operation, produces structured output, stores that output under its step ID, and emits telemetry and audit information. The engine uses the dependency graph to determine which steps are eligible to run.

## Reliability

External APIs and AI providers are treated as unreliable dependencies. PandectRun owns the workflow-level semantics for retryable versus terminal failures, persisted execution state, idempotency, cancellation, and execution history. Resile provides the underlying retry/resilience mechanics, including backoff, jitter, attempt limits, context-aware cancellation, and related execution policies.

This separation keeps dependency-specific behavior out of persisted workflow definitions and prevents the orchestrator API from becoming coupled to a particular retry library.

## Observability

Workflow executions should be traceable from the incoming API request through individual workflow steps and external dependencies. Planned telemetry includes execution and step latency, failures, retries, LLM latency and token usage, external API latency, queue depth, and execution status transitions.

## Target Repository Structure

```text
pandectrun/
├── cmd/
│   ├── api/
│   └── worker/
├── internal/
│   ├── engine/
│   ├── steps/
│   ├── providers/
│   ├── repository/
│   ├── api/
│   ├── config/
│   └── telemetry/
├── workflows/
│   ├── incident-analysis/
│   └── support-ticket-triage/
├── docs/
├── deployments/
├── terraform/
├── scripts/
├── docker-compose.yml
├── go.mod
└── README.md
```

This is a target structure, not a description of the current repository contents.

## Roadmap

### Phase 1 — Execution Core
- Workflow definition schema and dependency graph validation
- Execution state model and step interface
- Sequential workflow execution
- REST API
- PostgreSQL persistence

### Phase 2 — Production Behavior
- Resile-backed retry/backoff and timeout policies
- Idempotency and cancellation
- Execution event history
- Redis integration
- Structured logging

### Phase 3 — AI and Reference Workflows
- LLM provider abstraction and structured output
- Incident-analysis workflow
- Support-ticket-triage workflow
- LLM telemetry and error handling

### Phase 4 — Distributed Execution
- Kafka-backed execution events and worker processes
- Parallel execution of independent steps
- Recovery after worker failure
- Distributed tracing

### Phase 5 — Deployment and Operations
- Docker and Kubernetes
- Terraform and AWS deployment
- GitHub Actions CI/CD
- Prometheus metrics and Grafana dashboards
- Load testing

## Status

PandectRun is under active development. The architecture and interfaces documented here describe the project's intended direction and will evolve with the implementation.

## License

See the [`LICENSE`](LICENSE) file for licensing terms.
