# PandectRun

**AI workflow orchestration for deterministic logic, APIs, and LLMs.**

[pandectrun.com](https://pandectrun.com) · [GitHub](https://github.com/jyork/pandectrun)

PandectRun is a production-oriented workflow orchestration platform for building reliable workflows that combine deterministic application logic, external services, and AI models.

The goal is not to build another AI chatbot. PandectRun explores the infrastructure required to run AI-assisted workflows as dependable software: explicit execution graphs, retries, persistence, idempotency, observability, structured model output, and auditable execution state.

## Project Goals

PandectRun is designed around a few core principles:

- **Workflows, not prompts** — AI calls are individual steps inside a larger deterministic execution model.
- **Durable execution** — workflow state and intermediate results can survive process or service failures.
- **Explicit dependencies** — steps declare what must complete before they can execute.
- **Provider independence** — AI providers are accessed through a common abstraction rather than embedded throughout application logic.
- **Structured results** — workflow steps exchange typed or schema-validated data instead of unstructured conversations.
- **Operational visibility** — executions produce logs, metrics, traces, and audit events.
- **Recoverable failure** — retries, backoff, timeouts, cancellation, and idempotency are first-class execution concerns.

## Reference Workflows

PandectRun will initially ship with two example workflows that exercise the engine in realistic scenarios.

### Incident Analysis

The primary reference workflow analyzes operational incidents using alerts, recent deployments, log samples, runbooks, deterministic heuristics, and an LLM.

A typical execution might:

1. Validate the incident payload.
2. Correlate alerts with recent deployments.
3. Apply deterministic diagnostic heuristics.
4. Analyze logs and symptoms with an LLM.
5. Retrieve an applicable runbook.
6. Generate recommended diagnostic actions.
7. Assign confidence scores to likely causes.
8. Persist the result and execution history.

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
    {
      "cause": "Database connection pool exhaustion",
      "confidence": 0.87
    },
    {
      "cause": "Regression introduced in version 2.14.0",
      "confidence": 0.66
    }
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

The second reference workflow demonstrates a business-oriented orchestration use case.

A ticket can pass through:

1. Input validation.
2. Customer metadata lookup.
3. Sensitive-data redaction.
4. LLM classification.
5. Deterministic priority rules.
6. Summary generation.
7. Queue selection.
8. Result persistence and event publication.

This workflow demonstrates how deterministic rules and AI classification can coexist within the same execution graph.

## Architecture

![PandectRun architecture](docs/architecture.png)

At a high level, PandectRun separates workflow execution from the infrastructure and integrations used by individual steps.

```text
                         Clients
                           |
                           v
                    REST / gRPC API
                           |
                           v
                  +-------------------+
                  |  Workflow Engine  |
                  |-------------------|
                  | Orchestrator      |
                  | Step Runner       |
                  | Retry / Backoff   |
                  | Timeouts          |
                  | Idempotency       |
                  | State Management  |
                  +---------+---------+
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
        Step Handlers   State / Cache   Event Bus
              |             |             |
      +-------+------+      |             |
      |       |      |      |             |
      v       v      v      v             v
 Validation  HTTP   LLM  PostgreSQL     Kafka
              |
              +-------------------------+
              |
              v
       External Services
       LLMs / APIs / Tools
```

Workflow definitions describe reusable steps and their dependencies rather than embedding a single hard-coded workflow into the engine.

Conceptually:

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

Each step receives the workflow execution context and contributes structured output to that context.

## Planned Step Types

| Step | Purpose |
|---|---|
| `validation` | Validate input or intermediate state |
| `http` | Call external REST services |
| `llm` | Invoke a configured AI model |
| `rules` | Apply deterministic business or diagnostic rules |
| `event` | Publish an event to downstream consumers |

Additional step types can be added through the same execution interface.

## API Direction

The initial REST API is expected to expose operations similar to:

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

The project is intended to exercise a production-oriented cloud-native stack.

- **Application:** Go, REST APIs, gRPC where appropriate, structured JSON workflow definitions
- **Persistence and messaging:** PostgreSQL, Redis, Kafka
- **AI integration:** provider abstraction, structured responses, timeouts, retries, telemetry, caching where appropriate
- **Observability:** OpenTelemetry, Prometheus, Grafana, structured logging
- **Deployment:** Docker, Kubernetes, AWS, Terraform, GitHub Actions

Local development should remain lightweight enough to run with Docker Compose even as the production deployment architecture targets Kubernetes.

## Execution Model

A workflow execution maintains an accumulated context:

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

A step receives the current execution context, reads the portions it depends on, performs its operation, produces structured output, stores that output under its step ID, and emits execution telemetry and audit information. The engine uses the workflow dependency graph to determine which steps are eligible to run.

## Reliability

PandectRun is intended to treat external APIs and AI providers as unreliable dependencies. The orchestration layer will account for configurable retries, exponential backoff, step-level timeouts, idempotency, cancellation, retryable versus terminal errors, persisted execution state, and execution event history.

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

- Retry and backoff policies
- Step timeouts and idempotency
- Cancellation and execution event history
- Redis integration
- Structured logging

### Phase 3 — AI and Reference Workflows

- LLM provider abstraction and structured output
- Incident-analysis workflow
- Support-ticket-triage workflow
- LLM telemetry and error handling

### Phase 4 — Distributed Execution

- Kafka-backed execution events
- Worker processes
- Parallel execution of independent steps
- Recovery after worker failure
- Distributed tracing

### Phase 5 — Deployment and Operations

- Docker images and Kubernetes manifests
- Terraform and AWS deployment
- GitHub Actions CI/CD
- Prometheus metrics and Grafana dashboards
- Load testing

## Status

PandectRun is currently under active development. The architecture and interfaces documented here describe the project's intended direction and will evolve as the implementation progresses.

## License

See the [`LICENSE`](LICENSE) file for licensing terms.
