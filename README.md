# AEGIS Temporal Worker

[![CI](https://github.com/100monkeys-ai/aegis-temporal-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/100monkeys-ai/aegis-temporal-worker/actions/workflows/ci.yml)
[![Docker](https://github.com/100monkeys-ai/aegis-temporal-worker/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/100monkeys-ai/aegis-temporal-worker/actions/workflows/docker-publish.yml)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

TypeScript-based Temporal Worker for AEGIS workflow orchestration. Part of the [AEGIS](https://github.com/100monkeys-ai/aegis-orchestrator) platform.

## Overview

The AEGIS Temporal Worker is the **infrastructure layer** of the Workflow Orchestration bounded context. It dynamically generates and executes [Temporal](https://temporal.io) workflows from AEGIS YAML workflow definitions, bridging the Rust domain model to Temporal's durable execution engine.

```markdown
AEGIS YAML Manifest
       │
       ▼  (Rust TemporalWorkflowMapper — Anti-Corruption Layer)
TemporalWorkflowDefinition (JSON)
       │  HTTP POST /register-workflow
       ▼
┌──────────────────────────────────────────────┐
│          aegis-temporal-worker (this repo)   │
│                                              │
│  HTTP Server :3000 (workflow registration)   │
│  Temporal Worker (aegis-agents task queue)   │
│       │                                      │
│       ├── Activities ─gRPC──► Rust           │
│       │                       AegisRuntime   │
│       │                       :50051         │
│       └── Events    ─HTTP──► Rust            │
│                               /v1/temporal-  │
│                               events         │
└──────────────────────────────────────────────┘
```

### Key Features

- **Generic Interpreter Workflow** — a single Temporal workflow (`aegis_workflow`) interprets all AEGIS FSM definitions at runtime; no code generation needed
- **Dynamic workflow registration** — POST a JSON definition to `/register-workflow`; no worker restart required
- **Multi-worker coordination** — PostgreSQL ensures all worker replicas share the same definitions
- **Full activity suite** — `executeAgentActivity`, `executeSystemCommandActivity`, `validateOutputActivity`, `executeParallelAgentsActivity`
- **Event bridge** — publishes `WorkflowStateEntered`, `IterationStarted`, `RefinementApplied`, etc. back to the Rust orchestrator so they become typed domain events

## Docker Image

```bash
docker pull ghcr.io/100monkeys-ai/aegis-temporal-worker:latest
```

Images are published automatically on every push to `main` (`latest`) and on version tags (`v1.2.3`).

## Getting Started

### Prerequisites

| Dependency | Version |
| ----------- | --------- |
| Node.js | ≥ 20 |
| npm | ≥ 9 |
| PostgreSQL | ≥ 15 |
| Temporal Server | ≥ 1.22 |
| AEGIS Runtime (Rust gRPC) | current `main` |

The easiest way to run all dependencies locally is the [aegis-examples](https://github.com/100monkeys-ai/aegis-examples) Docker Compose stack.

### Quick Start (local dev)

```bash
git clone https://github.com/100monkeys-ai/aegis-temporal-worker.git
cd aegis-temporal-worker

npm ci
cp .env.example .env
# Edit .env with your local settings

# Start both HTTP server and worker with hot-reload
npm run dev:all
```

### Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for the full reference.

| Variable | Default | Description |
| ---------- | --------- | ------------- |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal server gRPC address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `aegis-agents` | Worker task queue name |
| `DATABASE_URL` | *(required)* | PostgreSQL connection string |
| `AEGIS_RUNTIME_GRPC_URL` | `localhost:50051` | Rust AegisRuntime gRPC address |
| `AEGIS_ORCHESTRATOR_URL` | `http://localhost:8088` | Rust orchestrator HTTP (for event callbacks) |
| `HTTP_PORT` | `3000` | Worker HTTP API port |
| `PROTO_PATH` | `./aegis-proto/proto/aegis_runtime.proto` | Path to gRPC proto file |
| `LOG_LEVEL` | `info` | Pino log level (`trace`/`debug`/`info`/`warn`/`error`) |
| `NODE_ENV` | `development` | Environment (`development`/`production`) |

### Production (Docker Compose)

See [aegis-examples/deploy/](https://github.com/100monkeys-ai/aegis-examples/tree/main/deploy) for the full stack. The worker service:

```yaml
temporal-worker:
  image: ghcr.io/100monkeys-ai/aegis-temporal-worker:latest
  environment:
    TEMPORAL_ADDRESS: temporal:7233
    DATABASE_URL: postgresql://temporal:temporal@postgres:5432/aegis
    AEGIS_RUNTIME_GRPC_URL: aegis-runtime:50051
    AEGIS_ORCHESTRATOR_URL: http://aegis-runtime:8088
  ports:
    - "3000:3000"
```

## API Reference

### `POST /register-workflow`

Register an AEGIS workflow definition. Called by the Rust `TemporalWorkflowMapper`.

```json
{
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "100monkeys-classic",
  "version": "1.0.0",
  "initial_state": "GENERATE",
  "context": {},
  "states": {
    "GENERATE": {
      "kind": "Agent",
      "agent": "coder-v1",
      "input": "{{workflow.task}}",
      "transitions": [{ "condition": "always", "target": "VALIDATE" }]
    }
  }
}
```

### `GET /workflows`

List all registered workflow definitions.

### `GET /workflows/:id`

Retrieve a specific workflow definition by ID.

### `DELETE /workflows/:id`

Remove a workflow definition.

### `GET /health`

Health check. Returns `{ "status": "healthy", "timestamp": "..." }`.

## Project Structure

```markdown
aegis-temporal-worker/
├── aegis-proto/               # git submodule: github.com/100monkeys-ai/aegis-proto
│   └── proto/
│       └── aegis_runtime.proto  # gRPC service definition
├── src/
│   ├── index.ts                   # Main entry: starts HTTP server + Temporal worker
│   ├── config.ts                  # Zod-validated environment configuration
│   ├── logger.ts                  # Pino structured logger
│   ├── types.ts                   # TypeScript type definitions
│   ├── database.ts                # PostgreSQL client
│   ├── server.ts                  # Express HTTP server (workflow registration API)
│   ├── worker.ts                  # Temporal worker initialization
│   ├── workflow-registry.ts       # In-memory workflow function registry
│   ├── workflow-generator.ts      # Generates Temporal workflow functions from definitions
│   ├── activities/
│   │   ├── index.ts               # All activities (executeAgent, validateOutput, etc.)
│   │   ├── event-activities.ts    # publishEventActivity (HTTP → Rust /v1/temporal-events)
│   │   └── workflow-activities.ts # fetchWorkflowDefinition (DB lookup)
│   ├── grpc/
│   │   └── client.ts              # gRPC client for AegisRuntime service
│   └── workflows/
│       ├── index.ts               # Workflow exports
│       └── aegis-workflow.ts      # Generic Interpreter Workflow (core FSM executor)
├── Dockerfile
├── .env.example
└── package.json
```

## Development

```bash
npm run build         # Compile TypeScript → dist/
npm start             # Run compiled output
npm run dev:all       # Hot-reload both server + worker
npm run lint          # ESLint
npm run format        # Prettier
npm test              # Vitest
npm run test:watch    # Vitest in watch mode
```

## Proto File

`aegis-proto/proto/aegis_runtime.proto` defines the gRPC contract between this worker and the Rust `AegisRuntime` service. It is managed in [github.com/100monkeys-ai/aegis-proto](https://github.com/100monkeys-ai/aegis-proto) as a git submodule. To update the proto, see [CONTRIBUTING.md](CONTRIBUTING.md#proto-changes).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[AGPL-3.0-only](LICENSE) — Copyright 2026 100monkeys.ai
