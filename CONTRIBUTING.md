# Contributing to AEGIS Temporal Worker

Thank you for your interest in contributing! This document provides guidelines for contributing to the `aegis-temporal-worker` repo.

## Table of Contents

- [Contributing to AEGIS Temporal Worker](#contributing-to-aegis-temporal-worker)
  - [Table of Contents](#table-of-contents)
  - [Code of Conduct](#code-of-conduct)
  - [Development Setup](#development-setup)
    - [Prerequisites](#prerequisites)
    - [Initial Setup](#initial-setup)
    - [Development Workflow](#development-workflow)
    - [Linting \& Formatting](#linting--formatting)
  - [Architecture Overview](#architecture-overview)
  - [Contribution Workflow](#contribution-workflow)
  - [Coding Standards](#coding-standards)
  - [Testing](#testing)
  - [Proto Changes](#proto-changes)

## Code of Conduct

By participating in this project you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Development Setup

### Prerequisites

- **Node.js** 20+
- **npm** 9+
- **PostgreSQL** 15+ (for workflow registry)
- **Temporal Server** (run locally via `temporal server start-dev` or the compose stack in [aegis-examples](https://github.com/100monkeys-ai/aegis-examples))
- **AEGIS Runtime** — the Rust gRPC service (`aegis-orchestrator`)

### Initial Setup

```bash
# Clone the repo
git clone https://github.com/100monkeys-ai/aegis-temporal-worker.git
cd aegis-temporal-worker

# Install dependencies
npm ci

# Configure environment
cp .env.example .env
# Edit .env with your local settings

# Build
npm run build

# Run tests
npm test
```

### Development Workflow

```bash
# Start both HTTP server and Temporal worker with hot-reload
npm run dev:all

# Or start them separately
npm run dev:server   # Terminal 1: HTTP registration API
npm run dev:worker   # Terminal 2: Temporal worker
```

### Linting & Formatting

```bash
npm run lint        # ESLint
npm run format      # Prettier
```

## Architecture Overview

The AEGIS Temporal Worker is the **infrastructure layer** of the Workflow Orchestration bounded context. It bridges the AEGIS domain model (Rust) with Temporal's durable execution engine (TypeScript).

```markdown
AEGIS YAML Manifest
    ↓ (Rust TemporalWorkflowMapper)
TemporalWorkflowDefinition (JSON)
    ↓ HTTP POST /register-workflow
Temporal Worker (this repo)
    ├── Activities → gRPC → Rust AegisRuntime :50051
    └── Events    → HTTP → Rust /v1/temporal-events
```

Key source files:

| File | Purpose |
| ------ | --------- |
| `src/index.ts` | Main entry point |
| `src/config.ts` | Zod-validated env config |
| `src/server.ts` | Express HTTP (workflow registration API) |
| `src/worker.ts` | Temporal worker initialization |
| `src/workflows/aegis-workflow.ts` | Generic Interpreter Workflow |
| `src/activities/index.ts` | Temporal activities (gRPC calls to Rust) |
| `src/grpc/client.ts` | gRPC client for AegisRuntime service |
| `aegis-proto/proto/aegis_runtime.proto` | gRPC service definition (git submodule) |

See [ADR-022](https://github.com/100monkeys-ai/aegis-architecture/blob/main/adrs/022-temporal-workflow-engine-integration.md) for the full architectural decision record.

## Contribution Workflow

1. **Create a feature branch**

   ```bash
   git checkout -b feat/your-feature-name
   ```

2. **Make your changes** following the coding standards below.

3. **Run checks locally**

   ```bash
   npm run lint
   npm run build
   npm test
   ```

4. **Commit** using [Conventional Commits](https://www.conventionalcommits.org/):

   ```markdown
   feat: add human input signal support
   fix: correct proto path resolution in Docker
   chore: update Temporal SDK to 1.11
   ```

5. **Open a Pull Request** against `main`.

## Coding Standards

- **TypeScript strict mode** — all code must pass `tsc --strict`
- **ESLint** — no warnings allowed (`npm run lint` must pass clean)
- **Prettier** — all files formatted (`npm run format`)
- **Domain language** — use terms from [AGENTS.md](https://github.com/100monkeys-ai/aegis-architecture/blob/main/AGENTS.md): `Execution`, `Iteration`, `Workflow`, `Blackboard`, `StateKind`, etc. Never `job`, `run`, `task`
- **License header** — all new files must include the SPDX header:

  ```typescript
  // SPDX-License-Identifier: AGPL-3.0-only
  // Copyright 2026 100monkeys.ai
  ```

- **No `any` except where explicitly required** — use proper types from `src/types.ts`

## Testing

```bash
npm test              # Run all tests
npm run test:watch    # Watch mode
```

Tests live alongside source in `src/**/*.test.ts`. Any new feature should include tests.

## Proto Changes

`aegis-proto/proto/aegis_runtime.proto` is a git submodule pinned to [github.com/100monkeys-ai/aegis-proto](https://github.com/100monkeys-ai/aegis-proto). To update the proto:

1. Make the change in the `aegis-proto` repo and tag a new version (e.g. `v1.1.0`)
2. In this repo, update the submodule ref to the new tag:

   ```bash
   cd aegis-proto
   git fetch
   git checkout v1.1.0
   cd ..
   git add aegis-proto
   git commit -m "chore: bump aegis-proto to v1.1.0"
   ```

3. Update the corresponding TypeScript types in `src/types.ts`
4. Submit PRs to all affected consumer repos together
