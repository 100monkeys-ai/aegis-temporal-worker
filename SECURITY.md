# Security Policy — AEGIS Temporal Worker

## Reporting a Vulnerability

**Please do not report security vulnerabilities through GitHub Issues.**

Report security vulnerabilities privately via GitHub's [Security Advisory](https://github.com/100monkeys-ai/aegis-temporal-worker/security/advisories/new) feature, or email **<security@100monkeys.ai>**.

Include:

- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested mitigations (if any)

We will acknowledge receipt within **48 hours** and aim to provide a fix or mitigation timeline within **7 days**.

## Security Model

The AEGIS Temporal Worker is an **infrastructure component** that sits between the Rust orchestrator and Temporal's durable execution engine. Key security considerations:

### Trust Boundaries

- **The worker trusts only the AEGIS orchestrator** — workflow definitions arrive via HTTP POST from the Rust service over a private internal network, not from the internet.
- **gRPC calls are internal** — all `AegisRuntime` gRPC calls are to the Rust service on `localhost` or a private Docker network.
- **No agent code runs inside this process** — agent execution is delegated to isolated containers via the Rust orchestrator.

### Secrets

- No API keys or secrets are stored in this service.
- Database credentials are injected via environment variables (see `.env.example`).
- Never log values of sensitive environment variables.

### Network Exposure

- Port `3000` (HTTP API) should **not** be exposed to the public internet — it is an internal service endpoint used by the Rust orchestrator.
- Temporal server (`7233`) and PostgreSQL (`5432`) must be on a private network.

### Dependencies

We use `npm audit` in CI to detect known vulnerabilities. Pin dependency versions and review `package-lock.json` changes carefully.

## Supported Versions

| Version            | Supported |
| ------------------ | --------- |
| `main` branch      | ✅        |
| All older releases | ❌        |

We are pre-alpha. Only the latest `main` branch receives security fixes.
