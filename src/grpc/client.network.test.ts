/**
 * AegisRuntimeClient against a real @grpc/grpc-js server on loopback.
 *
 * `client.test.ts` mocks @grpc/grpc-js; these tests do not. They reproduce
 * what production saw on 2026-10-02 when a deploy replaced the orchestrator's
 * pod under the worker: the old connection went silent (no FIN, no RST), the
 * name `aegis-core` moved to a new address, and a stream the client had
 * already settled later received the dead transport's error with no listener
 * left, which ended the Node process.
 *
 * The replacement is simulated with a TCP relay that can go silent (it keeps
 * every socket open and forwards nothing, as a vanished peer does), and with
 * `dns.promises.lookup`, which grpc-js's DNS resolver calls at resolution
 * time, answering for one test name: first 127.0.0.1, then nothing (the old
 * pod is gone, the new one not yet up), then 127.0.0.2, where the new server
 * listens on the same port.
 */

import dns from "node:dns";
import http from "node:http";
import net from "node:net";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { ApplicationFailure } from "@temporalio/activity";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { PROTO_LOADER_OPTIONS, PROTO_PATH } from "./proto-options.js";

const env = vi.hoisted(() => {
  process.env.DATABASE_URL ??= "postgresql://worker@localhost:5432/worker";
  // The status-polling path, fast: idle 10 ms, first poll 10 ms, two polls.
  process.env.AEGIS_EXECUTION_FALLBACK_IDLE_MS = "10";
  process.env.AEGIS_EXECUTION_FALLBACK_POLL_MS = "10";
  process.env.AEGIS_EXECUTION_FALLBACK_MAX_RETRIES = "2";
  return {};
});
void env;

vi.mock("../logger.js", () => ({
  logger: {
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    fatal: vi.fn(),
  },
}));

vi.mock("../auth/token-manager.js", () => ({
  getServiceToken: vi.fn().mockResolvedValue("test-token"),
}));

const { AegisRuntimeClient } = await import("./client.js");
const { config } = await import("../config.js");

const proto = grpc.loadPackageDefinition(
  protoLoader.loadSync(PROTO_PATH, PROTO_LOADER_OPTIONS),
) as any;
const AegisRuntime = proto.aegis.runtime.v1.AegisRuntime;

const INTERNAL_RST =
  "Received RST_STREAM with code 2 triggered by internal client error: read EHOSTUNREACH";

type Handlers = Partial<Record<string, (...args: any[]) => void>>;

const servers: grpc.Server[] = [];
const clients: { close(): void }[] = [];
const closers: (() => void)[] = [];

function startServer(address: string, handlers: Handlers): Promise<number> {
  const server = new grpc.Server();
  server.addService(AegisRuntime.service, handlers);
  servers.push(server);
  return new Promise((resolve, reject) =>
    server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (e, p) =>
      e ? reject(e) : resolve(p),
    ),
  );
}

function newClient(address: string, overrides: Record<string, number> = {}) {
  const client = new AegisRuntimeClient(address, {
    ...config.grpc,
    ...overrides,
  } as any);
  clients.push(client);
  return client;
}

function agentRequest(agentId: string, tenantId?: string) {
  return {
    agent_id: agentId,
    input: "x",
    context_json: "{}",
    timeout_seconds: 600,
    ...(tenantId ? { tenant_id: tenantId } : {}),
  };
}

/** Errors the process saw with no listener, while a test ran. */
function watchUncaught() {
  const seen: unknown[] = [];
  const listener = (error: unknown) => seen.push(error);
  process.on("uncaughtException", listener);
  return {
    seen,
    stop: () => process.off("uncaughtException", listener),
  };
}

/** A local stand-in for the orchestrator's GET /v1/executions/{id}. */
async function statusStandIn(status: number, body: object) {
  const requests: { url: string; headers: http.IncomingHttpHeaders }[] = [];
  const server = http.createServer((req, res) => {
    requests.push({ url: req.url ?? "", headers: req.headers });
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(body));
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  closers.push(() => server.close());
  const { port } = server.address() as net.AddressInfo;
  return { url: `http://127.0.0.1:${port}`, requests };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Settles with the promise's outcome and its time, or says it still pends. */
async function within<T>(promise: Promise<T>, capMs: number, what: string) {
  const t0 = performance.now();
  const outcome = await Promise.race([
    promise.then(
      (value) => ({ ok: true as const, value }),
      (error: any) => ({ ok: false as const, error }),
    ),
    sleep(capMs).then(() => ({ pending: true as const })),
  ]);
  if ("pending" in outcome) {
    throw new Error(`${what} still pending after ${capMs} ms`);
  }
  return { ...outcome, ms: Math.round(performance.now() - t0) };
}

afterAll(() => {
  for (const c of clients) c.close();
  for (const s of servers) s.forceShutdown();
  for (const close of closers) close();
});

describe("a stream the client has settled", () => {
  it("is cancelled when its terminal event settles it, and a later server error reaches a listener", async () => {
    let cancelled = false;
    const port = await startServer("127.0.0.1:0", {
      ExecuteAgent: (call: grpc.ServerWritableStream<any, any>) => {
        call.on("cancelled", () => (cancelled = true));
        call.write({
          execution_completed: {
            execution_id: "exec-terminal",
            final_output: "done",
            total_iterations: 1,
            completed_at: "2026-10-02T03:00:00Z",
          },
        });
        // The transport's error arrives after the client has its answer.
        setTimeout(() => {
          if (!call.cancelled) {
            call.emit("error", { code: grpc.status.INTERNAL, details: INTERNAL_RST });
          }
        }, 200);
      },
    });
    const client = newClient(`127.0.0.1:${port}`);
    const uncaught = watchUncaught();
    try {
      const events = await client.executeAgent(agentRequest("agent-1"));
      expect(events.at(-1)?.event_type).toBe("ExecutionCompleted");
      await sleep(500);
      expect(uncaught.seen).toEqual([]);
      expect(cancelled).toBe(true);
    } finally {
      uncaught.stop();
    }
  });

  it("is cancelled when status polling settles it, and a later server error reaches a listener", async () => {
    const standIn = await statusStandIn(200, { status: "Running" });
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
    let cancelled = false;
    const port = await startServer("127.0.0.1:0", {
      ExecuteAgent: (call: grpc.ServerWritableStream<any, any>) => {
        call.on("cancelled", () => (cancelled = true));
        call.write({
          iteration_started: {
            execution_id: "exec-polled",
            iteration_number: 1,
            started_at: "2026-10-02T03:00:00Z",
          },
        });
        setTimeout(() => {
          if (!call.cancelled) {
            call.emit("error", { code: grpc.status.INTERNAL, details: INTERNAL_RST });
          }
        }, 1000);
      },
    });
    const client = newClient(`127.0.0.1:${port}`);
    const uncaught = watchUncaught();
    try {
      const events = await client.executeAgent(agentRequest("agent-1"));
      expect(events.at(-1)?.reason).toBe(
        "Execution status polling exhausted after 2 attempts",
      );
      await sleep(1300);
      expect(uncaught.seen).toEqual([]);
      expect(cancelled).toBe(true);
    } finally {
      uncaught.stop();
    }
  });
});

describe("the status poll", () => {
  async function pollAgainst404(tenantId?: string) {
    const standIn = await statusStandIn(404, { error: "Execution not found" });
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
    const port = await startServer("127.0.0.1:0", {
      ExecuteAgent: (call: grpc.ServerWritableStream<any, any>) => {
        call.write({
          iteration_started: {
            execution_id: "exec-quiet",
            iteration_number: 1,
            started_at: "2026-10-02T03:38:24Z",
          },
        });
        // A quiet stream: the agent runs on and sends nothing more.
      },
    });
    const events = await newClient(`127.0.0.1:${port}`).executeAgent(
      agentRequest("coder", tenantId),
    );
    return { events, requests: standIn.requests };
  }

  it("names the request's tenant in X-Tenant-Id", async () => {
    const { requests } = await pollAgainst404("tenant-a");
    expect(requests.map((r) => r.url)).toEqual([
      "/v1/executions/exec-quiet",
      "/v1/executions/exec-quiet",
    ]);
    for (const r of requests) {
      expect(r.headers["x-tenant-id"]).toBe("tenant-a");
      expect(r.headers.authorization).toBe("Bearer test-token");
    }
  });

  it("sends no X-Tenant-Id for a request without a tenant", async () => {
    const { requests } = await pollAgainst404(undefined);
    expect(requests).toHaveLength(2);
    for (const r of requests) {
      expect(r.headers).not.toHaveProperty("x-tenant-id");
    }
  });

  it("still counts a 404 as a failed attempt", async () => {
    const { events, requests } = await pollAgainst404("tenant-a");
    expect(requests).toHaveLength(2);
    expect(events.at(-1)).toMatchObject({
      event_type: "ExecutionFailed",
      execution_id: "exec-quiet",
      reason: "Execution status polling exhausted after 2 attempts",
    });
  });
});

/**
 * A TCP relay in front of a server, which can go silent: it then keeps every
 * socket open, forwards nothing and accepts no new connection, as a replaced
 * pod's address does.
 */
async function silenceableRelay(upstreamPort: number) {
  let silent = false;
  const sockets: net.Socket[] = [];
  const listener = net.createServer((downstream) => {
    const upstream = net.connect(upstreamPort, "127.0.0.1");
    sockets.push(downstream, upstream);
    downstream.on("data", (d) => !silent && upstream.write(d));
    upstream.on("data", (d) => !silent && downstream.write(d));
    downstream.on("error", () => {});
    upstream.on("error", () => {});
    upstream.on("close", () => !silent && downstream.destroy());
    downstream.on("close", () => !silent && upstream.destroy());
  });
  const port = await new Promise<number>((r) =>
    listener.listen(0, "127.0.0.1", () =>
      r((listener.address() as net.AddressInfo).port),
    ),
  );
  closers.push(() => {
    listener.close();
    sockets.forEach((s) => s.destroy());
  });
  return {
    port,
    goSilent: () => {
      silent = true;
      listener.close();
    },
  };
}

/** Names this file resolves itself; everything else goes to the system. */
const names = new Map<string, string | null>();
const systemLookup = dns.promises.lookup;
beforeAll(() => {
  (dns.promises as any).lookup = async (host: string, options: any) => {
    if (!names.has(host)) return systemLookup(host, options);
    const address = names.get(host);
    if (!address) {
      throw Object.assign(new Error(`getaddrinfo ENOTFOUND ${host}`), {
        code: "ENOTFOUND",
      });
    }
    return options?.all ? [{ address, family: 4 }] : { address, family: 4 };
  };
});
afterAll(() => {
  (dns.promises as any).lookup = systemLookup;
});

const orchestratorHandlers = (label: string): Handlers => ({
  QueryCortexPatterns: (_call: any, cb: grpc.sendUnaryData<any>) =>
    cb(null, { patterns: [] }),
  ValidateWithJudges: () => {
    // Held: a judge panel that is still deliberating.
  },
  ExecuteAgent: (call: grpc.ServerWritableStream<any, any>) => {
    if (call.request.agent_id === "hold") return;
    call.write({
      execution_completed: {
        execution_id: `exec-${label}`,
        final_output: `served by ${label}`,
        total_iterations: 1,
        completed_at: "2026-10-02T03:00:00Z",
      },
    });
    call.end();
  },
});

/**
 * The orchestrator `name` served by server A behind a relay; `replace()` makes
 * A's connection silent, refuses A's address, makes the name unresolvable, and
 * after `outageMs` (unless null) serves it from server B at 127.0.0.2 on the
 * same port.
 */
async function replaceableOrchestrator(name: string) {
  const portA = await startServer("127.0.0.1:0", orchestratorHandlers("A"));
  const relay = await silenceableRelay(portA);
  names.set(name, "127.0.0.1");
  const serverA = servers.at(-1)!;
  return {
    target: `${name}:${relay.port}`,
    replace(outageMs: number | null) {
      const t0 = performance.now();
      relay.goSilent();
      serverA.forceShutdown();
      names.set(name, null);
      const bUp =
        outageMs === null
          ? null
          : sleep(outageMs)
              .then(() =>
                startServer(`127.0.0.2:${relay.port}`, orchestratorHandlers("B")),
              )
              .then(() => {
                names.set(name, "127.0.0.2");
                return performance.now() - t0;
              });
      return { t0, bUp };
    },
  };
}

function expectRetryableUnavailable(error: any) {
  // The activity layer rethrows it (src/activities/index.ts), and Temporal
  // retries any error that is not an ApplicationFailure marked non-retryable.
  expect(error).toBeInstanceOf(Error);
  expect(error).not.toBeInstanceOf(ApplicationFailure);
  expect(error.code).toBe(grpc.status.UNAVAILABLE);
}

describe("the orchestrator's pod replaced under a kept client", () => {
  const detectBoundMs = () =>
    config.grpc.keepaliveTimeMs + config.grpc.keepaliveTimeoutMs + 1_000;

  it.concurrent(
    "ends the calls in flight as retryable within the keepalive bound, and the next calls reach the new server",
    async () => {
      const orchestrator = await replaceableOrchestrator("core-a.aegis.test");
      const client = newClient(orchestrator.target);
      expect(
        (await within(client.queryCortexPatterns({ error_signature: "e" }), 5_000, "warm-up")).ok,
      ).toBe(true);

      const heldStream = client.executeAgent(agentRequest("hold"));
      const heldUnary = client.validateWithJudges({
        output: "o",
        judges: [],
      } as any);
      await sleep(300);
      const { bUp } = orchestrator.replace(8_000);

      const [stream, unary] = await Promise.all([
        within(heldStream, detectBoundMs(), "executeAgent in flight"),
        within(heldUnary, detectBoundMs(), "validateWithJudges in flight"),
      ]);
      expect(stream.ok).toBe(false);
      expect(unary.ok).toBe(false);
      expectRetryableUnavailable((stream as any).error);
      expectRetryableUnavailable((unary as any).error);
      // The error names the client method that made the call.
      expect(
        String((stream as any).error.stack).split("\nfor call at\n")[1],
      ).toContain("AegisRuntimeClient.executeAgent");
      console.log(
        `[measure] in flight at the replacement: executeAgent failed after ${stream.ms} ms, validateWithJudges after ${unary.ms} ms (bound ${detectBoundMs()} ms)`,
      );

      const bUpMs = await bUp!;
      await sleep(1_000); // Temporal's first retry interval.
      const nextUnary = await within(
        client.queryCortexPatterns({ error_signature: "e" }),
        config.grpc.connectTimeoutMs,
        "next queryCortexPatterns",
      );
      const nextStream = await within(
        client.executeAgent(agentRequest("agent-1")),
        config.grpc.connectTimeoutMs,
        "next executeAgent",
      );
      expect(nextUnary.ok).toBe(true);
      expect(nextStream.ok).toBe(true);
      expect((nextStream as any).value.at(-1).final_output).toBe("served by B");
      console.log(
        `[measure] B up ${Math.round(bUpMs)} ms after the replacement; next unary ${nextUnary.ms} ms, next executeAgent ${nextStream.ms} ms`,
      );
    },
    60_000,
  );

  it.concurrent(
    "waits for a new server that comes up after the dead connection was found, and reaches it within the reconnect bound",
    async () => {
      const orchestrator = await replaceableOrchestrator("core-b.aegis.test");
      const client = newClient(orchestrator.target);
      expect(
        (await within(client.queryCortexPatterns({ error_signature: "e" }), 5_000, "warm-up")).ok,
      ).toBe(true);
      const { t0, bUp } = orchestrator.replace(40_000);

      // The idle channel finds the dead connection by itself.
      await sleep(detectBoundMs());
      const next = within(
        client.executeAgent(agentRequest("agent-1")),
        config.grpc.connectTimeoutMs,
        "executeAgent issued during the outage",
      );
      const bUpMs = await bUp!;
      const result = await next;
      expect(result.ok).toBe(true);
      expect((result as any).value.at(-1).final_output).toBe("served by B");
      const afterBUp = Math.round(performance.now() - t0 - bUpMs);
      const reconnectBoundMs =
        Math.max(
          config.grpc.maxReconnectBackoffMs,
          config.grpc.dnsMinTimeBetweenResolutionsMs,
        ) *
          1.2 +
        1_000;
      console.log(
        `[measure] B up ${Math.round(bUpMs)} ms after the replacement; the waiting executeAgent succeeded ${afterBUp} ms after B was up (bound ${reconnectBoundMs} ms)`,
      );
      expect(afterBUp).toBeLessThanOrEqual(reconnectBoundMs);
    },
    90_000,
  );

  it.concurrent(
    "fails a call within the bounds, without hanging, when the server is gone for good",
    async () => {
      const orchestrator = await replaceableOrchestrator("core-c.aegis.test");
      const connectTimeoutMs = 5_000;
      const client = newClient(orchestrator.target, { connectTimeoutMs });
      expect(
        (await within(client.queryCortexPatterns({ error_signature: "e" }), 5_000, "warm-up")).ok,
      ).toBe(true);
      orchestrator.replace(null);

      const first = await within(
        client.executeAgent(agentRequest("agent-1")),
        detectBoundMs(),
        "executeAgent on the dead connection",
      );
      expect(first.ok).toBe(false);
      expectRetryableUnavailable((first as any).error);

      const second = await within(
        client.queryCortexPatterns({ error_signature: "e" }),
        connectTimeoutMs + 1_000,
        "queryCortexPatterns with no server",
      );
      expect(second.ok).toBe(false);
      expectRetryableUnavailable((second as any).error);
      console.log(
        `[measure] gone for good: first call failed after ${first.ms} ms (bound ${detectBoundMs()} ms), the next after ${second.ms} ms (bound ${connectTimeoutMs + 1_000} ms)`,
      );
    },
    60_000,
  );
});
