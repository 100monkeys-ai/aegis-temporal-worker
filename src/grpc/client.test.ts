import { EventEmitter } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  executeAgentRpcMock: vi.fn(),
  fetchMock: vi.fn(),
  closeMock: vi.fn(),
  waitForReadyMock: vi.fn(),
  createInsecureMock: vi.fn(() => "insecure-creds"),
  runtimeCtorMock: vi.fn(),
  logger: {
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock("@grpc/proto-loader", () => ({
  loadSync: vi.fn(() => ({})),
}));

vi.mock("@grpc/grpc-js", () => ({
  status: { UNAVAILABLE: 14 },
  credentials: {
    createInsecure: mocks.createInsecureMock,
  },
  loadPackageDefinition: vi.fn(() => ({
    aegis: {
      runtime: {
        v1: {
          AegisRuntime: mocks.runtimeCtorMock,
        },
      },
    },
  })),
  Metadata: class {
    add(_key: string, _value: string) {}
  },
}));

vi.mock("../config.js", () => ({
  config: {
    grpc: {
      runtimeUrl: "runtime:50051",
      keepaliveTimeMs: 10_000,
      keepaliveTimeoutMs: 5_000,
      maxReconnectBackoffMs: 5_000,
      dnsMinTimeBetweenResolutionsMs: 5_000,
      connectTimeoutMs: 60_000,
    },
  },
}));

/** A server-stream call as grpc-js returns it: an emitter that can be cancelled. */
function mockCall() {
  const call = new EventEmitter() as EventEmitter & {
    cancel: ReturnType<typeof vi.fn>;
  };
  call.cancel = vi.fn();
  return call;
}

/** Let executeAgent reach its RPC (it awaits the token, then the channel). */
async function callStarted() {
  for (let i = 0; i < 20 && !mocks.executeAgentRpcMock.mock.calls.length; i++) {
    await Promise.resolve();
  }
  expect(mocks.executeAgentRpcMock).toHaveBeenCalled();
}

/** The channel is ready at once, as grpc-js reports a READY channel. */
function readyChannel(_deadline: number, callback: (error?: Error) => void) {
  callback();
}

vi.mock("../logger.js", () => ({
  logger: mocks.logger,
}));

vi.mock("../auth/token-manager.js", () => ({
  getServiceToken: vi.fn().mockResolvedValue("test-token"),
}));

describe("AegisRuntimeClient.executeAgent", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.useRealTimers();
    mocks.executeAgentRpcMock.mockReset();
    mocks.fetchMock.mockReset();
    mocks.closeMock.mockReset();
    mocks.createInsecureMock.mockClear();
    mocks.runtimeCtorMock.mockReset();
    mocks.runtimeCtorMock.mockImplementation(function () {
      return {
        ExecuteAgent: mocks.executeAgentRpcMock,
        close: mocks.closeMock,
        waitForReady: mocks.waitForReadyMock,
      };
    });
    mocks.waitForReadyMock.mockReset();
    mocks.waitForReadyMock.mockImplementation(readyChannel);

    for (const fn of Object.values(mocks.logger)) {
      fn.mockReset();
    }

    vi.stubGlobal("fetch", mocks.fetchMock);
    process.env.AEGIS_EXECUTION_FALLBACK_IDLE_MS = "10";
    process.env.AEGIS_EXECUTION_FALLBACK_POLL_MS = "10";
    process.env.AEGIS_ORCHESTRATOR_URL = "http://orchestrator.test";
  });

  it("resolves when the runtime stream ends after a terminal event", async () => {
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    call.emit("data", {
      event: "execution_completed",
      execution_completed: {
        execution_id: "exec-1",
        completed_at: "2026-03-22T08:32:12.572760Z",
        final_output: "done",
        total_iterations: 1,
      },
    });
    call.emit("end");

    await expect(promise).resolves.toMatchObject([
      {
        event_type: "ExecutionCompleted",
        execution_id: "exec-1",
        final_output: "done",
        total_iterations: 1,
      },
    ]);

    expect(mocks.logger.info).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "ExecutionCompleted",
        event_count: 1,
        execution_id: "exec-1",
      }),
      "Agent execution reached terminal event",
    );
  });

  it("resolves as soon as a terminal event arrives, before stream end", async () => {
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
      parent_execution_id: "parent-1",
    });

    await callStarted();

    call.emit("data", {
      event: "execution_completed",
      execution_completed: {
        execution_id: "exec-2",
        completed_at: "2026-03-22T08:32:12.572760Z",
        final_output: "done",
        total_iterations: 1,
      },
    });

    await expect(
      Promise.race([
        promise,
        new Promise((resolve) => setTimeout(() => resolve("still-waiting"), 0)),
      ]),
    ).resolves.not.toBe("still-waiting");

    expect(mocks.logger.info).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "ExecutionCompleted",
        event_count: 1,
        execution_id: "exec-2",
      }),
      "Agent execution reached terminal event",
    );
  });

  it("logs the backend failure reason when the runtime stream fails terminally", async () => {
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    call.emit("data", {
      event: "execution_failed",
      execution_failed: {
        execution_id: "exec-3",
        failed_at: "2026-03-22T08:32:12.572760Z",
        reason: "Failed to start execution: Parent execution exec-1 not found",
        total_iterations: 0,
      },
    });

    await expect(promise).resolves.toMatchObject([
      {
        event_type: "ExecutionFailed",
        execution_id: "exec-3",
        reason: "Failed to start execution: Parent execution exec-1 not found",
        total_iterations: 0,
      },
    ]);

    expect(mocks.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "ExecutionFailed",
        event_count: 1,
        execution_id: "exec-3",
        reason: "Failed to start execution: Parent execution exec-1 not found",
      }),
      "Agent execution failed",
    );
  });

  it("rejects when the runtime stream errors before completion", async () => {
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    call.emit("error", new Error("stream failed"));

    await expect(promise).rejects.toThrow("stream failed");
  });

  it("resolves from persisted completed state when the stream goes idle after non-terminal events", async () => {
    vi.useFakeTimers();
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);
    mocks.fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "Completed" }),
    });

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    call.emit("data", {
      event: "iteration_completed",
      iteration_completed: {
        execution_id: "exec-4",
        iteration_number: 1,
        output: "registered workflow",
        completed_at: "2026-03-22T08:32:12.572760Z",
      },
    });

    await vi.advanceTimersByTimeAsync(25);

    await expect(promise).resolves.toMatchObject([
      {
        event_type: "IterationCompleted",
        execution_id: "exec-4",
        output: "registered workflow",
      },
      {
        event_type: "ExecutionCompleted",
        execution_id: "exec-4",
        final_output: "registered workflow",
        total_iterations: 1,
      },
    ]);

    expect(mocks.fetchMock).toHaveBeenCalledWith(
      "http://orchestrator.test/v1/executions/exec-4",
      { headers: { Authorization: "Bearer test-token" } },
    );
    expect(mocks.logger.info).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "ExecutionCompleted",
        execution_id: "exec-4",
      }),
      "Agent execution resolved from persisted terminal state",
    );
  });

  it("resolves from persisted failed state after the stream ends without a terminal event", async () => {
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);
    mocks.fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "Failed" }),
    });

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    call.emit("data", {
      event: "iteration_failed",
      iteration_failed: {
        execution_id: "exec-5",
        iteration_number: 2,
        error: { message: "tool blew up" },
        failed_at: "2026-03-22T08:32:12.572760Z",
      },
    });
    call.emit("end");

    await expect(promise).resolves.toMatchObject([
      expect.objectContaining({
        event_type: "IterationFailed",
        execution_id: "exec-5",
        error_message: "tool blew up",
      }),
      expect.objectContaining({
        event_type: "ExecutionFailed",
        execution_id: "exec-5",
        reason: "tool blew up",
      }),
    ]);
  });

  it("logs the actual error message when fetch fails, not an empty object", async () => {
    vi.useFakeTimers();
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    // fetch rejects with a plain Error — before the fix this logged `error: {}`
    mocks.fetchMock.mockRejectedValue(new Error("ECONNREFUSED"));

    process.env.AEGIS_EXECUTION_FALLBACK_MAX_RETRIES = "2";

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    // Emit a non-terminal event so executionId is set, then end the stream
    call.emit("data", {
      event: "iteration_started",
      iteration_started: {
        execution_id: "exec-err",
        iteration_number: 1,
        started_at: "2026-04-09T00:00:00Z",
      },
    });
    call.emit("end");

    // Advance through all poll retries (2 retries with exponential backoff)
    await vi.advanceTimersByTimeAsync(200);

    const result = await promise;

    // Verify the error was logged as a readable string, not `{}`
    const warnCalls = mocks.logger.warn.mock.calls.filter(
      ([, msg]: [any, string]) =>
        msg === "Failed to fetch persisted execution status",
    );
    expect(warnCalls.length).toBeGreaterThanOrEqual(1);

    for (const [ctx] of warnCalls) {
      expect(ctx.error).toBe("ECONNREFUSED");
      expect(ctx.error).not.toEqual({});
    }

    // Verify we eventually settled with a failure (exhausted retries)
    const lastEvent = result[result.length - 1];
    expect(lastEvent.event_type).toBe("ExecutionFailed");
    expect(lastEvent.reason).toContain("polling exhausted");
  });

  it("stops polling after max retries and returns a failure result", async () => {
    vi.useFakeTimers();
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    // Persisted status always returns "running" (non-terminal)
    mocks.fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "Running" }),
    });

    process.env.AEGIS_EXECUTION_FALLBACK_MAX_RETRIES = "3";

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await callStarted();

    call.emit("data", {
      event: "iteration_completed",
      iteration_completed: {
        execution_id: "exec-bounded",
        iteration_number: 1,
        output: "partial",
        completed_at: "2026-04-09T00:00:00Z",
      },
    });
    call.emit("end");

    // Advance enough time for all retries + backoff to complete
    await vi.advanceTimersByTimeAsync(5000);

    const result = await promise;
    const lastEvent = result[result.length - 1];

    expect(lastEvent.event_type).toBe("ExecutionFailed");
    expect(lastEvent.reason).toContain("polling exhausted after 3 attempts");
    expect(lastEvent.execution_id).toBe("exec-bounded");

    // Verify fetch was called exactly 3 times (not indefinitely)
    expect(mocks.fetchMock).toHaveBeenCalledTimes(3);
  });
});

describe("AegisRuntimeClient.executeContainerRun", () => {
  beforeEach(() => {
    vi.resetModules();
    mocks.runtimeCtorMock.mockReset();
    for (const fn of Object.values(mocks.logger)) {
      fn.mockReset();
    }
  });

  it("returns duration_ms as a number, not a string (proto uint64 coercion)", async () => {
    const executeContainerRunMock = vi.fn();
    mocks.runtimeCtorMock.mockImplementation(function () {
      return {
        ExecuteContainerRun: executeContainerRunMock,
        close: mocks.closeMock,
        waitForReady: readyChannel,
      };
    });

    // Simulate the gRPC response with duration_ms as a number
    // (longs: Number ensures proto-loader returns uint64 as Number, not String)
    executeContainerRunMock.mockImplementation(
      (_req: any, _meta: any, cb: any) => {
        cb(null, {
          exit_code: 0,
          stdout: "ok",
          stderr: "",
          duration_ms: 1033,
          attempts: 1,
        });
      },
    );

    const { aegisRuntimeClient } = await import("./client.js");

    const result = await aegisRuntimeClient.executeContainerRun({
      execution_id: "exec-1",
      state_name: "build",
      name: "build-step",
      image: "node:20",
      command: ["echo", "hello"],
      env: {},
      workdir: "/app",
      volumes: [],
      resources: undefined,
      registry_credentials: undefined,
      shell: false,
      max_attempts: 1,
      security_context_name: undefined,
    });

    expect(result.duration_ms).toBe(1033);
    expect(typeof result.duration_ms).toBe("number");
  });
});

describe("AegisRuntimeClient's channel", () => {
  beforeEach(() => {
    vi.resetModules();
    mocks.runtimeCtorMock.mockReset();
    mocks.runtimeCtorMock.mockImplementation(function () {
      return {
        ExecuteAgent: mocks.executeAgentRpcMock,
        close: mocks.closeMock,
        waitForReady: mocks.waitForReadyMock,
      };
    });
    mocks.executeAgentRpcMock.mockReset();
    mocks.waitForReadyMock.mockReset();
    mocks.waitForReadyMock.mockImplementation(readyChannel);
    for (const fn of Object.values(mocks.logger)) {
      fn.mockReset();
    }
  });

  it("pings the orchestrator with or without calls, and bounds reconnection and re-resolution", async () => {
    await import("./client.js");

    expect(mocks.runtimeCtorMock).toHaveBeenCalledWith(
      "runtime:50051",
      "insecure-creds",
      {
        "grpc.keepalive_time_ms": 10_000,
        "grpc.keepalive_timeout_ms": 5_000,
        "grpc.keepalive_permit_without_calls": 1,
        "grpc.max_reconnect_backoff_ms": 5_000,
        "grpc.dns_min_time_between_resolutions_ms": 5_000,
      },
    );
  });

  it("fails a call as UNAVAILABLE, naming the method, when no connection comes within the connect timeout", async () => {
    mocks.waitForReadyMock.mockImplementation(
      (_deadline: number, callback: (error?: Error) => void) =>
        callback(new Error("Failed to connect before the deadline")),
    );
    const { aegisRuntimeClient } = await import("./client.js");

    const error = await aegisRuntimeClient
      .executeAgent({ agent_id: "a", input: "i", context_json: "{}" })
      .catch((e: unknown) => e);

    expect(error).toMatchObject({
      code: 14,
      message:
        "14 UNAVAILABLE: ExecuteAgent: no connection to the orchestrator at runtime:50051 within 60000 ms (Failed to connect before the deadline)",
    });
    expect(mocks.executeAgentRpcMock).not.toHaveBeenCalled();
    const [deadline] = mocks.waitForReadyMock.mock.calls[0];
    expect(deadline - Date.now()).toBeGreaterThan(59_000);
  });
});

describe("a settled executeAgent call", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.useRealTimers();
    mocks.runtimeCtorMock.mockReset();
    mocks.runtimeCtorMock.mockImplementation(function () {
      return {
        ExecuteAgent: mocks.executeAgentRpcMock,
        close: mocks.closeMock,
        waitForReady: mocks.waitForReadyMock,
      };
    });
    mocks.executeAgentRpcMock.mockReset();
    mocks.waitForReadyMock.mockReset();
    mocks.waitForReadyMock.mockImplementation(readyChannel);
    mocks.fetchMock.mockReset();
    for (const fn of Object.values(mocks.logger)) {
      fn.mockReset();
    }
    vi.stubGlobal("fetch", mocks.fetchMock);
    process.env.AEGIS_EXECUTION_FALLBACK_IDLE_MS = "10";
    process.env.AEGIS_EXECUTION_FALLBACK_POLL_MS = "10";
    process.env.AEGIS_EXECUTION_FALLBACK_MAX_RETRIES = "2";
    process.env.AEGIS_ORCHESTRATOR_URL = "http://orchestrator.test";
  });

  const lateTransportError = () =>
    Object.assign(
      new Error(
        "13 INTERNAL: Received RST_STREAM with code 2 triggered by internal client error: read EHOSTUNREACH",
      ),
      { code: 13 },
    );

  async function start() {
    const call = mockCall();
    mocks.executeAgentRpcMock.mockReturnValue(call);
    const { aegisRuntimeClient } = await import("./client.js");
    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });
    await vi.waitFor(() =>
      expect(mocks.executeAgentRpcMock).toHaveBeenCalled(),
    );
    return { call, promise };
  }

  it("is cancelled and swallows a later error when its terminal event settled it", async () => {
    const { call, promise } = await start();
    call.emit("data", {
      event: "execution_completed",
      execution_completed: { execution_id: "exec-t", final_output: "done" },
    });
    await promise;

    expect(call.cancel).toHaveBeenCalledTimes(1);
    expect(() => call.emit("error", lateTransportError())).not.toThrow();
  });

  it("is cancelled and swallows a later error when status polling settled it", async () => {
    mocks.fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "Running" }),
    });
    const { call, promise } = await start();
    call.emit("data", {
      event: "iteration_started",
      iteration_started: { execution_id: "exec-p", iteration_number: 1 },
    });
    const events = await promise;

    expect(events.at(-1)?.reason).toBe(
      "Execution status polling exhausted after 2 attempts",
    );
    expect(call.cancel).toHaveBeenCalledTimes(1);
    expect(() => call.emit("error", lateTransportError())).not.toThrow();
  });

  it("is cancelled and swallows a later error when its error path settled it", async () => {
    mocks.fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ status: "Running" }),
    });
    const { call, promise } = await start();
    call.emit("data", {
      event: "iteration_started",
      iteration_started: { execution_id: "exec-e", iteration_number: 1 },
    });
    call.emit("error", Object.assign(new Error("14 UNAVAILABLE"), { code: 14 }));
    await expect(promise).rejects.toThrow("14 UNAVAILABLE");

    expect(call.cancel).toHaveBeenCalledTimes(1);
    expect(() => call.emit("error", lateTransportError())).not.toThrow();
  });
});
