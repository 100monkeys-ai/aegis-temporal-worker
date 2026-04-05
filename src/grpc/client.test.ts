import { EventEmitter } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  executeAgentRpcMock: vi.fn(),
  fetchMock: vi.fn(),
  closeMock: vi.fn(),
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
    },
  },
}));

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
      };
    });

    for (const fn of Object.values(mocks.logger)) {
      fn.mockReset();
    }

    vi.stubGlobal("fetch", mocks.fetchMock);
    process.env.AEGIS_EXECUTION_FALLBACK_IDLE_MS = "10";
    process.env.AEGIS_EXECUTION_FALLBACK_POLL_MS = "10";
    process.env.AEGIS_ORCHESTRATOR_URL = "http://orchestrator.test";
  });

  it("resolves when the runtime stream ends after a terminal event", async () => {
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await Promise.resolve();

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
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
      parent_execution_id: "parent-1",
    });

    await Promise.resolve();

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
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await Promise.resolve();

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
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import("./client.js");

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: "agent-1",
      input: "plan",
      context_json: "{}",
      timeout_seconds: 300,
    });

    await Promise.resolve();

    call.emit("error", new Error("stream failed"));

    await expect(promise).rejects.toThrow("stream failed");
  });

  it("resolves from persisted completed state when the stream goes idle after non-terminal events", async () => {
    vi.useFakeTimers();
    const call = new EventEmitter();
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

    await Promise.resolve();

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
    const call = new EventEmitter();
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

    await Promise.resolve();

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
});
