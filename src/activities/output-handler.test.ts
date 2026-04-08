import { beforeEach, describe, expect, it, vi } from "vitest";

const invokeOutputHandlerMock = vi.hoisted(() => vi.fn());

vi.mock("../grpc/client.js", () => ({
  aegisRuntimeClient: {
    invokeOutputHandler: invokeOutputHandlerMock,
  },
}));

vi.mock("../logger.js", () => ({
  logger: {
    info: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
  },
}));

import { executeOutputHandlerActivity } from "./output-handler.js";

describe("executeOutputHandlerActivity", () => {
  beforeEach(() => {
    invokeOutputHandlerMock.mockReset();
  });

  it("calls invokeOutputHandler with the correct gRPC request fields", async () => {
    invokeOutputHandlerMock.mockResolvedValue({
      success: true,
      result: "webhook delivered",
      error: "",
    });

    await executeOutputHandlerActivity({
      executionId: "exec-1",
      tenantId: "tenant-abc",
      finalOutput: '{"status":"ok"}',
      handlerConfigJson: JSON.stringify({
        type: "webhook",
        url: "https://example.com/hook",
        required: true,
      }),
    });

    expect(invokeOutputHandlerMock).toHaveBeenCalledOnce();
    expect(invokeOutputHandlerMock).toHaveBeenCalledWith({
      execution_id: "exec-1",
      tenant_id: "tenant-abc",
      final_output: '{"status":"ok"}',
      handler_config_json: JSON.stringify({
        type: "webhook",
        url: "https://example.com/hook",
        required: true,
      }),
    });
  });

  it("throws when the gRPC response indicates failure", async () => {
    invokeOutputHandlerMock.mockResolvedValue({
      success: false,
      result: "",
      error: "webhook returned 503",
    });

    await expect(
      executeOutputHandlerActivity({
        executionId: "exec-2",
        tenantId: "tenant-abc",
        finalOutput: "output",
        handlerConfigJson: JSON.stringify({
          type: "webhook",
          url: "https://example.com/hook",
          required: true,
        }),
      }),
    ).rejects.toThrow(
      "Output handler failed for execution exec-2: webhook returned 503",
    );
  });

  it("throws when the gRPC call itself rejects", async () => {
    invokeOutputHandlerMock.mockRejectedValue(new Error("grpc unavailable"));

    await expect(
      executeOutputHandlerActivity({
        executionId: "exec-3",
        tenantId: "tenant-abc",
        finalOutput: "output",
        handlerConfigJson: JSON.stringify({
          type: "agent",
          agent_id: "a1",
          required: false,
        }),
      }),
    ).rejects.toThrow("grpc unavailable");
  });

  it("resolves without throwing on successful response", async () => {
    invokeOutputHandlerMock.mockResolvedValue({
      success: true,
      result: "agent output processed",
      error: "",
    });

    await expect(
      executeOutputHandlerActivity({
        executionId: "exec-4",
        tenantId: "tenant-xyz",
        finalOutput: "done",
        handlerConfigJson: JSON.stringify({
          type: "agent",
          agent_id: "a1",
          required: true,
        }),
      }),
    ).resolves.toBeUndefined();
  });
});
