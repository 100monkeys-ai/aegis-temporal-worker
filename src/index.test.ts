import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  logger: {
    info: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
  },
}));

vi.mock("./logger.js", () => ({ logger: mocks.logger }));
vi.mock("./config.js", () => ({ config: {} }));
vi.mock("./database.js", () => ({
  database: { connect: vi.fn(), disconnect: vi.fn() },
}));
vi.mock("./server.js", () => ({ startServer: vi.fn() }));
vi.mock("./worker.js", () => ({ startWorker: vi.fn() }));
vi.mock("./observability/metrics.js", () => ({
  startMetricsServer: vi.fn(),
}));

describe("the worker's entrypoint", () => {
  it("logs an uncaught error with its gRPC call's name, and leaves the crash to Node", async () => {
    const events = [
      "uncaughtException",
      "uncaughtExceptionMonitor",
      "SIGINT",
      "SIGTERM",
    ] as const;
    const before = Object.fromEntries(
      events.map((e) => [e, process.listeners(e)]),
    );

    await import("./index.js");
    await vi.waitFor(() =>
      expect(mocks.logger.info).toHaveBeenCalledWith(
        "AEGIS Temporal Worker started successfully",
      ),
    );

    const added = (e: (typeof events)[number]) =>
      process.listeners(e).filter((l) => !before[e].includes(l));
    const monitors = added("uncaughtExceptionMonitor");
    try {
      // A monitor, and no handler: Node still ends the process.
      expect(monitors).toHaveLength(1);
      expect(added("uncaughtException")).toHaveLength(0);

      // The shape grpc-js gives a call's error: its stack, then "for call
      // at" and the stack of the code that made the call.
      const error = Object.assign(
        new Error(
          "13 INTERNAL: Received RST_STREAM with code 2 triggered by internal client error: read EHOSTUNREACH",
        ),
        { code: 13, details: "Received RST_STREAM with code 2" },
      );
      error.stack = [
        `Error: ${error.message}`,
        "    at callErrorFromStatus (/app/node_modules/@grpc/grpc-js/build/src/call.js:31:19)",
        "for call at",
        "    at ServiceClientImpl.makeServerStreamRequest (/app/node_modules/@grpc/grpc-js/build/src/client.js:340:32)",
        "    at file:///app/dist/grpc/client.js:233:38",
        "    at new Promise (<anonymous>)",
        "    at AegisRuntimeClient.executeAgent (file:///app/dist/grpc/client.js:226:16)",
      ].join("\n");

      (monitors[0] as (e: Error, origin: string) => void)(
        error,
        "uncaughtException",
      );

      expect(mocks.logger.fatal).toHaveBeenCalledWith(
        {
          err: error,
          origin: "uncaughtException",
          grpc_call: "executeAgent",
          grpc_code: 13,
        },
        "Uncaught exception; the worker exits",
      );
    } finally {
      for (const e of events) {
        for (const l of added(e)) process.off(e, l as () => void);
      }
    }
  });
});
