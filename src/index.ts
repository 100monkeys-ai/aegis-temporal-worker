/**
 * Main entry point for AEGIS Temporal Worker
 * Starts both HTTP server (for workflow registration) and Temporal worker (for execution)
 */

import { logger } from "./logger.js";
import { config } from "./config.js";
import { database } from "./database.js";
import { startServer } from "./server.js";
import { startWorker } from "./worker.js";
import { startMetricsServer } from "./observability/metrics.js";

/**
 * The AegisRuntimeClient method that made a gRPC call, read from its error:
 * grpc-js appends the caller's stack after "for call at" to every call error
 * (@grpc/grpc-js src/call.ts, callErrorFromStatus).
 */
function grpcCallName(error: unknown): string | undefined {
  const stack = error instanceof Error ? (error.stack ?? "") : "";
  const callerStack = stack.split("\nfor call at\n")[1];
  return callerStack?.match(/AegisRuntimeClient\.(\w+)/)?.[1];
}

// Last resort, for an error no code handled (an 'error' event with no
// listener among them): log it with the call that raised it before the
// process ends. A monitor observes and does not handle, so Node still prints
// the error and exits with code 1, and the pod restarts the worker: a real
// crash stays a crash. The logger's buffered output is flushed on exit.
process.on("uncaughtExceptionMonitor", (error, origin) => {
  logger.fatal(
    {
      err: error,
      origin,
      grpc_call: grpcCallName(error),
      grpc_code: (error as { code?: unknown }).code,
    },
    "Uncaught exception; the worker exits",
  );
});

async function main() {
  logger.info("Starting AEGIS Temporal Worker...");
  logger.info({ config }, "Configuration loaded");

  try {
    // Connect to database
    await database.connect();

    // Start Prometheus metrics listener on its own port (9094) — separate
    // from the workflow registration API.
    startMetricsServer();

    // Start HTTP server for workflow registration
    startServer();

    // Start Temporal worker for workflow execution
    await startWorker();

    logger.info("AEGIS Temporal Worker started successfully");

    // Graceful shutdown handlers
    process.on("SIGINT", async () => {
      logger.info("SIGINT received, shutting down gracefully...");
      await database.disconnect();
      process.exit(0);
    });

    process.on("SIGTERM", async () => {
      logger.info("SIGTERM received, shutting down gracefully...");
      await database.disconnect();
      process.exit(0);
    });
  } catch (error) {
    logger.error({ error }, "Failed to start AEGIS Temporal Worker");
    process.exit(1);
  }
}

main();
