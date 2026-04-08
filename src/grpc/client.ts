/**
 * gRPC Client for AEGIS Runtime
 * Calls back to Rust ExecutionService, ValidationService, CortexService
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { config } from "../config.js";
import { logger } from "../logger.js";
import { getServiceToken } from "../auth/token-manager.js";
import type {
  ExecuteAgentRequest,
  ExecutionEvent,
  ExecuteSystemCommandRequest,
  ExecuteSystemCommandResponse,
  ValidateRequest,
  ValidateResponse,
  QueryCortexRequest,
  QueryCortexResponse,
  StoreCortexPatternRequest,
  StoreCortexPatternResponse,
  StoreTrajectoryPatternRequest,
  StoreTrajectoryPatternResponse,
  ExecuteContainerRunRequest,
  ExecuteContainerRunResponse,
  InvokeOutputHandlerRequest,
  InvokeOutputHandlerResponse,
} from "../types.js";

// Load protobuf definition
// In Docker: /app/aegis-proto/proto/aegis_runtime.proto
// In development (from repo root): ./aegis-proto/proto/aegis_runtime.proto
const PROTO_PATH =
  process.env.PROTO_PATH || "./aegis-proto/proto/aegis_runtime.proto";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});

const aegisProto = grpc.loadPackageDefinition(packageDefinition) as any;

const TERMINAL_EVENT_TYPES = new Set<ExecutionEvent["event_type"]>([
  "ExecutionCompleted",
  "ExecutionFailed",
]);

const TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled"]);

const EXECUTION_FALLBACK_IDLE_MS = Number.parseInt(
  process.env.AEGIS_EXECUTION_FALLBACK_IDLE_MS ?? "5000",
  10,
);

const EXECUTION_FALLBACK_POLL_MS = Number.parseInt(
  process.env.AEGIS_EXECUTION_FALLBACK_POLL_MS ?? "1000",
  10,
);

interface PersistedExecutionStatusResponse {
  status?: string;
  error?: string;
}

function executionStatusUrl(executionId: string): string {
  const orchestratorUrl =
    process.env.AEGIS_ORCHESTRATOR_URL || "http://localhost:8088";
  return `${orchestratorUrl}/v1/executions/${encodeURIComponent(executionId)}`;
}

async function fetchPersistedExecutionStatus(
  executionId: string,
): Promise<string | null> {
  const token = await getServiceToken();
  const resp = await fetch(executionStatusUrl(executionId), {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!resp.ok) {
    throw new Error(
      `Failed to fetch execution ${executionId} status (HTTP ${resp.status})`,
    );
  }

  const payload = (await resp.json()) as PersistedExecutionStatusResponse;
  if (payload.error) {
    throw new Error(payload.error);
  }

  return payload.status?.toLowerCase() ?? null;
}

function latestKnownOutput(events: ExecutionEvent[]): string | undefined {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i];
    if (event.final_output) {
      return event.final_output;
    }
    if (event.output) {
      return event.output;
    }
  }
  return undefined;
}

function latestFailureReason(events: ExecutionEvent[]): string | undefined {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i];
    if (event.reason) {
      return event.reason;
    }
    if (event.error_message) {
      return event.error_message;
    }
  }
  return undefined;
}

function highestIteration(events: ExecutionEvent[]): number {
  return events.reduce(
    (max, event) => Math.max(max, event.iteration_number ?? 0),
    0,
  );
}

function synthesizeTerminalEvent(
  status: string,
  executionId: string,
  events: ExecutionEvent[],
): ExecutionEvent | null {
  const timestamp = new Date().toISOString();
  const totalIterations = highestIteration(events);

  switch (status) {
    case "completed":
      return {
        event_type: "ExecutionCompleted",
        execution_id: executionId,
        timestamp,
        final_output: latestKnownOutput(events),
        total_iterations: totalIterations,
      };
    case "failed":
      return {
        event_type: "ExecutionFailed",
        execution_id: executionId,
        timestamp,
        reason:
          latestFailureReason(events) ??
          "Execution reached persisted failed state",
        total_iterations: totalIterations,
      };
    case "cancelled":
      return {
        event_type: "ExecutionFailed",
        execution_id: executionId,
        timestamp,
        reason:
          latestFailureReason(events) ??
          "Execution reached persisted cancelled state",
        total_iterations: totalIterations,
      };
    default:
      return null;
  }
}

// Create gRPC client
class AegisRuntimeClient {
  private client: any;

  constructor(serverAddress: string) {
    // Package name is aegis.runtime.v1
    this.client = new aegisProto.aegis.runtime.v1.AegisRuntime(
      serverAddress,
      grpc.credentials.createInsecure(),
    );
    logger.info({ server_address: serverAddress }, "gRPC client initialized");
  }

  /**
   * Execute an agent (streaming response for real-time events)
   */
  async executeAgent(request: ExecuteAgentRequest): Promise<ExecutionEvent[]> {
    const token = await getServiceToken();
    const metadata = new grpc.Metadata();
    metadata.add("authorization", `Bearer ${token}`);
    if (request.tenant_id) {
      metadata.add("x-tenant-id", request.tenant_id);
    }

    return new Promise((resolve, reject) => {
      const events: ExecutionEvent[] = [];
      let settled = false;
      let executionId: string | undefined;
      let idleTimer: ReturnType<typeof setTimeout> | undefined;
      let pollTimer: ReturnType<typeof setTimeout> | undefined;
      let fallbackPollInFlight = false;

      const call = this.client.ExecuteAgent(request, metadata);

      const cleanup = () => {
        if (idleTimer) {
          clearTimeout(idleTimer);
          idleTimer = undefined;
        }
        if (pollTimer) {
          clearTimeout(pollTimer);
          pollTimer = undefined;
        }
        call.removeAllListeners("data");
        call.removeAllListeners("end");
        call.removeAllListeners("error");
      };

      const settleWithEvents = (
        resolvedEvents: ExecutionEvent[],
        logMessage: string,
        logLevel: "info" | "error" = "info",
      ) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();

        const terminalEvent = resolvedEvents.at(-1);
        logger[logLevel](
          {
            event_type: terminalEvent?.event_type,
            event_count: resolvedEvents.length,
            execution_id: terminalEvent?.execution_id ?? executionId,
            reason: terminalEvent?.reason ?? undefined,
          },
          logMessage,
        );
        resolve(resolvedEvents);
      };

      const scheduleFallbackProbe = () => {
        if (settled || !executionId || idleTimer) {
          return;
        }

        idleTimer = setTimeout(() => {
          idleTimer = undefined;
          void probePersistedTerminalState("idle_timeout");
        }, EXECUTION_FALLBACK_IDLE_MS);
      };

      const scheduleNextPoll = () => {
        if (settled || !executionId || pollTimer) {
          return;
        }

        pollTimer = setTimeout(() => {
          pollTimer = undefined;
          void probePersistedTerminalState("poll_retry");
        }, EXECUTION_FALLBACK_POLL_MS);
      };

      const probePersistedTerminalState = async (trigger: string) => {
        if (settled || !executionId || fallbackPollInFlight) {
          return;
        }

        fallbackPollInFlight = true;

        try {
          const status = await fetchPersistedExecutionStatus(executionId);
          if (status && TERMINAL_STATUSES.has(status)) {
            const synthesizedEvent = synthesizeTerminalEvent(
              status,
              executionId,
              events,
            );

            if (synthesizedEvent) {
              events.push(synthesizedEvent);
              settleWithEvents(
                events,
                "Agent execution resolved from persisted terminal state",
                synthesizedEvent.event_type === "ExecutionFailed"
                  ? "error"
                  : "info",
              );
              return;
            }
          }

          logger.debug(
            { execution_id: executionId, status, trigger },
            "Persisted execution state not terminal yet",
          );
          scheduleNextPoll();
        } catch (error) {
          logger.warn(
            { error, execution_id: executionId, trigger },
            "Failed to fetch persisted execution status",
          );
          scheduleNextPoll();
        } finally {
          fallbackPollInFlight = false;
        }
      };

      call.on("data", (rawEvent: any) => {
        // The proto uses `oneof event` which @grpc/proto-loader decodes as:
        // { event: 'execution_completed', execution_completed: { ... } }
        // Map it to the flat ExecutionEvent interface expected by activities.
        const eventCase: string = rawEvent.event ?? "";
        const inner: any = rawEvent[eventCase] ?? {};

        // Convert snake_case oneof case name to PascalCase event_type
        // e.g. 'execution_completed' → 'ExecutionCompleted'
        const eventType = eventCase
          .split("_")
          .map((w: string) => w.charAt(0).toUpperCase() + w.slice(1))
          .join("") as ExecutionEvent["event_type"];

        const event: ExecutionEvent = {
          event_type: eventType,
          execution_id: inner.execution_id ?? "",
          timestamp:
            inner.started_at ??
            inner.completed_at ??
            inner.failed_at ??
            inner.applied_at ??
            new Date().toISOString(),
          iteration_number: inner.iteration_number,
          action: inner.action,
          output: inner.output,
          error_message: inner.error?.message,
          code_diff: inner.code_diff,
          final_output: inner.final_output,
          reason: inner.reason,
          total_iterations: inner.total_iterations,
        };

        logger.debug(
          { event_type: event.event_type },
          "Received execution event",
        );
        events.push(event);
        if (event.execution_id) {
          executionId = event.execution_id;
        }

        if (TERMINAL_EVENT_TYPES.has(event.event_type)) {
          settleWithEvents(
            events,
            event.event_type === "ExecutionFailed"
              ? "Agent execution failed"
              : "Agent execution reached terminal event",
            event.event_type === "ExecutionFailed" ? "error" : "info",
          );
          return;
        }

        if (idleTimer) {
          clearTimeout(idleTimer);
          idleTimer = undefined;
        }
        scheduleFallbackProbe();
      });

      call.on("end", () => {
        if (settled) {
          return;
        }
        void probePersistedTerminalState("stream_end");
      });

      call.on("error", (error: Error) => {
        if (settled) {
          return;
        }

        if (executionId) {
          logger.warn(
            { error, execution_id: executionId },
            "Agent execution stream errored before terminal event; checking persisted status",
          );
          void probePersistedTerminalState("stream_error").finally(() => {
            if (settled) {
              return;
            }

            settled = true;
            cleanup();
            logger.error({ error }, "Agent execution failed");
            reject(error);
          });
          return;
        }

        settled = true;
        cleanup();
        logger.error({ error }, "Agent execution failed");
        reject(error);
      });
    });
  }

  /**
   * Execute a system command
   */
  async executeSystemCommand(
    request: ExecuteSystemCommandRequest,
  ): Promise<ExecuteSystemCommandResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.ExecuteSystemCommand(
        request,
        meta,
        (error: Error | null, response: ExecuteSystemCommandResponse) => {
          if (error) {
            logger.error({ error }, "System command execution failed");
            reject(error);
          } else {
            logger.info(
              { exit_code: response.exit_code },
              "System command completed",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Validate output with judge agents
   */
  async validateWithJudges(
    request: ValidateRequest,
  ): Promise<ValidateResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.ValidateWithJudges(
        request,
        meta,
        (error: Error | null, response: ValidateResponse) => {
          if (error) {
            logger.error({ error }, "Validation with judges failed");
            reject(error);
          } else {
            logger.info(
              {
                score: response.score,
                confidence: response.confidence,
                binary_valid: response.binary_valid,
              },
              "Validation completed",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Query Cortex for patterns matching an error
   */
  async queryCortexPatterns(
    request: QueryCortexRequest,
  ): Promise<QueryCortexResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.QueryCortexPatterns(
        request,
        meta,
        (error: Error | null, response: QueryCortexResponse) => {
          if (error) {
            logger.error({ error }, "Cortex pattern query failed");
            reject(error);
          } else {
            logger.info(
              { pattern_count: response.patterns.length },
              "Cortex patterns retrieved",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Store a new pattern in Cortex
   */
  async storeCortexPattern(
    request: StoreCortexPatternRequest,
  ): Promise<StoreCortexPatternResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.StoreCortexPattern(
        request,
        meta,
        (error: Error | null, response: StoreCortexPatternResponse) => {
          if (error) {
            logger.error({ error }, "Cortex pattern storage failed");
            reject(error);
          } else {
            logger.info(
              { pattern_id: response.pattern_id },
              "Cortex pattern stored",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Store a trajectory pattern in Cortex (ADR-049 Pillar 2)
   */
  async storeTrajectoryPattern(
    request: StoreTrajectoryPatternRequest,
  ): Promise<StoreTrajectoryPatternResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.StoreTrajectoryPattern(
        request,
        meta,
        (error: Error | null, response: StoreTrajectoryPatternResponse) => {
          if (error) {
            logger.error({ error }, "Trajectory pattern storage failed");
            reject(error);
          } else {
            logger.info(
              { new_weight: response.new_weight },
              "Trajectory pattern stored",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Execute a deterministic container step without an LLM loop (ADR-050)
   */
  async executeContainerRun(
    request: ExecuteContainerRunRequest,
  ): Promise<ExecuteContainerRunResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.ExecuteContainerRun(
        request,
        meta,
        (error: Error | null, response: ExecuteContainerRunResponse) => {
          if (error) {
            logger.error(
              { error, state_name: request.state_name },
              "Container run execution failed",
            );
            reject(error);
          } else {
            logger.info(
              {
                exit_code: response.exit_code,
                attempts: response.attempts,
                state_name: request.state_name,
              },
              "Container run completed",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Create an ephemeral workspace volume on the runtime (ADR-087)
   */
  async createWorkspaceVolume(request: {
    workflow_execution_id: string;
    tenant_id: string;
    ttl_hours: number;
    size_limit_mb: number;
  }): Promise<{ volume_id: string; remote_path: string }> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.CreateWorkspaceVolume(
        request,
        meta,
        (error: Error | null, response: any) => {
          if (error) {
            logger.error(
              { error, workflow_execution_id: request.workflow_execution_id },
              "Workspace volume creation failed",
            );
            reject(error);
          } else {
            logger.info(
              { volume_id: response.volume_id },
              "Workspace volume created",
            );
            resolve({
              volume_id: response.volume_id,
              remote_path: response.remote_path,
            });
          }
        },
      );
    });
  }

  /**
   * Destroy a workspace volume on the runtime (ADR-087)
   */
  async destroyWorkspaceVolume(request: {
    volume_id: string;
    workflow_execution_id: string;
  }): Promise<void> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    return new Promise((resolve, reject) => {
      this.client.DestroyWorkspaceVolume(
        request,
        meta,
        (error: Error | null, response: any) => {
          if (error) {
            logger.error(
              { error, volume_id: request.volume_id },
              "Workspace volume destruction failed",
            );
            reject(error);
          } else {
            logger.info(
              { volume_id: request.volume_id },
              "Workspace volume destroyed",
            );
            resolve();
          }
        },
      );
    });
  }

  /**
   * Invoke an output handler on the orchestrator (ADR-103)
   */
  async invokeOutputHandler(
    request: InvokeOutputHandlerRequest,
  ): Promise<InvokeOutputHandlerResponse> {
    const token = await getServiceToken();
    const meta = new grpc.Metadata();
    meta.add("authorization", `Bearer ${token}`);
    if (request.tenant_id) {
      meta.add("x-tenant-id", request.tenant_id);
    }
    return new Promise((resolve, reject) => {
      this.client.InvokeOutputHandler(
        request,
        meta,
        (error: Error | null, response: InvokeOutputHandlerResponse) => {
          if (error) {
            logger.error(
              { error, execution_id: request.execution_id },
              "Output handler invocation failed",
            );
            reject(error);
          } else {
            logger.info(
              {
                execution_id: request.execution_id,
                success: response.success,
              },
              "Output handler invoked",
            );
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Close the gRPC connection
   */
  close(): void {
    this.client.close();
    logger.info("gRPC client closed");
  }
}

// Singleton instance
export const aegisRuntimeClient = new AegisRuntimeClient(
  config.grpc.runtimeUrl,
);

// Export for testing
export { AegisRuntimeClient };
