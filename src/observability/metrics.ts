/**
 * Prometheus metrics for the AEGIS Temporal Worker.
 *
 * Exposes a singleton prom-client Registry plus the canonical metric
 * instruments defined for the Temporal Worker by the platform metrics ADR
 * (port 9094). The metrics endpoint is served on a SEPARATE HTTP listener
 * from the workflow registration API, on port 9094, bound by default to
 * 127.0.0.1.
 *
 * Cardinality discipline: per-execution identifiers (tenant_id, workflow_id,
 * execution_id, agent_id, iteration_id) MUST NOT appear as labels on any
 * metric defined in this file.
 */

import http from "node:http";
import {
  Registry,
  collectDefaultMetrics,
  Counter,
  Histogram,
  Gauge,
} from "prom-client";
import { logger } from "../logger.js";

/** Singleton registry for all temporal-worker metrics. */
export const register = new Registry();

// Default Node.js process / runtime metrics (CPU, memory, event loop, etc.)
collectDefaultMetrics({ register });

/**
 * Total number of activity executions, labelled by activity TYPE name and
 * terminal outcome.
 */
export const activitiesTotal = new Counter({
  name: "aegis_temporal_worker_activities_total",
  help: "Total number of Temporal activity executions, by activity type and outcome.",
  labelNames: ["activity", "outcome"] as const,
  registers: [register],
});

/**
 * Wall-clock duration of activity executions, labelled by activity TYPE name.
 * Buckets cover the realistic range for AEGIS activities, which can span from
 * sub-second gRPC calls (publish event) to multi-minute LLM-driven agent runs.
 */
export const activityDurationSeconds = new Histogram({
  name: "aegis_temporal_worker_activity_duration_seconds",
  help: "Duration of Temporal activity executions in seconds, by activity type.",
  labelNames: ["activity"] as const,
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600],
  registers: [register],
});

/**
 * Total number of task-queue polls, labelled by queue and poll result
 * (e.g. "task_received", "empty", "error").
 */
export const pollsTotal = new Counter({
  name: "aegis_temporal_worker_polls_total",
  help: "Total number of Temporal task-queue polls, by queue and result.",
  labelNames: ["queue", "result"] as const,
  registers: [register],
});

/**
 * Number of currently active workflow executions on this worker.
 *
 * The Temporal Node SDK does not expose a public hook for workflow lifecycle
 * events on the worker, so this gauge is registered for forward compatibility
 * and stays at 0 until a hook becomes available. Use Temporal Server's own
 * metrics for authoritative active-workflow counts.
 */
export const activeWorkflows = new Gauge({
  name: "aegis_temporal_worker_active_workflows",
  help: "Number of currently active workflow executions on this worker.",
  registers: [register],
});

/**
 * Start the Prometheus metrics HTTP listener on a dedicated port (9094),
 * separate from the workflow registration Express server.
 *
 * Bind address is configurable via METRICS_BIND (default 127.0.0.1).
 * The port is fixed at 9094 per the platform metrics ADR.
 */
export function startMetricsServer(): http.Server {
  const bind = process.env.METRICS_BIND ?? "127.0.0.1";
  const port = 9094;

  const server = http.createServer(async (req, res) => {
    if (req.method === "GET" && req.url === "/metrics") {
      try {
        const body = await register.metrics();
        res.writeHead(200, { "Content-Type": register.contentType });
        res.end(body);
      } catch (err) {
        logger.error({ err }, "Failed to render Prometheus metrics");
        res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("internal error rendering metrics");
      }
      return;
    }
    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("not found");
  });

  server.listen(port, bind, () => {
    logger.info({ bind, port }, "Prometheus metrics listener started");
  });

  return server;
}

/**
 * Wrap an activity function so that every invocation is timed and counted
 * against the canonical `aegis_temporal_worker_activities_*` metrics. The
 * `activityType` label MUST be the activity's TYPE name (the function name
 * registered with the Temporal Worker), never a per-invocation identifier.
 */
export function instrumentActivity<
  F extends (...args: unknown[]) => Promise<unknown>,
>(activityType: string, fn: F): F {
  const wrapped = async (
    ...args: Parameters<F>
  ): Promise<Awaited<ReturnType<F>>> => {
    const endTimer = activityDurationSeconds.startTimer({
      activity: activityType,
    });
    try {
      const result = await fn(...args);
      activitiesTotal.inc({ activity: activityType, outcome: "success" });
      return result as Awaited<ReturnType<F>>;
    } catch (err) {
      activitiesTotal.inc({ activity: activityType, outcome: "failure" });
      throw err;
    } finally {
      endTimer();
    }
  };
  return wrapped as F;
}
