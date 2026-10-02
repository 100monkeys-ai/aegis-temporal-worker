/**
 * AEGIS Temporal Worker Configuration
 * Loads environment variables and provides typed config
 */

import dotenv from "dotenv";
import { z } from "zod";

dotenv.config();

const configSchema = z.object({
  temporal: z.object({
    address: z.string().default("localhost:7233"),
    namespace: z.string().default("default"),
    taskQueue: z.string().default("aegis-agents"),
  }),
  database: z.object({
    url: z.string(),
  }),
  grpc: z.object({
    runtimeUrl: z.string().default("localhost:50051"),
    // The channel to the orchestrator. A replaced orchestrator pod leaves the
    // old connection silent (no FIN, no RST): only an HTTP/2 PING that goes
    // unanswered for keepaliveTimeoutMs reveals it, keepaliveTimeMs after the
    // last answered one, with or without calls in flight.
    keepaliveTimeMs: z.coerce.number().int().positive().default(10_000),
    keepaliveTimeoutMs: z.coerce.number().int().positive().default(5_000),
    // The longest wait between two reconnection attempts, and between two
    // lookups of runtimeUrl's name, while the orchestrator is unreachable
    // (grpc-js defaults: 120 s and 30 s).
    maxReconnectBackoffMs: z.coerce.number().int().positive().default(5_000),
    dnsMinTimeBetweenResolutionsMs: z.coerce
      .number()
      .int()
      .positive()
      .default(5_000),
    // How long a call waits for a connection before it fails as UNAVAILABLE,
    // an error Temporal retries.
    connectTimeoutMs: z.coerce.number().int().positive().default(60_000),
  }),
  http: z.object({
    port: z.coerce.number().default(3000),
    host: z.string().default("0.0.0.0"),
  }),
  worker: z.object({
    maxConcurrentActivityTaskExecutions: z.coerce.number().default(100),
    maxConcurrentWorkflowTaskExecutions: z.coerce.number().default(100),
  }),
  logging: z.object({
    level: z.enum(["trace", "debug", "info", "warn", "error"]).default("info"),
  }),
  nodeEnv: z.enum(["development", "production", "test"]).default("development"),
  serviceAccount: z.object({
    keycloakHost: z.string(),
    realm: z.string(),
    clientId: z.string(),
    clientSecret: z.string(),
  }),
});

export type Config = z.infer<typeof configSchema>;

function loadConfig(): Config {
  return configSchema.parse({
    temporal: {
      address: process.env.TEMPORAL_ADDRESS,
      namespace: process.env.TEMPORAL_NAMESPACE,
      taskQueue: process.env.TEMPORAL_TASK_QUEUE,
    },
    database: {
      url: process.env.DATABASE_URL,
    },
    grpc: {
      runtimeUrl: process.env.AEGIS_RUNTIME_GRPC_URL,
      keepaliveTimeMs: process.env.AEGIS_RUNTIME_GRPC_KEEPALIVE_TIME_MS,
      keepaliveTimeoutMs: process.env.AEGIS_RUNTIME_GRPC_KEEPALIVE_TIMEOUT_MS,
      maxReconnectBackoffMs:
        process.env.AEGIS_RUNTIME_GRPC_MAX_RECONNECT_BACKOFF_MS,
      dnsMinTimeBetweenResolutionsMs:
        process.env.AEGIS_RUNTIME_GRPC_DNS_MIN_TIME_BETWEEN_RESOLUTIONS_MS,
      connectTimeoutMs: process.env.AEGIS_RUNTIME_GRPC_CONNECT_TIMEOUT_MS,
    },
    http: {
      port: process.env.HTTP_PORT,
      host: process.env.HTTP_HOST,
    },
    worker: {
      maxConcurrentActivityTaskExecutions:
        process.env.MAX_CONCURRENT_ACTIVITY_TASK_EXECUTIONS,
      maxConcurrentWorkflowTaskExecutions:
        process.env.MAX_CONCURRENT_WORKFLOW_TASK_EXECUTIONS,
    },
    logging: {
      level: process.env.LOG_LEVEL,
    },
    nodeEnv: process.env.NODE_ENV,
    serviceAccount: {
      keycloakHost: process.env.KEYCLOAK_HOST,
      realm: process.env.KEYCLOAK_SYSTEM_REALM,
      clientId: process.env.KEYCLOAK_CLIENT_ID,
      clientSecret: process.env.KEYCLOAK_CLIENT_SECRET,
    },
  });
}

let _config: Config | undefined;
export const config: Config = new Proxy({} as Config, {
  get(_target, prop) {
    if (!_config) _config = loadConfig();
    return (_config as any)[prop];
  },
});
