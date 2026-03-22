/**
 * gRPC Client for AEGIS Runtime
 * Calls back to Rust ExecutionService, ValidationService, CortexService
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { config } from '../config.js';
import { logger } from '../logger.js';
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
} from '../types.js';

// Load protobuf definition
// In Docker: /app/aegis-proto/proto/aegis_runtime.proto
// In development (from repo root): ./aegis-proto/proto/aegis_runtime.proto
const PROTO_PATH = process.env.PROTO_PATH || './aegis-proto/proto/aegis_runtime.proto';

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const aegisProto = grpc.loadPackageDefinition(packageDefinition) as any;

// Create gRPC client
class AegisRuntimeClient {
  private client: any;

  constructor(serverAddress: string) {
    // Package name is aegis.runtime.v1
    this.client = new aegisProto.aegis.runtime.v1.AegisRuntime(
      serverAddress,
      grpc.credentials.createInsecure()
    );
    logger.info({ server_address: serverAddress }, 'gRPC client initialized');
  }

  /**
   * Execute an agent (streaming response for real-time events)
   */
  async executeAgent(request: ExecuteAgentRequest): Promise<ExecutionEvent[]> {
    return new Promise((resolve, reject) => {
      const events: ExecutionEvent[] = [];
      let settled = false;

      const call = this.client.ExecuteAgent(request);

      const cleanup = () => {
        call.removeAllListeners('data');
        call.removeAllListeners('end');
        call.removeAllListeners('error');
      };

      call.on('data', (rawEvent: any) => {
        // The proto uses `oneof event` which @grpc/proto-loader decodes as:
        // { event: 'execution_completed', execution_completed: { ... } }
        // Map it to the flat ExecutionEvent interface expected by activities.
        const eventCase: string = rawEvent.event ?? '';
        const inner: any = rawEvent[eventCase] ?? {};

        // Convert snake_case oneof case name to PascalCase event_type
        // e.g. 'execution_completed' → 'ExecutionCompleted'
        const eventType = eventCase
          .split('_')
          .map((w: string) => w.charAt(0).toUpperCase() + w.slice(1))
          .join('') as ExecutionEvent['event_type'];

        const event: ExecutionEvent = {
          event_type: eventType,
          execution_id: inner.execution_id ?? '',
          timestamp: inner.started_at ?? inner.completed_at ?? inner.failed_at ?? inner.applied_at ?? new Date().toISOString(),
          iteration_number: inner.iteration_number,
          action: inner.action,
          output: inner.output,
          error_message: inner.error?.message,
          code_diff: inner.code_diff,
          final_output: inner.final_output,
          reason: inner.reason,
          total_iterations: inner.total_iterations,
        };

        logger.debug({ event_type: event.event_type }, 'Received execution event');
        events.push(event);

        if (
          event.event_type === 'ExecutionCompleted' ||
          event.event_type === 'ExecutionFailed'
        ) {
          settled = true;
          cleanup();
          if (event.event_type === 'ExecutionFailed') {
            logger.error(
              {
                event_type: event.event_type,
                event_count: events.length,
                execution_id: event.execution_id,
                reason: event.reason ?? undefined,
              },
              'Agent execution failed'
            );
          } else {
            logger.info(
              {
                event_type: event.event_type,
                event_count: events.length,
                execution_id: event.execution_id,
              },
              'Agent execution reached terminal event'
            );
          }
          resolve(events);
        }
      });

      call.on('end', () => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        logger.info({ event_count: events.length }, 'Agent execution completed');
        resolve(events);
      });

      call.on('error', (error: Error) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        logger.error({ error }, 'Agent execution failed');
        reject(error);
      });
    });
  }

  /**
   * Execute a system command
   */
  async executeSystemCommand(request: ExecuteSystemCommandRequest): Promise<ExecuteSystemCommandResponse> {
    return new Promise((resolve, reject) => {
      this.client.ExecuteSystemCommand(request, (error: Error | null, response: ExecuteSystemCommandResponse) => {
        if (error) {
          logger.error({ error }, 'System command execution failed');
          reject(error);
        } else {
          logger.info({ exit_code: response.exit_code }, 'System command completed');
          resolve(response);
        }
      });
    });
  }

  /**
   * Validate output with judge agents
   */
  async validateWithJudges(request: ValidateRequest): Promise<ValidateResponse> {
    return new Promise((resolve, reject) => {
      this.client.ValidateWithJudges(request, (error: Error | null, response: ValidateResponse) => {
        if (error) {
          logger.error({ error }, 'Validation with judges failed');
          reject(error);
        } else {
          logger.info({ score: response.score, confidence: response.confidence, binary_valid: response.binary_valid }, 'Validation completed');
          resolve(response);
        }
      });
    });
  }

  /**
   * Query Cortex for patterns matching an error
   */
  async queryCortexPatterns(request: QueryCortexRequest): Promise<QueryCortexResponse> {
    return new Promise((resolve, reject) => {
      this.client.QueryCortexPatterns(request, (error: Error | null, response: QueryCortexResponse) => {
        if (error) {
          logger.error({ error }, 'Cortex pattern query failed');
          reject(error);
        } else {
          logger.info({ pattern_count: response.patterns.length }, 'Cortex patterns retrieved');
          resolve(response);
        }
      });
    });
  }

  /**
   * Store a new pattern in Cortex
   */
  async storeCortexPattern(request: StoreCortexPatternRequest): Promise<StoreCortexPatternResponse> {
    return new Promise((resolve, reject) => {
      this.client.StoreCortexPattern(request, (error: Error | null, response: StoreCortexPatternResponse) => {
        if (error) {
          logger.error({ error }, 'Cortex pattern storage failed');
          reject(error);
        } else {
          logger.info({ pattern_id: response.pattern_id }, 'Cortex pattern stored');
          resolve(response);
        }
      });
    });
  }

  /**
   * Store a trajectory pattern in Cortex (ADR-049 Pillar 2)
   */
  async storeTrajectoryPattern(request: StoreTrajectoryPatternRequest): Promise<StoreTrajectoryPatternResponse> {
    return new Promise((resolve, reject) => {
      this.client.StoreTrajectoryPattern(request, (error: Error | null, response: StoreTrajectoryPatternResponse) => {
        if (error) {
          logger.error({ error }, 'Trajectory pattern storage failed');
          reject(error);
        } else {
          logger.info({ new_weight: response.new_weight }, 'Trajectory pattern stored');
          resolve(response);
        }
      });
    });
  }

  /**
   * Execute a deterministic container step without an LLM loop (ADR-050)
   */
  async executeContainerRun(request: ExecuteContainerRunRequest): Promise<ExecuteContainerRunResponse> {
    return new Promise((resolve, reject) => {
      this.client.ExecuteContainerRun(request, (error: Error | null, response: ExecuteContainerRunResponse) => {
        if (error) {
          logger.error({ error, state_name: request.state_name }, 'Container run execution failed');
          reject(error);
        } else {
          logger.info(
            { exit_code: response.exit_code, attempts: response.attempts, state_name: request.state_name },
            'Container run completed'
          );
          resolve(response);
        }
      });
    });
  }

  /**
   * Close the gRPC connection
   */
  close(): void {
    this.client.close();
    logger.info('gRPC client closed');
  }
}

// Singleton instance
export const aegisRuntimeClient = new AegisRuntimeClient(config.grpc.runtimeUrl);

// Export for testing
export { AegisRuntimeClient };
