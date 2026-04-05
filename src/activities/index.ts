/**
 * Temporal Activities
 * Activities call back to Rust services via gRPC
 */

import { logger } from "../logger.js";
import { aegisRuntimeClient } from "../grpc/client.js";
import { getServiceToken } from "../auth/token-manager.js";
import type {
  ExecuteAgentRequest,
  ExecuteSystemCommandRequest,
  ValidateRequest,
  StoreTrajectoryPatternRequest,
  TrajectoryStep,
  Blackboard,
  ExecuteContainerRunRequest,
  ExecuteContainerRunResponse,
  ContainerRunConfig,
} from "../types.js";
import { fetchWorkflowDefinition } from "./workflow-activities.js";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Resolve an agent name to its UUID via the orchestrator REST API.
 * If the value is already a UUID it is returned as-is (fast path).
 * Phase 1c safety net: deploy-time resolution in register_workflow.rs (Phase 1a)
 * and gRPC-side lookup in server.rs (Phase 1b) handle most cases; this provides
 * a final defence for any path that bypasses those layers.
 */
async function resolveAgentId(
  nameOrId: string,
  tenantId?: string,
): Promise<string> {
  if (UUID_PATTERN.test(nameOrId)) {
    return nameOrId;
  }
  logger.warn(
    { agent_name: nameOrId },
    "agent_id is not a UUID — resolving via REST",
  );
  const orchestratorUrl =
    process.env.AEGIS_ORCHESTRATOR_URL || "http://localhost:8088";
  const url = `${orchestratorUrl}/v1/agents/lookup/${encodeURIComponent(nameOrId)}`;

  const MAX_RETRIES = 5;
  const BASE_DELAY_MS = 500;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const token = await getServiceToken();
    const headers: Record<string, string> = {
      Authorization: `Bearer ${token}`,
    };
    if (tenantId) headers["X-Tenant-Id"] = tenantId;
    const resp = await fetch(url, { headers });

    if (resp.ok) {
      const data = (await resp.json()) as { id: string };
      logger.info(
        { agent_name: nameOrId, resolved_id: data.id, attempt },
        "Resolved agent name to UUID",
      );
      return data.id;
    }

    if (resp.status === 404 && attempt < MAX_RETRIES) {
      const delay = BASE_DELAY_MS * Math.pow(2, attempt);
      logger.warn(
        { agent_name: nameOrId, attempt, delay_ms: delay },
        "Agent not yet visible in registry — retrying",
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      continue;
    }

    throw new Error(
      `Agent '${nameOrId}' not found (HTTP ${resp.status}). ` +
        `Deploy it with 'aegis agent deploy' before running this workflow.`,
    );
  }

  // Unreachable — loop always returns or throws — but satisfies TypeScript.
  throw new Error(
    `Agent '${nameOrId}' not found after ${MAX_RETRIES} retries.`,
  );
}

function normalizeAgentOutput(finalOutput: string | undefined): unknown {
  const rawOutput = finalOutput ?? "";
  const trimmedOutput = rawOutput.trim();

  // Strip markdown code fences (```json ... ``` or ``` ... ```)
  const stripped = trimmedOutput
    .replace(
      /^```(?:json|javascript|typescript|python|yaml|toml|bash|sh|text)?\s*\n?/i,
      "",
    )
    .replace(/\n?```\s*$/, "")
    .trim();

  if (
    (stripped.startsWith("{") && stripped.endsWith("}")) ||
    (stripped.startsWith("[") && stripped.endsWith("]"))
  ) {
    try {
      return JSON.parse(stripped);
    } catch {
      // Not valid JSON despite the shape — fall through to return stripped plain text.
    }
  }

  // Return stripped text (fence removed) so Handlebars gets clean content even for non-JSON output.
  return stripped || rawOutput;
}

/**
 * Execute an agent via Rust ExecutionService
 */
export async function executeAgentActivity(params: {
  agentId: string;
  input: string;
  context: Blackboard;
  workflowExecutionId?: string;
  parentExecutionId?: string;
  securityContextName?: string;
  workspaceVolumeId?: string;
  workspaceVolumeMountPath?: string;
}): Promise<any> {
  logger.info({ agent_id: params.agentId }, "Executing agent activity");

  const resolvedAgentId = await resolveAgentId(
    params.agentId,
    params.context?.tenant_id as string | undefined,
  );

  const request: ExecuteAgentRequest = {
    agent_id: resolvedAgentId,
    input: params.input,
    context_json: JSON.stringify(params.context),
    timeout_seconds: 600,
    tenant_id: params.context?.tenant_id as string | undefined,
  };

  if (params.workflowExecutionId) {
    request.workflow_execution_id = params.workflowExecutionId;
  }

  if (params.parentExecutionId) {
    request.parent_execution_id = params.parentExecutionId;
  }

  if (params.securityContextName) {
    request.security_context_name = params.securityContextName;
  }

  if (params.workspaceVolumeId) {
    request.workspace_volume_id = params.workspaceVolumeId;
    request.workspace_volume_mount_path =
      params.workspaceVolumeMountPath ?? "/workspace";
  }

  try {
    // Call Rust ExecutionService via gRPC (streaming)
    const events = await aegisRuntimeClient.executeAgent(request);

    // Extract final result from events
    const completedEvent = events.find(
      (e) => e.event_type === "ExecutionCompleted",
    );
    const failedEvent = events.find((e) => e.event_type === "ExecutionFailed");

    if (completedEvent) {
      return {
        status: "completed",
        output: normalizeAgentOutput(completedEvent.final_output),
        iterations: completedEvent.total_iterations || 0,
      };
    }

    if (failedEvent) {
      return {
        status: "failed",
        error: failedEvent.reason || "Unknown error",
        iterations: failedEvent.total_iterations || 0,
      };
    }

    throw new Error("No completion or failure event received");
  } catch (error) {
    logger.error(
      { error, agent_id: params.agentId },
      "Agent execution activity failed",
    );
    throw error;
  }
}

/**
 * Execute a system command
 */
export async function executeSystemCommandActivity(params: {
  command: string;
  env?: Record<string, string>;
  workdir?: string;
  timeout?: number;
}): Promise<any> {
  logger.info({ command: params.command }, "Executing system command activity");

  const request: ExecuteSystemCommandRequest = {
    command: params.command,
    env: params.env || {},
    workdir: params.workdir,
    timeout_seconds: params.timeout,
  };

  try {
    const response = await aegisRuntimeClient.executeSystemCommand(request);

    return {
      status: response.exit_code === 0 ? "success" : "failed",
      exit_code: response.exit_code,
      stdout: response.stdout,
      stderr: response.stderr,
    };
  } catch (error) {
    logger.error(
      { error, command: params.command },
      "System command activity failed",
    );
    throw error;
  }
}

/**
 * Validate output with judge agents
 */
export async function validateOutputActivity(params: {
  output: string;
  task?: string;
  judges: Array<{ agent_id: string; weight?: number; input_template?: string }>;
  consensus_strategy?: string;
  consensus_threshold?: number;
  context_json?: string;
  securityContextName?: string;
}): Promise<any> {
  logger.info(
    { judge_count: params.judges.length },
    "Validating output with judges",
  );

  const request: ValidateRequest = {
    output: params.output,
    task: params.task,
    judges: params.judges.map((j) => ({
      agent_id: j.agent_id,
      weight: j.weight,
      input_template: j.input_template,
    })),
    consensus: params.consensus_strategy
      ? {
          strategy: params.consensus_strategy,
          threshold: params.consensus_threshold ?? 0.8,
        }
      : undefined,
    context_json: params.context_json,
    security_context_name: params.securityContextName,
  };

  try {
    const response = await aegisRuntimeClient.validateWithJudges(request);

    return {
      score: response.score,
      confidence: response.confidence,
      binary_valid: response.binary_valid,
      individual_results: response.individual_results,
      reasoning: response.reasoning,
    };
  } catch (error) {
    logger.error({ error }, "Validation activity failed");
    throw error;
  }
}

/**
 * Execute multiple agents in parallel
 */
export async function executeParallelAgentsActivity(params: {
  agents: Array<{ agent: string; input: string; weight?: number }>;
  /** External judge agents from the state's `judges_for_parallel` field (ADR-016). */
  judges?: Array<{
    agent_id: string;
    weight?: number;
    input_template?: string;
  }>;
  consensus: {
    strategy: string;
    threshold: number;
  };
  securityContextName?: string;
  tenantId?: string;
}): Promise<any> {
  logger.info(
    { agent_count: params.agents.length },
    "Executing parallel agents",
  );

  try {
    // Execute all agents in parallel
    const results = await Promise.all(
      params.agents.map(async (agentConfig) => {
        const resolvedAgent = await resolveAgentId(
          agentConfig.agent,
          params.tenantId,
        );
        const events = await aegisRuntimeClient.executeAgent({
          agent_id: resolvedAgent,
          input: agentConfig.input,
          context_json: JSON.stringify({ input: agentConfig.input }),
          timeout_seconds: 600,
          security_context_name: params.securityContextName,
          tenant_id: params.tenantId,
        });

        // Extract output
        const completedEvent = events.find(
          (e) => e.event_type === "ExecutionCompleted",
        );
        if (completedEvent) {
          return {
            output: completedEvent.final_output || "",
            agent: agentConfig.agent,
            weight: agentConfig.weight || 1.0,
          };
        }

        throw new Error(
          `Agent ${agentConfig.agent} did not complete successfully`,
        );
      }),
    );

    // All agents completed – validate the combined output with dedicated judge agents.
    // judges_for_parallel must be a *separate* set of agents from the workers above;
    // passing workers as their own judges would violate ADR-016 (agents cannot judge themselves).
    if (!params.judges || params.judges.length === 0) {
      // No external judges configured — return raw results without consensus scoring.
      return {
        consensus: {
          score: 1.0,
          confidence: 1.0,
          strategy: params.consensus.strategy,
          metadata: {
            individual_outputs: results.map((r) => r.output),
            individual_results: [],
            reasoning:
              "No judge agents configured for this ParallelAgents state",
          },
        },
      };
    }

    const outputsForValidation = results
      .map((r) => `[${r.agent}]:\n${r.output}`)
      .join("\n\n---\n\n");

    const validationResult = await aegisRuntimeClient.validateWithJudges({
      output: outputsForValidation,
      judges: params.judges,
      consensus: {
        strategy: params.consensus.strategy,
        threshold: params.consensus.threshold,
      },
      security_context_name: params.securityContextName,
    });

    return {
      consensus: {
        score: validationResult.score,
        confidence: validationResult.confidence,
        binary_valid: validationResult.binary_valid,
        strategy: params.consensus.strategy,
        metadata: {
          individual_outputs: results.map((r) => r.output),
          individual_results: validationResult.individual_results,
          reasoning: validationResult.reasoning,
        },
      },
    };
  } catch (error) {
    logger.error({ error }, "Parallel agents activity failed");
    throw error;
  }
}

/**
 * Store a successful trajectory in Cortex memory (ADR-049 Pillar 2)
 */
export async function storeTrajectoryPatternActivity(params: {
  taskSignature: string;
  steps: TrajectoryStep[];
  successScore: number;
}): Promise<any> {
  logger.info(
    { task_signature: params.taskSignature, step_count: params.steps.length },
    "Storing trajectory pattern",
  );

  const request: StoreTrajectoryPatternRequest = {
    task_signature: params.taskSignature,
    steps: params.steps,
    success_score: params.successScore,
  };

  try {
    const response = await aegisRuntimeClient.storeTrajectoryPattern(request);
    return {
      trajectory_id: response.trajectory_id,
      new_weight: response.new_weight,
      deduplicated: response.deduplicated,
    };
  } catch (error) {
    // Cortex storage failure must never crash the workflow — log and swallow.
    logger.warn(
      { error, task_signature: params.taskSignature },
      "Trajectory pattern storage failed (non-fatal)",
    );
    return null;
  }
}

import { publishEventActivity } from "./event-activities.js";

/**
 * Create an ephemeral workspace volume for a workflow execution (ADR-087)
 */
export async function createEphemeralWorkspaceActivity(params: {
  execution_id: string;
  ttl_hours: number;
  tenant_id: string;
  size_limit_mb: number;
}): Promise<{ volume_id: string }> {
  logger.info(
    { execution_id: params.execution_id },
    "Creating ephemeral workspace volume",
  );
  const response = await aegisRuntimeClient.createWorkspaceVolume({
    workflow_execution_id: params.execution_id,
    tenant_id: params.tenant_id,
    ttl_hours: params.ttl_hours,
    size_limit_mb: params.size_limit_mb,
  });
  logger.info(
    { volume_id: response.volume_id },
    "Ephemeral workspace volume created",
  );
  return { volume_id: response.volume_id };
}

/**
 * Destroy a workspace volume after workflow completion (ADR-087)
 */
export async function destroyWorkspaceVolumeActivity(params: {
  volume_id: string;
  execution_id: string;
  tenant_id: string;
}): Promise<void> {
  logger.info({ volume_id: params.volume_id }, "Destroying workspace volume");
  await aegisRuntimeClient.destroyWorkspaceVolume({
    volume_id: params.volume_id,
    workflow_execution_id: params.execution_id,
  });
  logger.info({ volume_id: params.volume_id }, "Workspace volume destroyed");
}

/**
 * Execute a single deterministic container step without an LLM loop (ADR-050)
 *
 * Maps to the gRPC ExecuteContainerRun RPC on the Rust AegisRuntime service.
 * Retry logic is handled inside the Rust use case (RunContainerStepUseCase);
 * max_attempts controls how many times the orchestrator retries before surfacing failure.
 */
export async function executeContainerRunActivity(
  params: ExecuteContainerRunRequest,
): Promise<ExecuteContainerRunResponse> {
  logger.info(
    { state_name: params.state_name, image: params.image },
    "Executing container run activity",
  );

  try {
    const response = await aegisRuntimeClient.executeContainerRun(params);
    logger.info(
      {
        state_name: params.state_name,
        exit_code: response.exit_code,
        attempts: response.attempts,
      },
      "Container run activity completed",
    );
    return response;
  } catch (error) {
    logger.error(
      { error, state_name: params.state_name },
      "Container run activity failed",
    );
    throw error;
  }
}

/**
 * Execute multiple container steps concurrently within a single Temporal activity (ADR-050)
 *
 * Fans out via Promise.allSettled so all steps are attempted regardless of individual failures.
 * The completion strategy is then evaluated to determine the overall outcome:
 *   - all_succeed  — every step must exit with code 0; any non-zero exit throws
 *   - any_succeed  — at least one step must exit with code 0; all-failed throws
 *   - best_effort  — always resolves; per-step failures are surfaced in the output
 */
export async function executeParallelContainerRunActivity(params: {
  execution_id: string;
  state_name: string;
  steps: ContainerRunConfig[];
  completion: "all_succeed" | "any_succeed" | "best_effort";
  image_pull_policy?: string;
  securityContextName?: string;
}): Promise<{
  results: Array<{
    name: string;
    exit_code: number;
    stdout: string;
    stderr: string;
    duration_ms: number;
  }>;
  overall_success: boolean;
  succeeded: number;
  failed: number;
  completion: "all_succeed" | "any_succeed" | "best_effort";
}> {
  logger.info(
    {
      state_name: params.state_name,
      step_count: params.steps.length,
      completion: params.completion,
    },
    "Executing parallel container run activity",
  );

  const settled = await Promise.allSettled(
    params.steps.map(async (step) => {
      const request: ExecuteContainerRunRequest = {
        execution_id: params.execution_id,
        state_name: params.state_name,
        name: step.name,
        image: step.image,
        image_pull_policy: params.image_pull_policy,
        command: step.command,
        env: step.env,
        workdir: step.workdir,
        volumes: step.volumes,
        resources: step.resources,
        registry_credentials: step.registry_credentials,
        shell: step.shell ?? false,
        max_attempts: 1,
        security_context_name: params.securityContextName,
      };
      const response = await aegisRuntimeClient.executeContainerRun(request);
      return { name: step.name, ...response };
    }),
  );

  const results = settled.map((r, i) => {
    if (r.status === "fulfilled") {
      return r.value;
    }
    // Rejected promise — surface as a non-zero exit code so transition conditions can route on it
    logger.error(
      { step: params.steps[i].name, error: r.reason },
      "Parallel container step failed with exception",
    );
    return {
      name: params.steps[i].name,
      exit_code: 1,
      stdout: "",
      stderr: r.reason instanceof Error ? r.reason.message : String(r.reason),
      duration_ms: 0,
    };
  });

  const successCount = results.filter((r) => r.exit_code === 0).length;

  let overall_success: boolean;
  switch (params.completion) {
    case "all_succeed":
      overall_success = successCount === results.length;
      break;
    case "any_succeed":
      overall_success = successCount > 0;
      break;
    case "best_effort":
    default:
      overall_success = true;
      break;
  }

  logger.info(
    {
      state_name: params.state_name,
      total: results.length,
      succeeded: successCount,
      failed: results.length - successCount,
      completion: params.completion,
      overall_success,
    },
    "Parallel container run activity completed",
  );

  return {
    results,
    overall_success,
    succeeded: successCount,
    failed: results.length - successCount,
    completion: params.completion,
  };
}

// Ensure all activities are exported for Temporal Worker
export const activities = {
  executeAgentActivity,
  executeSystemCommandActivity,
  validateOutputActivity,
  executeParallelAgentsActivity,
  storeTrajectoryPatternActivity,
  fetchWorkflowDefinition,
  publishEventActivity,
  executeContainerRunActivity,
  executeParallelContainerRunActivity,
  createEphemeralWorkspaceActivity,
  destroyWorkspaceVolumeActivity,
};

export { fetchWorkflowDefinition, publishEventActivity };
