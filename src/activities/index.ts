/**
 * Temporal Activities
 * Activities call back to Rust services via gRPC
 */

import { ApplicationFailure, Context } from "@temporalio/activity";
import { logger } from "../logger.js";
import { aegisRuntimeClient, cancelAgentExecution } from "../grpc/client.js";
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

    if (resp.status === 403) {
      throw new Error(
        `Agent '${nameOrId}' lookup returned HTTP 403 (Forbidden). ` +
          `The Temporal worker service account may lack the 'agent:read' scope. ` +
          `Check Keycloak client configuration.`,
      );
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
 * The score an agent's normalized final output carries: its `score` when the
 * output is a JSON object whose `score` is a number from 0.0 to 1.0.
 * Undefined otherwise; a missing score is never read as 0.
 */
function agentOutputScore(output: unknown): number | undefined {
  if (!output || typeof output !== "object" || Array.isArray(output)) {
    return undefined;
  }
  return unitInterval((output as Record<string, unknown>).score);
}

/**
 * The orchestrator's bound on an agent run when its manifest names none
 * (`DEFAULT_EXECUTION_TIMEOUT_SECONDS`, `orchestrator/core/src/domain/supervisor.rs`).
 */
export const DEFAULT_AGENT_RUN_LIMIT_SECONDS = 1800;

/**
 * An agent manifest's `spec.security.resources.timeout` in seconds, read as
 * the orchestrator reads it (`ResourceLimits::parse_timeout_seconds`): a whole
 * number of hours ("1h"), minutes ("20m") or seconds ("1200s", "1200").
 */
function parseRunTimeoutSeconds(raw: unknown): number | undefined {
  if (typeof raw !== "string") return undefined;
  const m = raw.trim().match(/^(\d+)\s*([hms]?)$/);
  if (!m) return undefined;
  const value = Number.parseInt(m[1], 10);
  if (m[2] === "h") return value * 3600;
  if (m[2] === "m") return value * 60;
  return value;
}

/**
 * The run limit of an agent, in seconds: its manifest's
 * `spec.security.resources.timeout`, else the orchestrator's 1,800 s. The
 * workflow sets the agent activity's own limit from it.
 */
export async function agentRunLimitSecondsActivity(params: {
  agentId: string;
  tenantId?: string;
}): Promise<number> {
  const agentId = await resolveAgentId(params.agentId, params.tenantId);
  const orchestratorUrl =
    process.env.AEGIS_ORCHESTRATOR_URL || "http://localhost:8088";
  const token = await getServiceToken();
  const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
  if (params.tenantId) headers["X-Tenant-Id"] = params.tenantId;
  const resp = await fetch(
    `${orchestratorUrl}/v1/agents/${encodeURIComponent(agentId)}`,
    { headers },
  );
  if (!resp.ok) {
    throw new Error(
      `Failed to read agent ${params.agentId} for its run limit (HTTP ${resp.status})`,
    );
  }
  const agent = (await resp.json()) as {
    manifest?: { spec?: { security?: { resources?: { timeout?: unknown } } } };
  };
  return (
    parseRunTimeoutSeconds(
      agent.manifest?.spec?.security?.resources?.timeout,
    ) ?? DEFAULT_AGENT_RUN_LIMIT_SECONDS
  );
}

/** How often the agent activity heartbeats while its run's stream lives. */
export const AGENT_RUN_HEARTBEAT_INTERVAL_MS = 30_000;

/** What an agent activity's heartbeat records about its run. */
interface AgentRunHeartbeat {
  run_begun: boolean;
  execution_id?: string;
}

function earlierAttemptRun(details: unknown): AgentRunHeartbeat | undefined {
  if (!details || typeof details !== "object") return undefined;
  const run = details as Partial<AgentRunHeartbeat>;
  if (run.run_begun !== true) return undefined;
  return {
    run_begun: true,
    execution_id:
      typeof run.execution_id === "string" && run.execution_id.length > 0
        ? run.execution_id
        : undefined,
  };
}

/** The current activity's context, or none when called outside an activity. */
function currentActivityContext(): Context | undefined {
  try {
    return Context.current();
  } catch {
    return undefined;
  }
}

/**
 * Ask the orchestrator to cancel a run this step started. A refused cancel is
 * logged and never fails the step.
 */
async function askToCancelRun(
  executionId: string,
  tenantId: string | undefined,
  why: string,
): Promise<void> {
  try {
    await cancelAgentExecution(executionId, tenantId);
    logger.warn(
      { execution_id: executionId, why },
      "Asked the orchestrator to cancel the agent run",
    );
  } catch (error) {
    logger.error(
      {
        execution_id: executionId,
        why,
        error: error instanceof Error ? error.message : String(error),
      },
      "The orchestrator did not cancel the agent run",
    );
  }
}

/**
 * Execute an agent via Rust ExecutionService.
 *
 * An agent run starts once per step: it may send mail or change pages, so a
 * second start is a second act. Once the run's stream has begun, a failure of
 * this activity is not retried, and an attempt that finds an earlier attempt's
 * run in its heartbeat details starts none. When the activity ends before its
 * run (cancelled, timed out), it asks the orchestrator to cancel that run. A
 * transport failure before the run has begun is retried by the retry policy.
 */
export async function executeAgentActivity(params: {
  agentId: string;
  input: string;
  intent?: string;
  context: Blackboard;
  tenantId?: string;
  workflowExecutionId?: string;
  parentExecutionId?: string;
  securityContextName?: string;
  workspaceVolumeId?: string;
  workspaceVolumeMountPath?: string;
  workspaceRemotePath?: string;
  temperature?: number;
  /** ADR-113: structured attachment refs carried on the dispatch. Forwarded
   * to the runtime as ExecuteAgentRequest.attachments so the agent can read
   * the files via the aegis.attachment.read tool. */
  attachments?: import("../types.js").AttachmentRef[];
}): Promise<any> {
  logger.info({ agent_id: params.agentId }, "Executing agent activity");

  const activity = currentActivityContext();
  const earlier = earlierAttemptRun(activity?.info.heartbeatDetails);
  if (earlier) {
    if (earlier.execution_id) {
      await askToCancelRun(
        earlier.execution_id,
        params.tenantId,
        "an earlier attempt of this step began it and ended before it",
      );
    }
    throw ApplicationFailure.nonRetryable(
      `An earlier attempt of this step began the agent run${earlier.execution_id ? ` ${earlier.execution_id}` : ""}; it is not started again.`,
      "AgentRunAlreadyBegun",
    );
  }

  const resolvedAgentId = await resolveAgentId(params.agentId, params.tenantId);

  const request: ExecuteAgentRequest = {
    agent_id: resolvedAgentId,
    input: params.input,
    context_json: JSON.stringify(params.context),
    tenant_id: params.tenantId,
  };

  if (params.intent) {
    request.intent = params.intent;
  }

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
  if (params.workspaceRemotePath) {
    request.workspace_remote_path = params.workspaceRemotePath;
  }
  if (params.temperature !== undefined) {
    request.temperature = params.temperature;
  }
  if (params.attachments && params.attachments.length > 0) {
    request.attachments = params.attachments;
  }

  const run: AgentRunHeartbeat = { run_begun: false };
  const beat = () => activity?.heartbeat({ ...run });
  const heartbeats = activity
    ? setInterval(beat, AGENT_RUN_HEARTBEAT_INTERVAL_MS)
    : undefined;

  try {
    // Call Rust ExecutionService via gRPC (streaming)
    const events = await aegisRuntimeClient.executeAgent(request, {
      onRunBegun: () => {
        run.run_begun = true;
        beat();
      },
      onRunId: (executionId) => {
        run.execution_id = executionId;
        beat();
      },
      signal: activity?.cancellationSignal,
      // The status polling's deadline is this activity's own limit plus 60 s.
      stepTimeoutMs: activity?.info.startToCloseTimeoutMs,
    });

    // Extract final result from events
    const completedEvent = events.find(
      (e) => e.event_type === "ExecutionCompleted",
    );
    const failedEvent = events.find((e) => e.event_type === "ExecutionFailed");

    if (completedEvent) {
      const output = normalizeAgentOutput(completedEvent.final_output);
      const score = agentOutputScore(output);
      return {
        status: "completed",
        output,
        iterations: completedEvent.total_iterations || 0,
        execution_id: completedEvent.execution_id || undefined,
        // The state carries its agent's score so that score transitions can
        // read it (AEGIS ADR-017, Update of 2026-10-02). An output that is
        // not a JSON object with a numeric score from 0.0 to 1.0 carries none.
        ...(score === undefined ? {} : { score }),
      };
    }

    if (failedEvent) {
      return {
        status: "failed",
        error: failedEvent.reason || "Unknown error",
        iterations: failedEvent.total_iterations || 0,
        execution_id: failedEvent.execution_id || undefined,
      };
    }

    throw new Error("No completion or failure event received");
  } catch (error) {
    logger.error(
      { error, agent_id: params.agentId, execution_id: run.execution_id },
      "Agent execution activity failed",
    );
    if (!run.run_begun) {
      throw error;
    }
    if (run.execution_id) {
      await askToCancelRun(
        run.execution_id,
        params.tenantId,
        "the activity ended before its run",
      );
    }
    if (activity?.cancellationSignal.aborted) {
      // Cancelled or timed out: the activity ends as cancelled, and a retry
      // finds this run in its heartbeat details and starts none.
      throw activity.cancellationSignal.reason ?? error;
    }
    throw ApplicationFailure.nonRetryable(
      `The agent run${run.execution_id ? ` ${run.execution_id}` : ""} began and its stream failed: ${error instanceof Error ? error.message : String(error)}`,
      "AgentRunInterrupted",
    );
  } finally {
    if (heartbeats) clearInterval(heartbeats);
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

/** The confidence of a verdict that states none: the orchestrator's
 * `ABSENT_CONFIDENCE` (aegis-orchestrator domain/validation.rs). */
const ABSENT_VERDICT_CONFIDENCE = 0.8;

interface AgentVerdict {
  agent: string;
  score: number;
  confidence: number;
  reasoning: string;
  weight: number;
}

function unitInterval(value: unknown): number | undefined {
  return typeof value === "number" && value >= 0 && value <= 1
    ? value
    : undefined;
}

/**
 * Read an agent's final output as a verdict, by the orchestrator's contract
 * (read_judge_verdict): the first fenced block when there is one, else the
 * whole output, as a JSON object whose `score` is a number from 0.0 to 1.0.
 * Undefined when the output is not a verdict.
 */
function readAgentVerdict(
  output: string,
): Omit<AgentVerdict, "agent" | "weight"> | undefined {
  const fenced = /```[a-zA-Z]*\s*\n?([\s\S]*?)```/.exec(output);
  const candidate = (fenced ? fenced[1] : output).trim();
  let value: unknown;
  try {
    value = JSON.parse(candidate);
  } catch {
    return undefined;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  const fields = value as Record<string, unknown>;
  const score = unitInterval(fields.score);
  if (score === undefined) return undefined;
  const confidence =
    fields.confidence === undefined || fields.confidence === null
      ? ABSENT_VERDICT_CONFIDENCE
      : unitInterval(fields.confidence);
  if (confidence === undefined) return undefined;
  const reasoning = ["reasoning", "feedback", "message"]
    .map((k) => fields[k])
    .find((v): v is string => typeof v === "string");
  return { score, confidence, reasoning: reasoning ?? "" };
}

/**
 * Combine verdicts by the state's strategy, as the orchestrator's
 * validation_service.rs combines its judges' verdicts.
 */
function combineVerdicts(
  verdicts: AgentVerdict[],
  strategy: string,
  threshold: number,
): { score: number; confidence: number } {
  const count = verdicts.length;
  const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
  switch (strategy) {
    case "majority_vote": {
      const pass = verdicts.filter((v) => v.score >= threshold).length;
      const fail = count - pass;
      const score = pass > fail ? 1.0 : fail > pass ? 0.0 : 0.5;
      const margin = Math.min(Math.abs(pass - fail) / count, 1.0);
      return {
        score,
        confidence:
          margin * 0.7 + mean(verdicts.map((v) => v.confidence)) * 0.3,
      };
    }
    case "unanimous": {
      const allPass = verdicts.every((v) => v.score >= threshold);
      return {
        score: allPass ? mean(verdicts.map((v) => v.score)) : 0.0,
        confidence: Math.min(...verdicts.map((v) => v.confidence)),
      };
    }
    case "best_of_n": {
      const top = [...verdicts].sort(
        (a, b) => b.score * b.confidence - a.score * a.confidence,
      );
      const totalWeight = top.reduce((a, v) => a + v.weight, 0);
      return {
        score:
          totalWeight > 0
            ? top.reduce((a, v) => a + v.score * v.weight, 0) / totalWeight
            : mean(top.map((v) => v.score)),
        confidence: mean(top.map((v) => v.confidence)),
      };
    }
    default: {
      // weighted_average
      const totalWeight = verdicts.reduce((a, v) => a + v.weight, 0);
      const weightOf = (v: AgentVerdict) =>
        totalWeight > 0 ? v.weight / totalWeight : 1 / count;
      const unweightedMean = mean(verdicts.map((v) => v.score));
      const variance = mean(
        verdicts.map((v) => (v.score - unweightedMean) ** 2),
      );
      const agreement = 1.0 - Math.min(variance / 0.25, 1.0);
      return {
        score: verdicts.reduce((a, v) => a + v.score * weightOf(v), 0),
        confidence:
          agreement * 0.7 +
          verdicts.reduce((a, v) => a + v.confidence * weightOf(v), 0) * 0.3,
      };
    }
  }
}

/**
 * The consensus of a ParallelAgents state without judges_for_parallel: the
 * agents' own verdicts combined by the state's strategy. With no verdict to
 * combine it carries no score and says so; it never reports a score of its own.
 */
function consensusOfAgentVerdicts(
  results: Array<{ output: string; agent: string; weight: number }>,
  consensus: { strategy: string; threshold: number },
): Record<string, unknown> {
  const verdicts: AgentVerdict[] = [];
  const withoutVerdict: string[] = [];
  for (const r of results) {
    const verdict = readAgentVerdict(r.output);
    if (verdict) {
      verdicts.push({ ...verdict, agent: r.agent, weight: r.weight });
    } else {
      withoutVerdict.push(r.agent);
    }
  }
  const metadata = {
    individual_outputs: results.map((r) => r.output),
    individual_results: verdicts.map((v) => ({
      agent: v.agent,
      score: v.score,
      confidence: v.confidence,
      reasoning: v.reasoning,
    })),
    agents_without_verdict: withoutVerdict,
  };
  if (verdicts.length === 0) {
    return {
      strategy: consensus.strategy,
      metadata: {
        ...metadata,
        reasoning:
          "No judge agents are configured for this ParallelAgents state and none of its agents returned a verdict (a JSON object with a score from 0.0 to 1.0): there is no score to combine.",
      },
    };
  }
  const combined = combineVerdicts(
    verdicts,
    consensus.strategy,
    consensus.threshold,
  );
  const missing =
    withoutVerdict.length > 0
      ? ` ${withoutVerdict.length} returned no verdict (${withoutVerdict.join(", ")}) and are not counted.`
      : "";
  return {
    score: combined.score,
    confidence: combined.confidence,
    strategy: consensus.strategy,
    metadata: {
      ...metadata,
      reasoning: `No judge agents are configured for this ParallelAgents state: the consensus combines the verdicts of ${verdicts.length} of its ${results.length} agents by ${consensus.strategy}.${missing}`,
    },
  };
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
      // No external judges: the agents' own verdicts are the scores to combine.
      return {
        consensus: consensusOfAgentVerdicts(results, params.consensus),
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
import { executeOutputHandlerActivity } from "./output-handler.js";
import { fireScheduleActivity } from "./schedule-activities.js";

/**
 * Create an ephemeral workspace volume for a workflow execution (ADR-087)
 */
export async function createEphemeralWorkspaceActivity(params: {
  execution_id: string;
  ttl_hours: number;
  tenant_id: string;
  size_limit_mb: number;
}): Promise<{ volume_id: string; remote_path: string | undefined }> {
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
    { volume_id: response.volume_id, remote_path: response.remote_path },
    "Ephemeral workspace volume created",
  );
  return { volume_id: response.volume_id, remote_path: response.remote_path };
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
  agentRunLimitSecondsActivity,
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
  executeOutputHandlerActivity,
  fireScheduleActivity,
};

export {
  fetchWorkflowDefinition,
  publishEventActivity,
  executeOutputHandlerActivity,
  fireScheduleActivity,
};
