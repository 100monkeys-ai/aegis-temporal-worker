import * as workflow from "@temporalio/workflow";
import {
  proxyActivities,
  setHandler,
  defineSignal,
  condition,
  workflowInfo,
  uuid4,
} from "@temporalio/workflow";
import Handlebars from "handlebars";
import type {
  WorkflowResult,
  Blackboard,
  WorkflowState,
  TransitionRule,
  JudgeConfig,
} from "../types.js";
import * as activities from "../activities/index.js";

// Proxy activities
const agentActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 minutes",
  retry: {
    maximumAttempts: 3,
  },
});

const terminalAgentActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 minutes",
  retry: {
    maximumAttempts: 1,
  },
});

const {
  executeAgentActivity,
  executeSystemCommandActivity,
  validateOutputActivity,
  executeParallelAgentsActivity,
  storeTrajectoryPatternActivity,
  fetchWorkflowDefinition,
  publishEventActivity,
  executeContainerRunActivity,
  executeParallelContainerRunActivity,
  executeOutputHandlerActivity,
} = agentActivities;

const { executeAgentActivity: executeAgentTerminalActivity } =
  terminalAgentActivities;

const workspaceActivities = proxyActivities<{
  createEphemeralWorkspaceActivity: typeof import("../activities/index.js").createEphemeralWorkspaceActivity;
  destroyWorkspaceVolumeActivity: typeof import("../activities/index.js").destroyWorkspaceVolumeActivity;
}>({
  startToCloseTimeout: "2 minutes",
  retry: {
    maximumAttempts: 3,
  },
});

// Register Handlebars helpers (idempotent if registered multiple times in sandbox)
Handlebars.registerHelper("length", (value: any) => {
  if (Array.isArray(value)) return value.length;
  if (typeof value === "string") return value.length;
  if (typeof value === "object" && value !== null)
    return Object.keys(value).length;
  return 0;
});
Handlebars.registerHelper("upper", (str: string) => (str || "").toUpperCase());
Handlebars.registerHelper("lower", (str: string) => (str || "").toLowerCase());
Handlebars.registerHelper("trim", (str: string) => (str || "").trim());
// ADR-031: additional helpers for blackboard context hydration
Handlebars.registerHelper("json", (value: any) => JSON.stringify(value));
Handlebars.registerHelper("first_line", (str: string) =>
  (str || "").split("\n")[0].trim(),
);
Handlebars.registerHelper("default", (value: any, fallback: any) =>
  value !== null && value !== undefined && value !== "" ? value : fallback,
);

interface GenericWorkflowInput {
  workflow_id: string;
  input: Record<string, any>;
  /** ADR-092: Natural-language steering extracted from the caller's request. */
  intent?: string;
  blackboard?: Record<string, any>;
  /** Tenant slug derived from the caller's JWT. Threaded through all
   *  downstream gRPC calls for tenant-scoped isolation. */
  tenant_id?: string;
  /** Security context name for policy enforcement. Threaded through all
   *  downstream gRPC calls for security-context-scoped policy. */
  security_context_name?: string;
  /** When this workflow is invoked as a child, the parent's execution ID. */
  parent_execution_id?: string;
}

/**
 * AEGIS Generic Interpreter Workflow
 *
 * This workflow acts as an interpreter for AEGIS workflow definitions.
 * Instead of compiling definitions to creating TS code, it fetches the definition
 * at runtime and executes it step-by-step.
 */
export async function aegis_workflow(
  args: GenericWorkflowInput,
): Promise<WorkflowResult> {
  const {
    workflow_id,
    input,
    intent: argsIntent,
    blackboard: blackboardOverridesArg,
    tenant_id,
    security_context_name,
  } = args;
  let blackboardOverrides = blackboardOverridesArg;
  const info = workflowInfo();
  const executionId = info.workflowId; // In AEGIS, Temporal workflowId is the Execution UUID
  let temporalSequenceNumber = 1;

  let workflowId: string | undefined = workflow_id;

  // Register the humanInput signal at workflow root — MUST be before any await/activity
  // to satisfy Temporal's determinism requirements. A single signal registration covers
  // all Human states in the workflow. clearResponse() is called after each Human state
  // consumes the value so successive Human gates wait for distinct signals.
  const humanInputSignal = defineSignal<[string]>("humanInput");
  let humanResponse: string | null = null;
  setHandler(humanInputSignal, (response: string) => {
    humanResponse = response;
  });

  const humanSignal = {
    getResponse: () => humanResponse,
    clearResponse: () => {
      humanResponse = null;
    },
  };

  const emit = async (eventType: string, extra: any = {}) => {
    await publishEventActivity({
      event_type: eventType,
      execution_id: executionId,
      temporal_sequence_number: temporalSequenceNumber++,
      workflow_id: workflowId,
      timestamp: new Date().toISOString(),
      ...extra,
    });
  };

  await emit("WorkflowExecutionStarted");

  // 1. Fetch Definition
  const definition = await fetchWorkflowDefinition(workflow_id);
  workflowId = definition.workflow_id;

  // ADR-087: Resolve tenant_id early — needed for workspace provisioning
  const resolvedTenantId = tenant_id || definition.tenant_id || "";

  // ADR-087: provision ephemeral workspace volume if spec.storage.workspace is declared
  let workspaceVolumeId: string | undefined;
  if (definition.spec_storage?.workspace?.storage_class === "ephemeral") {
    const ws = definition.spec_storage.workspace;
    const bbKey = ws.blackboard_key ?? "workspace_volume_id";
    const result = await workspaceActivities.createEphemeralWorkspaceActivity({
      execution_id: executionId,
      ttl_hours: ws.ttl_hours ?? 1,
      tenant_id: resolvedTenantId,
      size_limit_mb: ws.size_limit_mb ?? 256,
    });
    workspaceVolumeId = result.volume_id;
    const workspaceRemotePath: string | undefined = result.remote_path;
    blackboardOverrides = {
      ...(blackboardOverrides ?? {}),
      [bbKey]: workspaceVolumeId,
      [`${bbKey}_remote_path`]: workspaceRemotePath,
    };
  } else if (
    definition.spec_storage?.workspace?.storage_class === "persistent" &&
    definition.spec_storage.workspace.volume_id
  ) {
    const ws = definition.spec_storage.workspace;
    const bbKey = ws.blackboard_key ?? "workspace_volume_id";
    blackboardOverrides = {
      ...(blackboardOverrides ?? {}),
      [bbKey]: ws.volume_id,
    };
  }

  // 2. Initialize Blackboard (ADR-092: intent + input namespace)
  const blackboard: Blackboard = {
    ...definition.context,
    ...(blackboardOverrides ?? {}),
    tenant_id: resolvedTenantId,
    intent: argsIntent ?? "",
    input: input,
    workflow: {
      name: definition.name,
      version: definition.version,
      context: definition.context ?? {},
      storage: definition.spec_storage ?? {},
    },
  };

  // 3. Execution Loop
  let currentState: string | null = definition.initial_state;
  let iterationCount = 0;
  const maxIterations = 1000;

  while (currentState !== null && iterationCount < maxIterations) {
    iterationCount++;
    const state = definition.states[currentState];

    if (!state) {
      const err = `State "${currentState}" not found in definition`;
      await emit("WorkflowExecutionFailed", { error: err });
      throw new Error(err);
    }

    try {
      await emit("WorkflowStateEntered", { state_name: currentState });

      // Execute State
      const stateOutput = await executeState(
        state,
        currentState,
        blackboard,
        emit,
        executionId,
        humanSignal,
        security_context_name,
      );

      await emit("WorkflowStateExited", {
        state_name: currentState,
        output: stateOutput,
      });

      // Update Blackboard — state handlers already return structured objects
      // with `status` at root and `output` nested, so store directly.
      blackboard[currentState] = stateOutput;

      // Check Terminal
      if (!state.transitions || state.transitions.length === 0) {
        // ADR-087: destroy ephemeral workspace volume on terminal state
        if (workspaceVolumeId) {
          try {
            await workspaceActivities.destroyWorkspaceVolumeActivity({
              volume_id: workspaceVolumeId,
              execution_id: executionId,
              tenant_id: resolvedTenantId,
            });
          } catch (err) {
            workflow.log.warn(
              "Failed to destroy workspace volume; TTL will clean up",
            );
          }
        }
        await emit("WorkflowExecutionCompleted", {
          final_blackboard: blackboard,
        });
        return {
          status: "completed",
          output: stateOutput,
          iterations: iterationCount,
          final_state: currentState,
          blackboard,
        };
      }

      // Transition
      currentState = await evaluateTransitions(
        state.transitions,
        stateOutput,
        blackboard,
      );

      if (currentState === null) {
        // ADR-087: destroy ephemeral workspace volume on terminal state
        if (workspaceVolumeId) {
          try {
            await workspaceActivities.destroyWorkspaceVolumeActivity({
              volume_id: workspaceVolumeId,
              execution_id: executionId,
              tenant_id: resolvedTenantId,
            });
          } catch (err) {
            workflow.log.warn(
              "Failed to destroy workspace volume; TTL will clean up",
            );
          }
        }
        await emit("WorkflowExecutionCompleted", {
          final_blackboard: blackboard,
        });
        return {
          status: "completed",
          output: stateOutput,
          iterations: iterationCount,
          final_state: currentState ?? undefined,
          blackboard,
        };
      }
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      // ADR-087: destroy ephemeral workspace volume on failure
      if (workspaceVolumeId) {
        try {
          await workspaceActivities.destroyWorkspaceVolumeActivity({
            volume_id: workspaceVolumeId,
            execution_id: executionId,
            tenant_id: resolvedTenantId,
          });
        } catch (cleanupErr) {
          workflow.log.warn(
            "Failed to destroy workspace volume; TTL will clean up",
          );
        }
      }
      await emit("WorkflowExecutionFailed", {
        error: errMsg,
        final_blackboard: blackboard,
      });
      return {
        status: "failed",
        error: errMsg,
        iterations: iterationCount,
        final_state: currentState ?? undefined,
        blackboard,
      };
    }
  }

  const err = "Max iterations exceeded";
  // ADR-087: destroy ephemeral workspace volume on max iterations exceeded
  if (workspaceVolumeId) {
    try {
      await workspaceActivities.destroyWorkspaceVolumeActivity({
        volume_id: workspaceVolumeId,
        execution_id: executionId,
        tenant_id: resolvedTenantId,
      });
    } catch (cleanupErr) {
      workflow.log.warn(
        "Failed to destroy workspace volume; TTL will clean up",
      );
    }
  }
  await emit("WorkflowExecutionFailed", {
    error: err,
    final_blackboard: blackboard,
  });
  return {
    status: "failed",
    error: err,
    iterations: iterationCount,
    final_state: currentState ?? undefined,
    blackboard,
  };
}

// ----------------------------------------------------------------------------
// Helper Functions (Same logic as workflow-generator.ts but adapted for static context)
// ----------------------------------------------------------------------------

async function executeState(
  state: WorkflowState,
  stateName: string,
  blackboard: Blackboard,
  emit: (eventType: string, extra?: any) => Promise<void>,
  executionId: string,
  humanSignal: { getResponse: () => string | null; clearResponse: () => void },
  securityContextName?: string,
): Promise<any> {
  switch (state.kind) {
    case "Agent":
      if (!state.agent || !state.input)
        throw new Error("Invalid Agent State: missing agent or input");

      let iteration = 1;
      let currentInput = renderTemplate(state.input, blackboard);
      // Resolve per-state intent: render state.intent template if present,
      // otherwise fall back to the workflow-level intent from the blackboard (ADR-092).
      const resolvedIntent: string = state.intent
        ? renderTemplate(state.intent, blackboard)
        : typeof blackboard.intent === "string"
          ? blackboard.intent
          : "";
      const resolvedAgent = renderTemplate(state.agent, blackboard);
      if (!resolvedAgent || resolvedAgent.trim() === "") {
        throw new Error(
          `Template resolution failed: "${state.agent}" resolved to empty string. Check blackboard keys.`,
        );
      }
      // Iteration bound: state config > workflow-level context > default of 10
      const maxIterations: number = state.max_iterations ?? 10;

      // Judge agents come from the state YAML declaration (ADR-016/017).
      // Falling back to a blackboard key is an anti-pattern and is explicitly removed.
      const judges: JudgeConfig[] = state.judges || [];

      // Trajectory accumulator for Cortex (ADR-049 Pillar 2).
      // tool_name = the agent id acting as the "tool"; arguments_json = serialized output.
      const trajectorySteps: Array<{
        tool_name: string;
        arguments_json: string;
      }> = [];
      let lastOutput = null;

      // ADR-049 Pillar 1 — Pre-execution semantic validation
      // Run the rendered prompt through the pre-execution validator *before* spinning
      // up the agent container.  A score that results in binary_valid === false
      // short-circuits the entire state with a graceful failure so the Workflow can
      // route to a fallback state rather than burning a full iteration budget on a
      // plan the validator already knows will fail.
      if (state.pre_execution_validator) {
        const preValResult = await validateOutputActivity({
          output: currentInput,
          task: `Pre-execution plan validation for workflow state: ${stateName}`,
          judges: [{ agent_id: state.pre_execution_validator, weight: 1.0 }],
          context_json: JSON.stringify({
            execution_id: executionId,
            state_name: stateName,
          }),
          securityContextName,
        });
        if (preValResult.binary_valid === false) {
          await emit("WorkflowIterationFailed", {
            iteration_number: 0,
            error: `Pre-execution validator rejected plan: ${preValResult.reasoning}`,
          });
          return {
            status: "failed" as const,
            error: `Pre-execution validator (${state.pre_execution_validator}) rejected plan: ${preValResult.reasoning}`,
            pre_validation_score: preValResult.score,
          };
        }
      }

      while (iteration <= maxIterations) {
        await emit("WorkflowIterationStarted", { iteration_number: iteration });

        try {
          const isTerminalValidationAgent =
            resolvedAgent === "workflow-creator-validator-agent" ||
            stateName === "GENERATE_AND_REGISTER_WORKFLOW";

          const workspaceVolumeId =
            typeof blackboard.workspace_volume_id === "string" &&
            blackboard.workspace_volume_id.length > 0
              ? blackboard.workspace_volume_id
              : undefined;

          const workspaceRemotePath =
            typeof blackboard.workspace_volume_id_remote_path === "string" &&
            blackboard.workspace_volume_id_remote_path.length > 0
              ? blackboard.workspace_volume_id_remote_path
              : undefined;

          const result = await (
            isTerminalValidationAgent
              ? executeAgentTerminalActivity
              : executeAgentActivity
          )({
            agentId: resolvedAgent,
            input: currentInput,
            intent: resolvedIntent,
            context: blackboard,
            tenantId: blackboard.tenant_id as string | undefined,
            workflowExecutionId: executionId,
            securityContextName: securityContextName,
            workspaceVolumeId,
            workspaceVolumeMountPath: workspaceVolumeId
              ? "/workspace"
              : undefined,
            workspaceRemotePath,
          });

          lastOutput = result;

          if (result.status !== "completed") {
            // Throw here — the catch block below emits WorkflowIterationFailed
            throw new Error(`Agent execution failed: ${result.error}`);
          }

          await emit("WorkflowIterationCompleted", {
            iteration_number: iteration,
            output: result.output ?? "",
          });

          if (judges.length === 0) {
            // No judges configured — treat first successful execution as valid.
            break;
          }

          const validationResult = await validateOutputActivity({
            output:
              typeof result.output === "string"
                ? result.output
                : JSON.stringify(result.output),
            task: currentInput,
            judges,
            consensus_strategy: state.consensus?.strategy,
            consensus_threshold: state.consensus?.threshold,
            // Pass execution context so Rust can link judge child executions
            context_json: JSON.stringify({ execution_id: executionId }),
            securityContextName,
          });

          const iterScore: number = validationResult.score ?? 0;
          trajectorySteps.push({
            tool_name: resolvedAgent,
            arguments_json: JSON.stringify({
              output: result.output,
              score: iterScore,
            }),
          });

          if (validationResult.binary_valid === true) {
            // Successful iteration — commit trajectory to Cortex.
            await storeTrajectoryPatternActivity({
              taskSignature: `${stateName}:${executionId}`,
              steps: trajectorySteps,
              successScore: iterScore,
            });
            break;
          }

          if (iteration >= maxIterations) {
            // Exhausted iterations without passing validation.
            break;
          }

          await emit("RefinementApplied", {
            iteration_number: iteration,
            code_diff: validationResult.reasoning,
            agent_id: resolvedAgent,
          });

          currentInput =
            currentInput +
            `\n\nValidation failed with score ${iterScore}.\nReasoning: ${validationResult.reasoning}\nPlease refine your response.`;
          iteration++;
        } catch (error) {
          const errMsg = error instanceof Error ? error.message : String(error);
          await emit("WorkflowIterationFailed", {
            iteration_number: iteration,
            error: errMsg,
          });
          throw error;
        }
      }

      if (state.output_handler) {
        // Use the agent execution ID when available so the output handler can
        // spawn a child execution linked to the correct parent. Falls back to
        // the workflow-level executionId for states without an agent execution.
        // Extract the agent execution ID from the last iteration result.
        // Empty string signals the orchestrator to spawn a standalone execution.
        const agentExecutionId =
          lastOutput &&
          typeof lastOutput === "object" &&
          lastOutput.execution_id
            ? (lastOutput.execution_id as string)
            : "";
        try {
          await executeOutputHandlerActivity({
            executionId: agentExecutionId,
            tenantId: (blackboard.tenant_id as string) ?? "",
            finalOutput:
              typeof lastOutput === "string"
                ? lastOutput
                : JSON.stringify(lastOutput),
            handlerConfigJson: JSON.stringify(state.output_handler),
          });
        } catch (err) {
          if (state.output_handler.required) {
            throw err;
          }
          // fire-and-forget: log and continue
          console.warn("Optional output handler failed:", err);
        }
      }

      return lastOutput;

    case "System":
      if (!state.command) throw new Error("Invalid System State");
      const env: Record<string, string> = {};
      if (state.env) {
        for (const [k, v] of Object.entries(state.env)) {
          env[k] = renderTemplate(String(v), blackboard);
        }
      }
      const resolvedCommand = renderTemplate(state.command, blackboard);
      if (!resolvedCommand || resolvedCommand.trim() === "") {
        throw new Error(
          `Template resolution failed: "${state.command}" resolved to empty string. Check blackboard keys.`,
        );
      }
      return await executeSystemCommandActivity({
        command: resolvedCommand,
        env,
        workdir: state.workdir
          ? renderTemplate(state.workdir, blackboard)
          : undefined,
        timeout: state.timeout ? parseTimeout(state.timeout) : undefined,
      });

    case "Human":
      if (!state.prompt) throw new Error("Invalid Human State");

      // Clear any previous signal response so this Human state receives a fresh one.
      humanSignal.clearResponse();

      await emit("HumanInputRequested", {
        prompt: state.prompt,
        default_response: state.default_response,
      });

      // Signal already registered at aegis_workflow() root — just wait for it.
      const timeout = state.timeout ? parseTimeout(state.timeout) : 3600;
      await condition(() => humanSignal.getResponse() !== null, timeout * 1000);

      const hr = humanSignal.getResponse();
      humanSignal.clearResponse();

      if (hr === null) {
        if (state.default_response)
          return { response: state.default_response, timeout: true };
        throw new Error("Human input timeout");
      }
      return { response: hr, timeout: false };

    case "ParallelAgents": {
      if (!state.agents || !state.consensus)
        throw new Error("Invalid ParallelAgents State");
      const agentConfigs = state.agents.map((a) => ({
        agent: renderTemplate(a.agent, blackboard),
        input: renderTemplate(a.input, blackboard),
        weight: a.weight,
      }));
      const parallelResult = await executeParallelAgentsActivity({
        agents: agentConfigs,
        judges: state.judges_for_parallel,
        consensus: {
          strategy: state.consensus.strategy,
          threshold: state.consensus.threshold ?? 0.7,
        },
        securityContextName,
        tenantId: blackboard.tenant_id as string | undefined,
      });

      if (state.output_handler) {
        // ParallelAgents has no single agent execution — pass empty string
        // so the orchestrator spawns a standalone output handler execution.
        try {
          await executeOutputHandlerActivity({
            executionId: "",
            tenantId: (blackboard.tenant_id as string) ?? "",
            finalOutput:
              typeof parallelResult === "string"
                ? parallelResult
                : JSON.stringify(parallelResult),
            handlerConfigJson: JSON.stringify(state.output_handler),
          });
        } catch (err) {
          if (state.output_handler.required) {
            throw err;
          }
          // fire-and-forget: log and continue
          console.warn("Optional output handler failed:", err);
        }
      }

      return parallelResult;
    }

    case "ContainerRun": {
      if (!state.container_run_image || !state.container_run_command) {
        throw new Error(
          `Invalid ContainerRun state '${stateName}': missing image or command`,
        );
      }

      const renderedImage = renderTemplate(
        state.container_run_image,
        blackboard,
      );
      const renderedCommand = state.container_run_command
        .map((arg: string) => renderTemplate(arg, blackboard))
        .filter((arg: string) => arg !== "");
      const renderedName = state.container_run_name
        ? renderTemplate(state.container_run_name, blackboard)
        : stateName;
      const renderedWorkdir = state.container_run_workdir
        ? renderTemplate(state.container_run_workdir, blackboard)
        : undefined;

      const crEnv: Record<string, string> = {};
      if (state.container_run_env) {
        for (const [k, v] of Object.entries(state.container_run_env)) {
          crEnv[k] = renderTemplate(String(v), blackboard);
        }
      }

      // ADR-092: inject caller-supplied input fields as INTENT_INPUTS for parametric scripts
      // Only if the workflow didn't already define INTENT_INPUTS in its env section
      if (
        !crEnv["INTENT_INPUTS"] &&
        blackboard.input &&
        typeof blackboard.input === "object"
      ) {
        const inputObj = blackboard.input as Record<string, unknown>;
        if (Object.keys(inputObj).length > 0) {
          crEnv["INTENT_INPUTS"] = JSON.stringify(inputObj);
        }
      }

      await emit("ContainerRunStarted", {
        state_name: stateName,
        name: renderedName,
        image: renderedImage,
      });

      const crResult = await executeContainerRunActivity({
        execution_id: executionId,
        state_name: stateName,
        name: renderedName,
        image: renderedImage,
        image_pull_policy: state.container_run_image_pull_policy,
        command: renderedCommand,
        env: crEnv,
        workdir: renderedWorkdir,
        volumes: (state.container_run_volumes ?? []).map((vm) => {
          const resolvedName =
            (blackboard[`${vm.name}_volume_id`] as string | undefined) ??
            vm.name;
          return { ...vm, name: resolvedName };
        }),
        resources: state.container_run_resources,
        registry_credentials: state.container_run_registry_credentials,
        shell: state.container_run_shell ?? false,
        max_attempts: state.container_run_retry?.max_attempts ?? 1,
        security_context_name: securityContextName,
        workflow_execution_id: executionId,
      });

      if (crResult.exit_code === 0) {
        await emit("ContainerRunCompleted", {
          state_name: stateName,
          exit_code: crResult.exit_code,
          duration_ms: crResult.duration_ms,
          attempts: crResult.attempts,
        });
      } else {
        await emit("ContainerRunFailed", {
          state_name: stateName,
          exit_code: crResult.exit_code,
          stderr: crResult.stderr,
          duration_ms: crResult.duration_ms,
          attempts: crResult.attempts,
        });
      }

      const crStateResult = {
        status: crResult.exit_code === 0 ? "success" : "failed",
        output: {
          exit_code: crResult.exit_code,
          stdout: crResult.stdout,
          stderr: crResult.stderr,
          duration_ms: crResult.duration_ms,
          attempts: crResult.attempts,
        },
        exit_code: crResult.exit_code,
        stdout: crResult.stdout,
        stderr: crResult.stderr,
        duration_ms: crResult.duration_ms,
        attempts: crResult.attempts,
      };

      if (state.output_handler) {
        // ContainerRun has no agent execution — pass empty string so the
        // orchestrator spawns a standalone output handler execution.
        try {
          await executeOutputHandlerActivity({
            executionId: "",
            tenantId: (blackboard.tenant_id as string) ?? "",
            finalOutput: JSON.stringify(crStateResult),
            handlerConfigJson: JSON.stringify(state.output_handler),
          });
        } catch (err) {
          if (state.output_handler.required) {
            throw err;
          }
          // fire-and-forget: log and continue
          console.warn("Optional output handler failed:", err);
        }
      }

      return crStateResult;
    }

    case "ParallelContainerRun": {
      if (
        !state.parallel_container_steps ||
        state.parallel_container_steps.length === 0
      ) {
        throw new Error(
          `Invalid ParallelContainerRun state '${stateName}': no steps defined`,
        );
      }

      const renderedSteps = state.parallel_container_steps.map((step) => ({
        name: step.name ? renderTemplate(step.name, blackboard) : step.name,
        image: renderTemplate(step.image, blackboard),
        command: step.command.map((arg: string) =>
          renderTemplate(arg, blackboard),
        ),
        workdir: step.workdir
          ? renderTemplate(step.workdir, blackboard)
          : undefined,
        env: step.env
          ? Object.fromEntries(
              Object.entries(step.env).map(([k, v]) => [
                k,
                renderTemplate(String(v), blackboard),
              ]),
            )
          : undefined,
        volumes: step.volumes,
        resources: step.resources,
        registry_credentials: step.registry_credentials,
        shell: step.shell,
      }));

      await emit("ContainerRunStarted", {
        state_name: stateName,
        step_count: renderedSteps.length,
        completion: state.parallel_container_completion ?? "all_succeed",
      });

      const pcrResult = await executeParallelContainerRunActivity({
        execution_id: executionId,
        state_name: stateName,
        steps: renderedSteps,
        completion: state.parallel_container_completion ?? "all_succeed",
        securityContextName,
      });

      const byStep = Object.fromEntries(
        pcrResult.results.map((r) => [
          r.name,
          {
            exit_code: r.exit_code,
            stdout: r.stdout,
            stderr: r.stderr,
            duration_ms: r.duration_ms,
          },
        ]),
      );

      if (pcrResult.overall_success) {
        await emit("ContainerRunCompleted", {
          state_name: stateName,
          overall_success: pcrResult.overall_success,
          completion: pcrResult.completion,
          succeeded: pcrResult.succeeded,
          failed: pcrResult.failed,
          step_results: pcrResult.results.map((r) => ({
            name: r.name,
            exit_code: r.exit_code,
            duration_ms: r.duration_ms,
          })),
        });
      } else {
        await emit("ContainerRunFailed", {
          state_name: stateName,
          overall_success: pcrResult.overall_success,
          completion: pcrResult.completion,
          succeeded: pcrResult.succeeded,
          failed: pcrResult.failed,
          step_results: pcrResult.results.map((r) => ({
            name: r.name,
            exit_code: r.exit_code,
            stderr: r.stderr,
          })),
        });
      }

      return {
        status: pcrResult.overall_success ? "success" : "failed",
        overall_success: pcrResult.overall_success,
        completion: pcrResult.completion,
        succeeded: pcrResult.succeeded,
        failed: pcrResult.failed,
        output: byStep,
        results: pcrResult.results,
      };
    }

    case "Subworkflow": {
      if (!state.subworkflow_id) {
        throw new Error(
          `Invalid Subworkflow state '${stateName}': missing subworkflow_id`,
        );
      }
      const childWorkflowId = renderTemplate(state.subworkflow_id, blackboard);
      if (!childWorkflowId || childWorkflowId.trim() === "") {
        throw new Error(
          `Template resolution failed: "${state.subworkflow_id}" resolved to empty string. Check blackboard keys.`,
        );
      }
      const childMode = state.subworkflow_mode ?? "blocking";

      // Evaluate input template if provided
      let childInput: Record<string, any> = {};
      if (state.subworkflow_input) {
        const rendered = renderTemplate(state.subworkflow_input, blackboard);
        try {
          childInput = JSON.parse(rendered);
        } catch {
          childInput = { input: rendered };
        }
      }

      const childExecutionId = uuid4();

      await emit("SubworkflowTriggered", {
        child_execution_id: childExecutionId,
        child_workflow_id: childWorkflowId,
        mode: childMode,
        parent_state_name: stateName,
        parent_execution_id: executionId,
      });

      if (childMode === "blocking") {
        // Use Temporal executeChild — blocks until child completes
        let childResult;
        try {
          childResult = await workflow.executeChild("aegis_workflow", {
            args: [
              {
                workflow_id: childWorkflowId,
                execution_id: childExecutionId,
                input: childInput,
                blackboard: {},
                parent_execution_id: executionId,
                security_context_name: securityContextName,
                tenant_id: blackboard.tenant_id as string | undefined,
              },
            ],
            workflowId: `${childWorkflowId}-${childExecutionId}`,
          });
        } catch (error) {
          const errMsg = error instanceof Error ? error.message : String(error);
          await emit("SubworkflowFailed", {
            child_execution_id: childExecutionId,
            error: errMsg,
            parent_execution_id: executionId,
          });
          throw error;
        }

        // Write result to parent blackboard under result_key
        const resultKey = state.subworkflow_result_key ?? `${stateName}_result`;
        blackboard[resultKey] = childResult;

        await emit("SubworkflowCompleted", {
          child_execution_id: childExecutionId,
          result_key: resultKey,
          parent_execution_id: executionId,
        });

        return childResult ?? {};
      } else {
        // Fire-and-forget — start child but don't wait
        await workflow.startChild("aegis_workflow", {
          args: [
            {
              workflow_id: childWorkflowId,
              execution_id: childExecutionId,
              input: childInput,
              blackboard: {},
              parent_execution_id: executionId,
              security_context_name: securityContextName,
              tenant_id: blackboard.tenant_id as string | undefined,
            },
          ],
          workflowId: `${childWorkflowId}-${childExecutionId}`,
        });

        return {
          child_execution_id: childExecutionId,
          mode: "fire_and_forget",
        };
      }
    }

    default:
      throw new Error(`Unknown state kind: ${state.kind}`);
  }
}

async function evaluateTransitions(
  transitions: TransitionRule[],
  stateOutput: any,
  blackboard: Blackboard,
): Promise<string | null> {
  for (const t of transitions) {
    if (await evaluateCondition(t, stateOutput, blackboard)) {
      return t.target;
    }
  }
  return null;
}

async function evaluateCondition(
  t: TransitionRule,
  output: any,
  bb: Blackboard,
): Promise<boolean> {
  const resolvedExitCode =
    typeof output?.exit_code === "number"
      ? output.exit_code
      : typeof output?.output?.exit_code === "number"
        ? output.output.exit_code
        : undefined;

  const resolvedStatus = output?.status;

  switch (t.condition) {
    case "always":
      return true;
    case "on_success":
      return (
        resolvedStatus === "completed" ||
        resolvedStatus === "success" ||
        resolvedExitCode === 0 ||
        output?.overall_success === true
      );
    case "on_failure":
      return (
        resolvedStatus === "failed" ||
        resolvedStatus === "error" ||
        (typeof resolvedExitCode === "number" && resolvedExitCode !== 0) ||
        output?.overall_success === false
      );
    case "exit_code_zero":
      return resolvedExitCode === 0;
    case "exit_code_non_zero":
      return typeof resolvedExitCode === "number" && resolvedExitCode !== 0;
    case "exit_code":
      return (
        typeof resolvedExitCode === "number" && resolvedExitCode === t.exit_code
      );
    case "score_above":
      return (output?.score || output?.final_score || 0) > (t.threshold || 0);
    case "score_below":
      return (output?.score || output?.final_score || 0) < (t.threshold || 1);
    case "score_between":
      const score = output?.score || output?.final_score || 0;
      return score >= (t.min || 0) && score <= (t.max || 1);
    case "confidence_above":
      return (output?.confidence || 0) > (t.threshold || 0);
    case "score_and_confidence_above":
      return (
        (output?.score || output?.final_score || 0) > (t.threshold || 0) &&
        (output?.confidence || 0) > (t.threshold || 0)
      );
    case "consensus":
      const consensusScore =
        output?.consensus?.score ?? output?.final_score ?? 0;
      const consensusConfidence =
        output?.consensus?.confidence ?? output?.confidence ?? 0;
      return (
        consensusScore >= (t.threshold || 0) &&
        consensusConfidence >= (t.agreement || 0)
      );
    case "all_approved":
      return (
        Array.isArray(output?.individual_scores) &&
        output.individual_scores.length > 0 &&
        output.individual_scores.every((s: number) => s >= (t.threshold || 0.8))
      );
    case "any_rejected":
      return (
        Array.isArray(output?.individual_scores) &&
        output.individual_scores.some((s: number) => s < (t.threshold || 0.8))
      );
    case "input_equals":
      return output?.response === t.value;
    case "input_equals_yes":
      return ["yes", "y", "1"].includes(String(output?.response).toLowerCase());
    case "input_equals_no":
      return ["no", "n", "0"].includes(String(output?.response).toLowerCase());
    case "custom":
      if (!t.expression) return false;
      try {
        const tmpl = `{{#if ${t.expression}}}true{{else}}false{{/if}}`;
        return (
          renderTemplate(tmpl, { ...bb, state_output: output }).trim() ===
          "true"
        );
      } catch {
        // If custom handlebar evaluation throws, we assume false to prevent FSM crash
        return false;
      }
    default:
      return false;
  }
}

function renderTemplate(tmpl: string, ctx: any): string {
  // Expose the full context under a `blackboard` key so workflow YAML templates
  // can reference state results via either `{{EXECUTE_CODE.output.stdout}}` or
  // `{{blackboard.EXECUTE_CODE.output.stdout}}`.  Without this, any template
  // that uses the `blackboard.X` path style silently resolves to an empty string
  // because there is no `blackboard` property on the top-level context object,
  // causing downstream agents to receive truncated or empty inputs.
  const enrichedCtx = { ...ctx, blackboard: ctx };
  return Handlebars.compile(tmpl)(enrichedCtx);
}

function parseTimeout(str: string): number {
  const m = str.match(/^(\d+)([smh])$/);
  if (!m) return 60;
  const v = parseInt(m[1]);
  const u = m[2];
  if (u === "m") return v * 60;
  if (u === "h") return v * 3600;
  return v;
}
