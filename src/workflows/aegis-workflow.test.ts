import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TemporalWorkflowDefinition } from "../types.js";

const { activityMocks, terminalActivityMocks, executeAgentRpcMock } =
  vi.hoisted(() => {
    // A panel's activity has one attempt; it is the same mock either way.
    const executeParallelAgentsActivity = vi.fn();
    return {
      activityMocks: {
        executeAgentActivity: vi.fn(),
        executeSystemCommandActivity: vi.fn(),
        validateOutputActivity: vi.fn(),
        executeParallelAgentsActivity,
        storeTrajectoryPatternActivity: vi.fn(),
        fetchWorkflowDefinition: vi.fn(),
        publishEventActivity: vi.fn(),
        executeContainerRunActivity: vi.fn(),
        executeParallelContainerRunActivity: vi.fn(),
        executeOutputHandlerActivity: vi.fn(),
        runRepositoryActionActivity: vi.fn(),
        runImageGenerateActivity: vi.fn(),
        createEphemeralWorkspaceActivity: vi.fn(),
        destroyWorkspaceVolumeActivity: vi.fn(),
      },
      terminalActivityMocks: {
        executeAgentActivity: vi.fn(),
        executeParallelAgentsActivity,
      },
      executeAgentRpcMock: vi.fn(),
    };
  });

/** The agent's run limit as the workflow reads it; 1,800 s unless a test says otherwise. */
const { agentRunLimitSecondsMock } = vi.hoisted(() => ({
  agentRunLimitSecondsMock: vi.fn(async () => 1800),
}));

vi.mock("../activities/index.js", () => activityMocks);

vi.mock("../activities/workflow-activities.js", () => ({
  fetchWorkflowDefinition: vi.fn(),
}));

vi.mock("../grpc/client.js", () => ({
  cancelAgentExecution: vi.fn(),
  aegisRuntimeClient: {
    executeAgent: executeAgentRpcMock,
    executeSystemCommand: vi.fn(),
    validateWithJudges: vi.fn(),
    storeTrajectoryPattern: vi.fn(),
    executeContainerRun: vi.fn(),
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

vi.mock("@temporalio/workflow", () => ({
  proxyActivities: vi.fn(
    (options?: { retry?: { maximumAttempts?: number } }) => ({
      ...(options?.retry?.maximumAttempts === 1
        ? terminalActivityMocks
        : activityMocks),
      agentRunLimitSecondsActivity: agentRunLimitSecondsMock,
    }),
  ),
  setHandler: vi.fn(),
  defineSignal: vi.fn(() => Symbol("humanInput")),
  condition: vi.fn(async (predicate: () => boolean) => predicate()),
  workflowInfo: vi.fn(() => ({ workflowId: "exec-123" })),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { condition, proxyActivities, setHandler } from "@temporalio/workflow";
import { aegis_workflow } from "./aegis-workflow.js";

function baseDefinition(
  states: TemporalWorkflowDefinition["states"],
  initial = "BUILD",
): TemporalWorkflowDefinition {
  return {
    workflow_id: "wf-1",
    tenant_id: "local",
    name: "ci-workflow",
    version: "1.0.0",
    initial_state: initial,
    context: {},
    states,
  };
}

describe("aegis_workflow container orchestration behavior", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });
  });

  it("stores ContainerRun output shape with nested output object for blackboard templates", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "build-ok",
      stderr: "",
      duration_ms: 120,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    const build = result.blackboard?.BUILD;

    expect(result.status).toBe("completed");
    expect(activityMocks.fetchWorkflowDefinition).toHaveBeenCalledWith("wf-1");
    expect(build?.status).toBe("success");
    expect(build?.output?.exit_code).toBe(0);
    expect(build?.output?.stdout).toBe("build-ok");
    expect(build?.exit_code).toBe(0);
  });

  it("routes on_success using container exit code 0", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          BUILD: {
            kind: "ContainerRun",
            container_run_name: "build",
            container_run_image: "rust:1.75",
            container_run_command: ["cargo", "build"],
            transitions: [
              { condition: "on_success", target: "PASS" },
              { condition: "on_failure", target: "FAIL" },
            ],
          },
          PASS: { kind: "System", command: "echo pass", transitions: [] },
          FAIL: { kind: "System", command: "echo fail", transitions: [] },
        },
        "BUILD",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 10,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    expect(result.final_state).toBe("PASS");
  });

  it("routes on_failure using non-zero container exit code", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          BUILD: {
            kind: "ContainerRun",
            container_run_name: "build",
            container_run_image: "rust:1.75",
            container_run_command: ["cargo", "build"],
            transitions: [
              { condition: "on_success", target: "PASS" },
              { condition: "on_failure", target: "FAIL" },
            ],
          },
          PASS: { kind: "System", command: "echo pass", transitions: [] },
          FAIL: { kind: "System", command: "echo fail", transitions: [] },
        },
        "BUILD",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 2,
      stdout: "",
      stderr: "compile failed",
      duration_ms: 10,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    expect(result.final_state).toBe("FAIL");
  });

  it("supports exit_code_non_zero transition for ContainerRun outputs", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          BUILD: {
            kind: "ContainerRun",
            container_run_name: "build",
            container_run_image: "rust:1.75",
            container_run_command: ["cargo", "build"],
            transitions: [
              { condition: "exit_code_non_zero", target: "FAIL" },
              { condition: "always", target: "PASS" },
            ],
          },
          PASS: { kind: "System", command: "echo pass", transitions: [] },
          FAIL: { kind: "System", command: "echo fail", transitions: [] },
        },
        "BUILD",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 9,
      stdout: "",
      stderr: "failed",
      duration_ms: 10,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    expect(result.final_state).toBe("FAIL");
  });

  it("returns ParallelContainerRun blackboard output keyed by step name", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          TEST: {
            kind: "ParallelContainerRun",
            parallel_container_steps: [
              {
                name: "unit-tests",
                image: "rust:1.75",
                command: ["cargo", "test"],
              },
              {
                name: "lint",
                image: "rust:1.75",
                command: ["cargo", "clippy"],
              },
            ],
            parallel_container_completion: "all_succeed",
            transitions: [],
          },
        },
        "TEST",
      ),
    );
    activityMocks.executeParallelContainerRunActivity.mockResolvedValue({
      overall_success: true,
      completion: "all_succeed",
      succeeded: 2,
      failed: 0,
      results: [
        {
          name: "unit-tests",
          exit_code: 0,
          stdout: "ok",
          stderr: "",
          duration_ms: 12,
        },
        {
          name: "lint",
          exit_code: 0,
          stdout: "ok",
          stderr: "",
          duration_ms: 8,
        },
      ],
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    const testOutput = result.blackboard?.TEST?.output;

    expect(result.status).toBe("completed");
    expect(testOutput?.["unit-tests"]?.stdout).toBe("ok");
    expect(testOutput?.["lint"]?.exit_code).toBe(0);
  });

  it("passes -s flag when runner_flags is '-s' so Python does not prepend /workspace to sys.path", async () => {
    // Regression: without runner_flags="-s", `python /workspace/solution.py` puts
    // /workspace at sys.path[0].  Every import then triggers _fill_cache which
    // calls readdir on the FUSE mount — a directory-enumeration operation the FUSE
    // implementation does not support — yielding OSError: [Errno 5] Input/output
    // error before a single line of user code executes.
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_CODE: {
            kind: "ContainerRun",
            container_run_name: "execute-user-code",
            container_run_image: "python:3.12-slim",
            container_run_command: [
              "{{input.runner}}",
              "{{input.runner_flags}}",
              "/workspace/solution.{{input.language_ext}}",
            ],
            transitions: [],
          },
        },
        "EXECUTE_CODE",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "42",
      stderr: "",
      duration_ms: 80,
      attempts: 1,
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {
        runner: "python3",
        runner_flags: "-s",
        language_ext: "py",
      },
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    expect(call.command).toEqual(["python3", "-s", "/workspace/solution.py"]);
  });

  it("omits runner_flags from command when runner_flags is empty string", async () => {
    // Regression: when runner_flags resolves to "" (e.g. for node or bash), the
    // empty string must not be forwarded as an argument — it would be treated as
    // an invalid filename by the interpreter.
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_CODE: {
            kind: "ContainerRun",
            container_run_name: "execute-user-code",
            container_run_image: "node:22-slim",
            container_run_command: [
              "{{input.runner}}",
              "{{input.runner_flags}}",
              "/workspace/solution.{{input.language_ext}}",
            ],
            transitions: [],
          },
        },
        "EXECUTE_CODE",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "hello",
      stderr: "",
      duration_ms: 40,
      attempts: 1,
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {
        runner: "node",
        runner_flags: "",
        language_ext: "js",
      },
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    expect(call.command).toEqual(["node", "/workspace/solution.js"]);
  });

  it("merges startup blackboard overrides at top level and preserves workflow metadata", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          BUILD: {
            kind: "Agent",
            agent: "builder-agent",
            input:
              "Repo {{repo}} for {{owner}} on {{workflow.name}} {{input.branch}}",
            transitions: [],
          },
        },
        "BUILD",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "done",
      iterations: 1,
    });

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { branch: "main" },
      blackboard: { owner: "alice", repo: "override-repo" },
    });

    expect(result.status).toBe("completed");
    expect(activityMocks.executeAgentActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        input: "Repo override-repo for alice on ci-workflow main",
        context: expect.objectContaining({
          owner: "alice",
          repo: "override-repo",
          input: expect.objectContaining({
            branch: "main",
          }),
          workflow: expect.objectContaining({
            name: "ci-workflow",
          }),
        }),
      }),
    );
  });

  it("transitions past PLAN when the runtime child execution completes with JSON output", async () => {
    const actualActivities = await vi.importActual<
      typeof import("../activities/index.js")
    >("../activities/index.js");

    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          PLAN: {
            kind: "Agent",
            agent: "123e4567-e89b-12d3-a456-426614174000",
            input: "plan",
            transitions: [{ condition: "on_success", target: "NEXT" }],
          },
          NEXT: {
            kind: "System",
            command: "echo {{PLAN.output.workflow_prompt}}",
            transitions: [],
          },
        },
        "PLAN",
      ),
    );
    activityMocks.executeAgentActivity.mockImplementation(
      actualActivities.executeAgentActivity,
    );
    executeAgentRpcMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-1",
        timestamp: "2026-03-22T08:32:12.572760Z",
        final_output: JSON.stringify({
          workflow_prompt: "generate-workflow",
        }),
        total_iterations: 1,
      },
    ]);

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(executeAgentRpcMock).toHaveBeenCalledWith(
      expect.objectContaining({
        agent_id: "123e4567-e89b-12d3-a456-426614174000",
        workflow_execution_id: "exec-123",
      }),
      expect.anything(),
    );
    const request = executeAgentRpcMock.mock.calls[0][0];
    expect(request.parent_execution_id).toBeUndefined();
    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("NEXT");
    expect(result.blackboard?.PLAN?.output?.workflow_prompt).toBe(
      "generate-workflow",
    );
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        command: "echo generate-workflow",
      }),
    );
  });

  it("routes the final workflow-creator-validator-agent through the terminal activity proxy and fails cleanly on error", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          GENERATE_AND_REGISTER_WORKFLOW: {
            kind: "Agent",
            agent: "workflow-creator-validator-agent",
            input: "validate and register",
            transitions: [],
          },
        },
        "GENERATE_AND_REGISTER_WORKFLOW",
      ),
    );
    terminalActivityMocks.executeAgentActivity.mockRejectedValue(
      new Error("Connection dropped"),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(terminalActivityMocks.executeAgentActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        agentId: "workflow-creator-validator-agent",
      }),
    );
    expect(activityMocks.executeAgentActivity).not.toHaveBeenCalled();
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Connection dropped");
    expect(activityMocks.publishEventActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "WorkflowExecutionFailed",
      }),
    );
  });

  it("names an activity failure's cause in WorkflowIterationFailed", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          ANALYZE: {
            kind: "Agent",
            agent: "requirements-analyst",
            input: "analyze",
            transitions: [],
          },
        },
        "ANALYZE",
      ),
    );
    // The shape Temporal gives the workflow: an ActivityFailure whose cause
    // is the activity's own ApplicationFailure.
    const activityFailure = new Error("Activity task failed", {
      cause: new Error(
        "An earlier attempt of this step began the agent run; it is not started again.",
      ),
    });
    activityMocks.executeAgentActivity.mockRejectedValue(activityFailure);
    terminalActivityMocks.executeAgentActivity.mockRejectedValue(
      activityFailure,
    );

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.publishEventActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "WorkflowIterationFailed",
        error:
          "Activity task failed: An earlier attempt of this step began the agent run; it is not started again.",
      }),
    );
  });

  it("routes an Agent state's failed execution to its on_failure transition", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          DRAFT: {
            kind: "Agent",
            agent: "outreach-drafter",
            input: "draft",
            transitions: [
              { condition: "on_success", target: "SEND" },
              { condition: "on_failure", target: "REJECTED" },
            ],
          },
          SEND: {
            kind: "System",
            command: "echo send",
            transitions: [],
          },
          REJECTED: {
            kind: "System",
            command: "echo rejected",
            transitions: [],
          },
        },
        "DRAFT",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "failed",
      error: "Failed to execute task: Max retries exceeded",
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    const eventTypes = activityMocks.publishEventActivity.mock.calls.map(
      ([event]) => event.event_type,
    );
    expect(result.final_state).toBe("REJECTED");
    expect(result.status).toBe("completed");
    expect(
      eventTypes.filter((t) => t === "WorkflowIterationFailed"),
    ).toHaveLength(1);
    expect(activityMocks.publishEventActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "WorkflowIterationFailed",
        error:
          "Agent execution failed: Failed to execute task: Max retries exceeded",
      }),
    );
    expect(eventTypes).not.toContain("WorkflowExecutionFailed");
    expect(result.blackboard?.DRAFT).toEqual({
      status: "failed",
      error:
        "Agent execution failed: Failed to execute task: Max retries exceeded",
    });
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledWith(
      expect.objectContaining({ command: "echo rejected" }),
    );
  });

  it("fails the run when an Agent state's execution fails and it declares no on_failure transition", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          DRAFT: {
            kind: "Agent",
            agent: "outreach-drafter",
            input: "draft",
            transitions: [{ condition: "on_success", target: "SEND" }],
          },
          SEND: {
            kind: "System",
            command: "echo send",
            transitions: [],
          },
        },
        "DRAFT",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "failed",
      error: "Failed to execute task: Max retries exceeded",
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    expect(result.final_state).toBe("DRAFT");
    expect(result.error).toBe(
      "Agent execution failed: Failed to execute task: Max retries exceeded",
    );
    expect(activityMocks.publishEventActivity).toHaveBeenCalledWith(
      expect.objectContaining({ event_type: "WorkflowExecutionFailed" }),
    );
    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
  });

  it("stores Agent output under a nested output field and transitions out of PLAN on completion", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          PLAN: {
            kind: "Agent",
            agent: "planner-agent",
            input: "plan",
            transitions: [{ condition: "on_success", target: "NEXT" }],
          },
          NEXT: {
            kind: "System",
            command: "echo {{PLAN.output.workflow_prompt}}",
            transitions: [],
          },
        },
        "PLAN",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: {
        workflow_prompt: "generate-workflow",
      },
      iterations: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("NEXT");
    expect(result.blackboard?.PLAN?.output?.workflow_prompt).toBe(
      "generate-workflow",
    );
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        command: "echo generate-workflow",
      }),
    );
  });

  it("renders state.agent Handlebars template before passing agentId to activity", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          GENERATE_CODE: {
            kind: "Agent",
            agent: "{{DISCOVER_OR_GENERATE_AGENT.output.agent_name}}",
            input: "write code",
            transitions: [],
          },
        },
        "GENERATE_CODE",
      ),
    );
    // Pre-populate the blackboard via definition context so the template resolves
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      workflow_id: "wf-1",
      tenant_id: "local",
      name: "ci-workflow",
      version: "1.0.0",
      initial_state: "GENERATE_CODE",
      context: {
        DISCOVER_OR_GENERATE_AGENT: {
          output: { agent_name: "actual-agent-id" },
        },
      },
      states: {
        GENERATE_CODE: {
          kind: "Agent",
          agent: "{{DISCOVER_OR_GENERATE_AGENT.output.agent_name}}",
          input: "write code",
          transitions: [],
        },
      },
    });
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "generated",
      iterations: 1,
    });

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.executeAgentActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        agentId: "actual-agent-id",
      }),
    );
    // Confirm the raw template string was NOT passed through
    expect(activityMocks.executeAgentActivity).not.toHaveBeenCalledWith(
      expect.objectContaining({
        agentId: "{{DISCOVER_OR_GENERATE_AGENT.output.agent_name}}",
      }),
    );
  });

  it("renders ContainerRun image and command templates from blackboard", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_CODE: {
            kind: "ContainerRun",
            container_run_image: "{{input.container_image}}",
            container_run_command: [
              "{{input.runner}}",
              "/workspace/solution.{{input.language_ext}}",
            ],
            transitions: [],
          },
        },
        "EXECUTE_CODE",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "42",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: {
        container_image: "python:3.11-slim",
        runner: "python",
        language_ext: "py",
      },
    });

    expect(result.status).toBe("completed");
    expect(activityMocks.executeContainerRunActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        image: "python:3.11-slim",
        command: ["python", "/workspace/solution.py"],
      }),
    );
    // Confirm the raw template strings were NOT passed through
    expect(activityMocks.executeContainerRunActivity).not.toHaveBeenCalledWith(
      expect.objectContaining({
        image: "{{input.container_image}}",
      }),
    );
  });

  it("renders ParallelContainerRun image and command templates for each step", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_PARALLEL: {
            kind: "ParallelContainerRun",
            parallel_container_steps: [
              {
                name: "test-step",
                image: "{{input.container_image}}",
                command: [
                  "{{input.runner}}",
                  "/workspace/solution.{{input.language_ext}}",
                ],
              },
            ],
            parallel_container_completion: "all_succeed",
            transitions: [],
          },
        },
        "EXECUTE_PARALLEL",
      ),
    );
    activityMocks.executeParallelContainerRunActivity.mockResolvedValue({
      overall_success: true,
      completion: "all_succeed",
      succeeded: 1,
      failed: 0,
      results: [
        {
          name: "test-step",
          exit_code: 0,
          stdout: "ok",
          stderr: "",
          duration_ms: 50,
        },
      ],
    });

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: {
        container_image: "python:3.11-slim",
        runner: "python",
        language_ext: "py",
      },
    });

    expect(result.status).toBe("completed");
    expect(
      activityMocks.executeParallelContainerRunActivity,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        steps: expect.arrayContaining([
          expect.objectContaining({
            image: "python:3.11-slim",
            command: ["python", "/workspace/solution.py"],
          }),
        ]),
      }),
    );
    // Confirm the raw template strings were NOT passed through
    expect(
      activityMocks.executeParallelContainerRunActivity,
    ).not.toHaveBeenCalledWith(
      expect.objectContaining({
        steps: expect.arrayContaining([
          expect.objectContaining({
            image: "{{input.container_image}}",
          }),
        ]),
      }),
    );
  });

  it("routes on_failure for ParallelContainerRun when aggregation fails", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          TEST: {
            kind: "ParallelContainerRun",
            parallel_container_steps: [
              { name: "unit", image: "rust:1.75", command: ["cargo", "test"] },
              {
                name: "lint",
                image: "rust:1.75",
                command: ["cargo", "clippy"],
              },
            ],
            parallel_container_completion: "all_succeed",
            transitions: [
              { condition: "on_success", target: "PASS" },
              { condition: "on_failure", target: "FAIL" },
            ],
          },
          PASS: { kind: "System", command: "echo pass", transitions: [] },
          FAIL: { kind: "System", command: "echo fail", transitions: [] },
        },
        "TEST",
      ),
    );
    activityMocks.executeParallelContainerRunActivity.mockResolvedValue({
      overall_success: false,
      completion: "all_succeed",
      succeeded: 1,
      failed: 1,
      results: [
        {
          name: "unit",
          exit_code: 0,
          stdout: "ok",
          stderr: "",
          duration_ms: 12,
        },
        {
          name: "lint",
          exit_code: 2,
          stdout: "",
          stderr: "lint fail",
          duration_ms: 8,
        },
      ],
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    expect(result.final_state).toBe("FAIL");
    expect(result.blackboard?.TEST?.status).toBe("failed");
  });
});

describe("INTENT_INPUTS env var precedence regression", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
  });

  it("does not overwrite workflow-defined INTENT_INPUTS with the full input object", async () => {
    // Regression: when a workflow template sets
    //   container_run_env: { INTENT_INPUTS: "{{{input.inputs_json}}}" }
    // the ADR-092 fallback was unconditionally overwriting it with
    // JSON.stringify(blackboard.input) — which includes container_image,
    // language, runner, etc., not just the user's inputs.
    const userInputs = JSON.stringify({ x: 42, name: "test" });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_CODE: {
            kind: "ContainerRun",
            container_run_name: "run",
            container_run_image: "python:3.12-slim",
            container_run_command: ["python3", "/workspace/solution.py"],
            container_run_env: {
              INTENT_INPUTS: "{{{input.inputs_json}}}",
            },
            transitions: [],
          },
        },
        "EXECUTE_CODE",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 50,
      attempts: 1,
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {
        container_image: "python:3.12-slim",
        language: "python",
        runner: "python3",
        inputs_json: userInputs,
      },
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    // INTENT_INPUTS must be the rendered template value (just user inputs),
    // NOT JSON.stringify of the entire input object
    expect(call.env.INTENT_INPUTS).toBe(userInputs);
  });

  it("falls back to ADR-092 injection when workflow does not define INTENT_INPUTS", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_CODE: {
            kind: "ContainerRun",
            container_run_name: "run",
            container_run_image: "python:3.12-slim",
            container_run_command: ["python3", "/workspace/solution.py"],
            transitions: [],
          },
        },
        "EXECUTE_CODE",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 50,
      attempts: 1,
    });

    const inputPayload = { language: "python", runner: "python3" };
    await aegis_workflow({
      workflow_id: "wf-1",
      input: inputPayload,
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    // When no workflow-level INTENT_INPUTS exists, the ADR-092 fallback
    // should inject the full input object
    expect(call.env.INTENT_INPUTS).toBe(JSON.stringify(inputPayload));
  });
});

describe("output handler execution ID regression", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeOutputHandlerActivity.mockResolvedValue(undefined);
  });

  it("passes agent execution_id to output handler for Agent states", async () => {
    const agentExecId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "Agent",
          agent: "writer-agent",
          input: "write something",
          transitions: [],
          output_handler: {
            type: "webhook",
            url: "http://localhost:9999/hook",
            method: "POST",
            headers: {},
            required: false,
          },
        },
      }),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "written content",
      iterations: 1,
      execution_id: agentExecId,
    });

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.executeOutputHandlerActivity).toHaveBeenCalledTimes(1);
    const call = activityMocks.executeOutputHandlerActivity.mock.calls[0][0];
    expect(call.executionId).toBe(agentExecId);
  });

  it("passes empty executionId to output handler for ContainerRun states", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
          output_handler: {
            type: "webhook",
            url: "http://localhost:9999/hook",
            method: "POST",
            headers: {},
            required: false,
          },
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.executeOutputHandlerActivity).toHaveBeenCalledTimes(1);
    const call = activityMocks.executeOutputHandlerActivity.mock.calls[0][0];
    expect(call.executionId).toBe("");
  });

  it("passes empty executionId when agent result has no execution_id", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "Agent",
          agent: "writer-agent",
          input: "write something",
          transitions: [],
          output_handler: {
            type: "webhook",
            url: "http://localhost:9999/hook",
            method: "POST",
            headers: {},
            required: false,
          },
        },
      }),
    );
    // Simulate agent result WITHOUT execution_id (e.g. synthesized terminal event)
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "written content",
      iterations: 1,
    });

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.executeOutputHandlerActivity).toHaveBeenCalledTimes(1);
    const call = activityMocks.executeOutputHandlerActivity.mock.calls[0][0];
    expect(call.executionId).toBe("");
  });
});

describe("Handlebars keys helper", () => {
  it("keys helper extracts object keys as JSON array", async () => {
    // The keys helper is registered at module load. To exercise it end-to-end
    // we run a workflow that uses {{{keys ...}}} in a template and verify the
    // rendered output reaches the activity.
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          CHECK: {
            kind: "System",
            command: "echo {{{keys input.inputs}}}",
            transitions: [],
          },
        },
        "CHECK",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: { inputs: { a: 1, b: 2 } },
    });

    const call = activityMocks.executeSystemCommandActivity.mock.calls[0][0];
    expect(call.command).toBe('echo ["a","b"]');
  });

  it("keys helper parses JSON string inputs and extracts keys", async () => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          CHECK: {
            kind: "System",
            command: "echo {{{keys input.inputs}}}",
            transitions: [],
          },
        },
        "CHECK",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: { inputs: '{"log":"hello","level":"info"}' },
    });

    const call = activityMocks.executeSystemCommandActivity.mock.calls[0][0];
    expect(call.command).toBe('echo ["log","level"]');
  });

  it("keys helper returns empty array for invalid JSON strings", async () => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          CHECK: {
            kind: "System",
            command: "echo {{{keys input.inputs}}}",
            transitions: [],
          },
        },
        "CHECK",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: { inputs: "not-valid-json" },
    });

    const call = activityMocks.executeSystemCommandActivity.mock.calls[0][0];
    expect(call.command).toBe("echo []");
  });

  it("keys helper returns empty array for non-object values", async () => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          CHECK: {
            kind: "System",
            command: "echo {{{keys input.missing}}}",
            transitions: [],
          },
        },
        "CHECK",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
    });

    const call = activityMocks.executeSystemCommandActivity.mock.calls[0][0];
    expect(call.command).toBe("echo []");
  });
});

describe("per-agent temperature threading", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
  });

  it("threads state-level temperature to executeAgentActivity", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          GENERATE: {
            kind: "Agent",
            agent: "creative-agent",
            input: "write a poem",
            temperature: 0.9,
            transitions: [],
          },
        },
        "GENERATE",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "a poem",
      iterations: 1,
    });

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.executeAgentActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        agentId: "creative-agent",
        temperature: 0.9,
      }),
    );
  });

  it("omits temperature when not set on state", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          BUILD: {
            kind: "Agent",
            agent: "builder-agent",
            input: "build it",
            transitions: [],
          },
        },
        "BUILD",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "built",
      iterations: 1,
    });

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    const call = activityMocks.executeAgentActivity.mock.calls[0][0];
    expect(call.temperature).toBeUndefined();
  });
});

describe("output handler wiring", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
  });

  it("ContainerRun state with output handler replaces stdout with handler result", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
          output_handler: {
            type: "webhook",
            url: "http://localhost:9999/format",
            method: "POST",
            headers: {},
            required: false,
          },
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "raw-container-stdout",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });
    activityMocks.executeOutputHandlerActivity.mockResolvedValue(
      "formatted-by-handler",
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    const build = result.blackboard?.BUILD;

    expect(result.status).toBe("completed");
    expect(build?.stdout).toBe("formatted-by-handler");
    expect(build?.output?.stdout).toBe("formatted-by-handler");
  });

  it("Agent state with output handler merges handler result into output", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          GENERATE: {
            kind: "Agent",
            agent: "writer-agent",
            input: "write something",
            transitions: [],
            output_handler: {
              type: "webhook",
              url: "http://localhost:9999/format",
              method: "POST",
              headers: {},
              required: false,
            },
          },
        },
        "GENERATE",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "raw-agent-output",
      iterations: 1,
      execution_id: "agent-exec-1",
    });
    activityMocks.executeOutputHandlerActivity.mockResolvedValue(
      "handler-formatted-output",
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    const gen = result.blackboard?.GENERATE;

    expect(result.status).toBe("completed");
    expect(gen?.output).toBe("handler-formatted-output");
  });

  it("output handler failure with required: true transitions to failure", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
          output_handler: {
            type: "webhook",
            url: "http://localhost:9999/format",
            method: "POST",
            headers: {},
            required: true,
          },
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });
    activityMocks.executeOutputHandlerActivity.mockRejectedValue(
      new Error("Handler webhook failed"),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    expect(result.error).toContain("Handler webhook failed");
  });

  it("output handler failure with required: false preserves raw output and continues", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
          output_handler: {
            type: "webhook",
            url: "http://localhost:9999/format",
            method: "POST",
            headers: {},
            required: false,
          },
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "raw-output-preserved",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });
    activityMocks.executeOutputHandlerActivity.mockRejectedValue(
      new Error("Handler failed"),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    const build = result.blackboard?.BUILD;

    expect(result.status).toBe("completed");
    expect(build?.stdout).toBe("raw-output-preserved");
    expect(build?.output?.stdout).toBe("raw-output-preserved");
  });
});

describe("output template evaluation", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });
  });

  it("renders output_template Handlebars expressions from blackboard into final_output", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
        },
      }),
      output_template: {
        result: "{{BUILD.stdout}}",
        exit: "{{BUILD.exit_code}}",
      },
    });
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "build-ok",
      stderr: "",
      duration_ms: 120,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_output).toBeDefined();
    expect(result.final_output?.result).toBe("build-ok");
    expect(result.final_output?.exit).toBe("0");
  });

  it("coerces output_template integer type from rendered string to number", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          COMPUTE: {
            kind: "ContainerRun",
            container_run_name: "compute",
            container_run_image: "python:3.12",
            container_run_command: ["python", "-c", "print(42)"],
            transitions: [],
          },
        },
        "COMPUTE",
      ),
      output_template: {
        answer: "{{COMPUTE.stdout}}",
      },
      output_schema: {
        type: "object",
        properties: {
          answer: { type: "integer" },
        },
      },
    });
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "42",
      stderr: "",
      duration_ms: 50,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.final_output?.answer).toBe(42);
    expect(typeof result.final_output?.answer).toBe("number");
  });

  it("coerces boolean and object types correctly via output_schema", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          CHECK: {
            kind: "ContainerRun",
            container_run_name: "check",
            container_run_image: "alpine",
            container_run_command: ["echo", "true"],
            transitions: [],
          },
        },
        "CHECK",
      ),
      output_template: {
        passed: "{{CHECK.stdout}}",
        metadata: "{{{CHECK.output.stdout}}}",
      },
      output_schema: {
        type: "object",
        properties: {
          passed: { type: "boolean" },
          metadata: { type: "object" },
        },
      },
    });
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "true",
      stderr: "",
      duration_ms: 10,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.final_output?.passed).toBe(true);
    expect(typeof result.final_output?.passed).toBe("boolean");
  });

  it("returns undefined final_output when no output_template is defined", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_output).toBeUndefined();
  });

  it("renders missing blackboard key as empty string in output_template", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
        },
      }),
      output_template: {
        present: "{{BUILD.stdout}}",
        missing: "{{NONEXISTENT.output.value}}",
      },
    });
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "build-ok",
      stderr: "",
      duration_ms: 100,
      attempts: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.final_output?.present).toBe("build-ok");
    expect(result.final_output?.missing).toBe("");
  });
});

describe("ContainerRunStateResult shape", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
  });

  it("ContainerRun state output matches ContainerRunStateResult with root and nested fields", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition({
        BUILD: {
          kind: "ContainerRun",
          container_run_name: "build",
          container_run_image: "rust:1.75",
          container_run_command: ["cargo", "build"],
          transitions: [],
        },
      }),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "compiled",
      stderr: "warn: unused",
      duration_ms: 250,
      attempts: 2,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });
    const build = result.blackboard?.BUILD;

    expect(result.status).toBe("completed");
    // Root-level fields
    expect(build?.status).toBe("success");
    expect(build?.exit_code).toBe(0);
    expect(build?.stdout).toBe("compiled");
    expect(build?.stderr).toBe("warn: unused");
    expect(build?.duration_ms).toBe(250);
    expect(build?.attempts).toBe(2);
    // Nested output object mirrors root fields
    expect(build?.output).toBeDefined();
    expect(build?.output?.exit_code).toBe(0);
    expect(build?.output?.stdout).toBe("compiled");
    expect(build?.output?.stderr).toBe("warn: unused");
    expect(build?.output?.duration_ms).toBe(250);
    expect(build?.output?.attempts).toBe(2);
  });
});

describe("max_state_visits and max_total_transitions", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });
  });

  it("terminates workflow when a state exceeds its explicit max_state_visits", async () => {
    // A -> B -> A loop where A has max_state_visits: 2
    // A will be visited twice; on the third visit it should terminate.
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            max_state_visits: 2,
            transitions: [{ condition: "always", target: "B" }],
          },
          B: {
            kind: "System",
            command: "echo b",
            transitions: [{ condition: "always", target: "A" }],
          },
        },
        "A",
      ),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    expect(result.output?.error).toContain(
      'State "A" exceeded max_state_visits limit of 2',
    );
    // A visited at iteration 1, 3 (after A->B->A), then iteration 5 triggers the limit
    expect(activityMocks.publishEventActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: "WorkflowStateVisitLimitExceeded",
        state_name: "A",
        max_visits: 2,
        actual_visits: 2,
      }),
    );
  });

  it("applies default max_state_visits of 5 when not explicitly set", async () => {
    // A -> B -> A loop with no explicit max_state_visits
    // Should terminate after A is visited 5 times (default)
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            transitions: [{ condition: "always", target: "B" }],
          },
          B: {
            kind: "System",
            command: "echo b",
            transitions: [{ condition: "always", target: "A" }],
          },
        },
        "A",
      ),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    expect(result.output?.error).toContain(
      'State "A" exceeded max_state_visits limit of 5',
    );
  });

  it("terminates workflow when max_total_transitions is exceeded", async () => {
    // Linear chain A -> B -> C -> A with max_total_transitions: 3
    // Should terminate after 3 transitions total
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            max_state_visits: 20,
            transitions: [{ condition: "always", target: "B" }],
          },
          B: {
            kind: "System",
            command: "echo b",
            max_state_visits: 20,
            transitions: [{ condition: "always", target: "C" }],
          },
          C: {
            kind: "System",
            command: "echo c",
            max_state_visits: 20,
            transitions: [{ condition: "always", target: "A" }],
          },
        },
        "A",
      ),
      max_total_transitions: 3,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    expect(result.error).toBe("Max iterations exceeded");
    expect(result.iterations).toBe(3);
  });

  function emittedEventTypes(): string[] {
    return activityMocks.publishEventActivity.mock.calls.map(
      (call) => call[0].event_type,
    );
  }

  function failedEvents(): any[] {
    return activityMocks.publishEventActivity.mock.calls
      .map((call) => call[0])
      .filter((event) => event.event_type === "WorkflowExecutionFailed");
  }

  it("ends a run that exceeds max_state_visits with exactly one WorkflowExecutionFailed, after the visit-limit event and the workspace's destruction", async () => {
    // Production, 2026-10-01: execution 6054290c reached its third visit to
    // EXECUTION_FAILED (max 2); the worker emitted only
    // WorkflowStateVisitLimitExceeded, so the orchestrator's record stayed
    // running.
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            max_state_visits: 2,
            transitions: [{ condition: "always", target: "B" }],
          },
          B: {
            kind: "System",
            command: "echo b",
            transitions: [{ condition: "always", target: "A" }],
          },
        },
        "A",
      ),
      spec_storage: { workspace: { storage_class: "ephemeral" } },
    });
    activityMocks.createEphemeralWorkspaceActivity.mockResolvedValue({
      volume_id: "vol-1",
      remote_path: "/remote/vol-1",
    });
    activityMocks.destroyWorkspaceVolumeActivity.mockResolvedValue(undefined);

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    const types = emittedEventTypes();
    const limitIndex = types.indexOf("WorkflowStateVisitLimitExceeded");
    const failedIndex = types.indexOf("WorkflowExecutionFailed");
    expect(limitIndex).toBeGreaterThanOrEqual(0);
    expect(failedIndex).toBeGreaterThan(limitIndex);
    expect(failedIndex).toBe(types.length - 1);

    const failed = failedEvents();
    expect(failed).toHaveLength(1);
    expect(failed[0].error).toBe(
      'State "A" exceeded max_state_visits limit of 2',
    );
    expect(failed[0].error).toBe(result.output?.error);

    expect(activityMocks.destroyWorkspaceVolumeActivity).toHaveBeenCalledTimes(
      1,
    );
    const destroyOrder =
      activityMocks.destroyWorkspaceVolumeActivity.mock.invocationCallOrder[0];
    const publishOrders =
      activityMocks.publishEventActivity.mock.invocationCallOrder;
    expect(destroyOrder).toBeGreaterThan(publishOrders[limitIndex]);
    expect(publishOrders[failedIndex]).toBeGreaterThan(destroyOrder);
  });

  it("still emits WorkflowExecutionFailed on a visit-limit run whose workspace destruction fails", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            max_state_visits: 1,
            transitions: [{ condition: "always", target: "A" }],
          },
        },
        "A",
      ),
      spec_storage: { workspace: { storage_class: "ephemeral" } },
    });
    activityMocks.createEphemeralWorkspaceActivity.mockResolvedValue({
      volume_id: "vol-1",
    });
    activityMocks.destroyWorkspaceVolumeActivity.mockRejectedValue(
      new Error("storage unavailable"),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    const failed = failedEvents();
    expect(failed).toHaveLength(1);
    expect(failed[0].error).toBe(
      'State "A" exceeded max_state_visits limit of 1',
    );
  });

  it("emits exactly one WorkflowExecutionFailed when max_total_transitions is exceeded", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            max_state_visits: 20,
            transitions: [{ condition: "always", target: "A" }],
          },
        },
        "A",
      ),
      max_total_transitions: 3,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    const failed = failedEvents();
    expect(failed).toHaveLength(1);
    expect(failed[0].error).toBe("Max iterations exceeded");
    expect(emittedEventTypes()).not.toContain(
      "WorkflowStateVisitLimitExceeded",
    );
  });

  it("emits exactly one WorkflowExecutionFailed when a state's activity fails", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          A: {
            kind: "System",
            command: "echo a",
            transitions: [],
          },
        },
        "A",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockRejectedValue(
      new Error("command crashed"),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    const failed = failedEvents();
    expect(failed).toHaveLength(1);
    expect(failed[0].error).toBe("command crashed");
  });
});

describe("ADR-113 attachment hydration", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
  });

  const sampleAttachments = [
    {
      volume_id: "chat-attachments",
      path: "uploads/2026-04/abc.pdf",
      name: "abc.pdf",
      mime_type: "application/pdf",
      size: 12345,
      sha256: "deadbeef",
    },
  ];

  it("threads attachments onto Agent state's executeAgentActivity call", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          ANALYZE: {
            kind: "Agent",
            agent: "doc-analyzer",
            input: "Analyze the attached docs",
            transitions: [],
          },
        },
        "ANALYZE",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "ok",
      iterations: 1,
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      attachments: sampleAttachments,
    });

    expect(activityMocks.executeAgentActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        attachments: sampleAttachments,
      }),
    );
  });

  it("merges attachments into INTENT_INPUTS env for ContainerRun states", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE: {
            kind: "ContainerRun",
            container_run_name: "run",
            container_run_image: "python:3.12-slim",
            container_run_command: ["python3", "/workspace/solution.py"],
            transitions: [],
          },
        },
        "EXECUTE",
      ),
    );
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 50,
      attempts: 1,
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: { language: "python" },
      attachments: sampleAttachments,
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    const intentInputs = JSON.parse(call.env.INTENT_INPUTS);
    expect(intentInputs.attachments).toEqual(sampleAttachments);
    expect(intentInputs.language).toBe("python");
  });

  it("renders {{attachments}} in Handlebars context for Agent input templates", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          ANALYZE: {
            kind: "Agent",
            agent: "doc-analyzer",
            input: "Files: {{{json attachments}}}",
            transitions: [],
          },
        },
        "ANALYZE",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "ok",
      iterations: 1,
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      attachments: sampleAttachments,
    });

    const call = activityMocks.executeAgentActivity.mock.calls[0][0];
    expect(call.input).toBe(`Files: ${JSON.stringify(sampleAttachments)}`);
  });

  it("omits attachments from gRPC request when dispatch carries none", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          ANALYZE: {
            kind: "Agent",
            agent: "123e4567-e89b-12d3-a456-426614174000",
            input: "no attachments here",
            transitions: [],
          },
        },
        "ANALYZE",
      ),
    );
    const actualActivities = await vi.importActual<
      typeof import("../activities/index.js")
    >("../activities/index.js");
    activityMocks.executeAgentActivity.mockImplementation(
      actualActivities.executeAgentActivity,
    );
    executeAgentRpcMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-1",
        timestamp: "2026-03-22T08:32:12.572760Z",
        final_output: "ok",
        total_iterations: 1,
      },
    ]);

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    const request = executeAgentRpcMock.mock.calls[0][0];
    expect(request.attachments).toBeUndefined();
  });
});

describe("score transitions read the score a state carries", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    executeAgentRpcMock.mockReset();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "ok",
      stderr: "",
    });
  });

  const terminals = {
    DONE: { kind: "System" as const, command: "echo done", transitions: [] },
    FAILED: {
      kind: "System" as const,
      command: "echo failed",
      transitions: [],
    },
    OTHER: { kind: "System" as const, command: "echo other", transitions: [] },
  };

  const scoreRoutes = [
    { condition: "score_above" as const, threshold: 0.7, target: "DONE" },
    { condition: "score_below" as const, threshold: 0.7, target: "FAILED" },
  ];

  function panelDefinition() {
    return baseDefinition(
      {
        PANEL: {
          kind: "ParallelAgents",
          agents: [
            { agent: "judge-a", input: "{}", weight: 1.0 },
            { agent: "judge-b", input: "{}", weight: 1.0 },
          ],
          consensus: { strategy: "weighted_average", threshold: 0.8 },
          transitions: scoreRoutes,
        },
        ...terminals,
      },
      "PANEL",
    );
  }

  function failedEvents() {
    return activityMocks.publishEventActivity.mock.calls
      .map((call) => call[0])
      .filter((e) => e.event_type === "WorkflowExecutionFailed");
  }

  it("routes a ParallelAgents state whose consensus score is 0.94 by score_above to DONE", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(panelDefinition());
    activityMocks.executeParallelAgentsActivity.mockResolvedValue({
      consensus: {
        score: 0.94,
        confidence: 0.97,
        strategy: "weighted_average",
      },
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("DONE");
  });

  it("routes a ParallelAgents state whose consensus score is 0.5 by score_below to FAILED", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(panelDefinition());
    activityMocks.executeParallelAgentsActivity.mockResolvedValue({
      consensus: { score: 0.5, confidence: 0.9, strategy: "weighted_average" },
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.final_state).toBe("FAILED");
  });

  it.each([
    ["score", { score: 0.9 }, "DONE"],
    ["score", { score: 0.3 }, "FAILED"],
    ["final_score", { final_score: 0.9 }, "DONE"],
    ["final_score", { final_score: 0.3 }, "FAILED"],
  ])(
    "routes an Agent state whose output carries %s %j to %s",
    async (_field, extra, expected) => {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            CHECK: {
              kind: "Agent",
              agent: "checker",
              input: "check",
              transitions: scoreRoutes,
            },
            ...terminals,
          },
          "CHECK",
        ),
      );
      activityMocks.executeAgentActivity.mockResolvedValue({
        status: "completed",
        output: "checked",
        iterations: 1,
        ...extra,
      });

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.final_state).toBe(expected);
    },
  );

  it("fails the run, naming the state, when a state with only score transitions carries no score (skill-import's PARSE_AND_VALIDATE)", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          PARSE_AND_VALIDATE: {
            kind: "Agent",
            agent: "skill-validator",
            input: "validate",
            transitions: scoreRoutes,
          },
          ...terminals,
        },
        "PARSE_AND_VALIDATE",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: { quality_score: 0.9 },
      iterations: 1,
      execution_id: "exec-child",
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    const expectedError =
      'State "PARSE_AND_VALIDATE" carries no score for its score transitions';
    expect(result.status).toBe("failed");
    expect(result.error).toBe(expectedError);
    expect(result.final_state).toBe("PARSE_AND_VALIDATE");
    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    const failed = failedEvents();
    expect(failed).toHaveLength(1);
    expect(failed[0].error).toBe(expectedError);
  });

  describe("an Agent state carries its agent's score", () => {
    function agentDefinition() {
      return baseDefinition(
        {
          PARSE_AND_VALIDATE: {
            kind: "Agent",
            agent: "123e4567-e89b-12d3-a456-426614174000",
            input: "validate",
            transitions: [
              { condition: "score_above", threshold: 0.7, target: "NEXT" },
              { condition: "score_below", threshold: 0.7, target: "FAILED" },
            ],
          },
          NEXT: { kind: "System", command: "echo next", transitions: [] },
          ...terminals,
        },
        "PARSE_AND_VALIDATE",
      );
    }

    async function runWithFinalOutput(finalOutput: string) {
      const actualActivities = await vi.importActual<
        typeof import("../activities/index.js")
      >("../activities/index.js");
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        agentDefinition(),
      );
      activityMocks.executeAgentActivity.mockImplementation(
        actualActivities.executeAgentActivity,
      );
      executeAgentRpcMock.mockResolvedValue([
        {
          event_type: "ExecutionCompleted",
          execution_id: "child-exec-1",
          timestamp: "2026-10-02T06:30:00Z",
          final_output: finalOutput,
          total_iterations: 1,
        },
      ]);
      return aegis_workflow({ workflow_id: "wf-1", input: {} });
    }

    it("routes by score_above to NEXT when the agent returns a JSON object with score 0.9", async () => {
      const result = await runWithFinalOutput(
        JSON.stringify({ score: 0.9, summary: "valid skill" }),
      );

      expect(result.error).toBeUndefined();
      expect(result.status).toBe("completed");
      expect(result.final_state).toBe("NEXT");
      expect(result.blackboard?.PARSE_AND_VALIDATE?.score).toBe(0.9);
      expect(result.blackboard?.PARSE_AND_VALIDATE?.output?.summary).toBe(
        "valid skill",
      );
    });

    it("routes by score_below to FAILED when the agent returns a JSON object with score 0.3", async () => {
      const result = await runWithFinalOutput(
        "```json\n" + JSON.stringify({ score: 0.3 }) + "\n```",
      );

      expect(result.error).toBeUndefined();
      expect(result.status).toBe("completed");
      expect(result.final_state).toBe("FAILED");
      expect(result.blackboard?.PARSE_AND_VALIDATE?.score).toBe(0.3);
    });

    it.each([
      ["text", "The skill looks valid, score 0.9"],
      ["an object without a score", JSON.stringify({ quality_score: 0.9 })],
      ["a score that is not a number", JSON.stringify({ score: "0.9" })],
      ["a score above 1.0", JSON.stringify({ score: 1.5 })],
      ["a score below 0.0", JSON.stringify({ score: -0.1 })],
      ["an array", JSON.stringify([{ score: 0.9 }])],
    ])(
      "carries no score when the agent returns %s, and the run fails naming the state",
      async (_kind, finalOutput) => {
        const result = await runWithFinalOutput(finalOutput);

        expect(result.status).toBe("failed");
        expect(result.error).toBe(
          'State "PARSE_AND_VALIDATE" carries no score for its score transitions',
        );
        expect(result.final_state).toBe("PARSE_AND_VALIDATE");
        expect(
          activityMocks.executeSystemCommandActivity,
        ).not.toHaveBeenCalled();
      },
    );
  });

  it("takes another matching transition when a state carries no score", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          CHECK: {
            kind: "Agent",
            agent: "checker",
            input: "check",
            transitions: [
              ...scoreRoutes,
              { condition: "on_success", target: "OTHER" },
            ],
          },
          ...terminals,
        },
        "CHECK",
      ),
    );
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "checked",
      iterations: 1,
    });

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("OTHER");
  });

  describe("score_between, score_and_confidence_above and consensus read a missing score as absent", () => {
    function agentState(
      transitions: TemporalWorkflowDefinition["states"][string]["transitions"],
      output: Record<string, unknown>,
    ) {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            CHECK: {
              kind: "Agent",
              agent: "checker",
              input: "check",
              transitions,
            },
            ...terminals,
          },
          "CHECK",
        ),
      );
      activityMocks.executeAgentActivity.mockResolvedValue({
        status: "completed",
        output: "checked",
        iterations: 1,
        ...output,
      });
    }

    function panelState(
      transitions: TemporalWorkflowDefinition["states"][string]["transitions"],
      consensus: Record<string, unknown>,
    ) {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            PANEL: {
              kind: "ParallelAgents",
              agents: [
                { agent: "judge-a", input: "{}", weight: 1.0 },
                { agent: "judge-b", input: "{}", weight: 1.0 },
              ],
              consensus: { strategy: "weighted_average", threshold: 0.8 },
              transitions,
            },
            ...terminals,
          },
          "PANEL",
        ),
      );
      activityMocks.executeParallelAgentsActivity.mockResolvedValue({
        consensus,
      });
    }

    const noScoreError = (state: string) =>
      `State "${state}" carries no score for its score transitions`;

    it("score_between does not match a state that carries no score", async () => {
      agentState(
        [
          { condition: "score_between", min: 0, max: 0.5, target: "FAILED" },
          { condition: "on_success", target: "OTHER" },
        ],
        {},
      );

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.error).toBeUndefined();
      expect(result.final_state).toBe("OTHER");
    });

    it.each([
      [0.3, "FAILED"],
      [0.9, "OTHER"],
    ])(
      "score_between reads a score of %s as today",
      async (score, expected) => {
        agentState(
          [
            { condition: "score_between", min: 0, max: 0.5, target: "FAILED" },
            { condition: "on_success", target: "OTHER" },
          ],
          { score },
        );

        const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

        expect(result.final_state).toBe(expected);
      },
    );

    it.each([
      ["no score", { confidence: 0.9 }],
      ["no confidence", { score: 0.9 }],
      ["neither", {}],
    ])(
      "score_and_confidence_above does not match a state with %s",
      async (_kind, output) => {
        agentState(
          [
            {
              condition: "score_and_confidence_above",
              threshold: 0,
              target: "DONE",
            },
            { condition: "on_success", target: "OTHER" },
          ],
          output,
        );

        const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

        expect(result.error).toBeUndefined();
        expect(result.final_state).toBe("OTHER");
      },
    );

    it.each([
      [{ score: 0.9, confidence: 0.9 }, "DONE"],
      [{ score: 0.6, confidence: 0.9 }, "OTHER"],
      [{ score: 0.9, confidence: 0.6 }, "OTHER"],
      [{ final_score: 0.9, confidence: 0.9 }, "DONE"],
    ])(
      "score_and_confidence_above reads %j as today",
      async (output, expected) => {
        agentState(
          [
            {
              condition: "score_and_confidence_above",
              threshold: 0.7,
              target: "DONE",
            },
            { condition: "on_success", target: "OTHER" },
          ],
          output,
        );

        const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

        expect(result.final_state).toBe(expected);
      },
    );

    it("the consensus condition does not match a ParallelAgents state whose consensus has no score", async () => {
      panelState(
        [
          { condition: "consensus", target: "DONE" },
          { condition: "always", target: "OTHER" },
        ],
        {
          strategy: "weighted_average",
          metadata: { reasoning: "there is no score to combine" },
        },
      );

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.error).toBeUndefined();
      expect(result.final_state).toBe("OTHER");
    });

    it.each([
      [{ score: 0.9, confidence: 0.8 }, "DONE"],
      [{ score: 0.6, confidence: 0.8 }, "OTHER"],
      [{ score: 0.9, confidence: 0.5 }, "OTHER"],
    ])(
      "the consensus condition reads a consensus of %j as today",
      async (consensus, expected) => {
        panelState(
          [
            {
              condition: "consensus",
              threshold: 0.8,
              agreement: 0.7,
              target: "DONE",
            },
            { condition: "always", target: "OTHER" },
          ],
          { ...consensus, strategy: "weighted_average" },
        );

        const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

        expect(result.final_state).toBe(expected);
      },
    );

    it("score_between and score_and_confidence_above read a ParallelAgents state's consensus score and confidence", async () => {
      panelState(
        [
          { condition: "score_between", min: 0, max: 0.5, target: "FAILED" },
          {
            condition: "score_and_confidence_above",
            threshold: 0.7,
            target: "DONE",
          },
          { condition: "always", target: "OTHER" },
        ],
        { score: 0.9, confidence: 0.8, strategy: "weighted_average" },
      );

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.final_state).toBe("DONE");
    });

    it("fails the run, naming the state, when it declares only score_between and carries no score", async () => {
      agentState(
        [{ condition: "score_between", min: 0, max: 0.5, target: "FAILED" }],
        {},
      );

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.error).toBe(noScoreError("CHECK"));
      expect(result.status).toBe("failed");
      expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    });

    it("fails the run, naming the state, when it declares only score_and_confidence_above and carries no score", async () => {
      agentState(
        [
          {
            condition: "score_and_confidence_above",
            threshold: 0.7,
            target: "DONE",
          },
        ],
        { confidence: 0.9 },
      );

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.error).toBe(noScoreError("CHECK"));
      expect(result.status).toBe("failed");
    });

    it("fails the run, naming the state, when it declares only the consensus condition and its consensus has no score", async () => {
      panelState([{ condition: "consensus", target: "DONE" }], {
        strategy: "weighted_average",
      });

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.error).toBe(noScoreError("PANEL"));
      expect(result.status).toBe("failed");
      expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    });
  });
});

describe("an Agent state's activity limit follows its agent's run limit", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    for (const fn of Object.values(terminalActivityMocks)) {
      fn.mockReset();
    }
    vi.mocked(proxyActivities).mockClear();
    agentRunLimitSecondsMock.mockClear();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    const completed = { status: "completed", output: "done", iterations: 1 };
    activityMocks.executeAgentActivity.mockResolvedValue(completed);
    terminalActivityMocks.executeAgentActivity.mockResolvedValue(completed);
  });

  function agentState(agent: string, stateName = "RECORD") {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        { [stateName]: { kind: "Agent", agent, input: "go", transitions: [] } },
        stateName,
      ),
      tenant_id: "tenant-a",
    });
  }

  it.each([
    [1200, "1260 seconds"],
    [1800, "1860 seconds"],
  ])(
    "gives an agent whose run limit is %d s an activity of %s, a heartbeat timeout, and 3 attempts",
    async (limit, startToClose) => {
      agentState("email-loop-clerk");
      agentRunLimitSecondsMock.mockResolvedValueOnce(limit);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: {},
        blackboard: { tenant_id: "tenant-a" },
      });

      expect(result.status).toBe("completed");
      expect(agentRunLimitSecondsMock).toHaveBeenCalledWith({
        agentId: "email-loop-clerk",
        tenantId: "tenant-a",
      });
      expect(proxyActivities).toHaveBeenCalledWith({
        startToCloseTimeout: startToClose,
        heartbeatTimeout: "2 minutes",
        retry: { maximumAttempts: 3 },
      });
      expect(activityMocks.executeAgentActivity).toHaveBeenCalledTimes(1);
    },
  );

  it("gives the terminal validation agent the same limit with one attempt", async () => {
    agentState("workflow-creator-validator-agent", "VALIDATE");
    agentRunLimitSecondsMock.mockResolvedValueOnce(600);

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(proxyActivities).toHaveBeenCalledWith({
      startToCloseTimeout: "660 seconds",
      heartbeatTimeout: "2 minutes",
      retry: { maximumAttempts: 1 },
    });
    expect(terminalActivityMocks.executeAgentActivity).toHaveBeenCalledTimes(1);
  });
});

// The Forge's AUDIT: a panel of judges whose own run limits exceed ten minutes
// must not be cut off at ten minutes and re-run as a fresh panel.
describe("a ParallelAgents state's activity limit follows its agents' run limits", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    vi.mocked(proxyActivities).mockClear();
    agentRunLimitSecondsMock.mockClear();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "",
      stderr: "",
    });
    activityMocks.executeParallelAgentsActivity.mockResolvedValue({
      consensus: {
        score: 0.99,
        confidence: 0.95,
        strategy: "weighted_average",
      },
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue({
      ...baseDefinition(
        {
          AUDIT: {
            kind: "ParallelAgents",
            agents: [
              { agent: "code-reviewer-agent", input: "{}", weight: 1.0 },
              { agent: "security-auditor-agent", input: "{}", weight: 2.0 },
            ],
            consensus: { strategy: "weighted_average", threshold: 0.95 },
            transitions: [
              { condition: "score_above", threshold: 0.95, target: "DONE" },
            ],
          },
          DONE: { kind: "System", command: "echo done", transitions: [] },
        },
        "AUDIT",
      ),
      tenant_id: "tenant-a",
    });
    agentRunLimitSecondsMock
      .mockResolvedValueOnce(900)
      .mockResolvedValueOnce(600);
  });

  /** The options of every activity proxy the run itself built. */
  function proxiesBuiltByTheRun() {
    return vi.mocked(proxyActivities).mock.calls.map((call) => call[0]);
  }

  it("gives a panel whose longest agent limit is 900 s an activity of 960 seconds with a heartbeat timeout, and each agent its own limit", async () => {
    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      blackboard: { tenant_id: "tenant-a" },
    });

    expect(result.status).toBe("completed");
    expect(proxiesBuiltByTheRun()).toEqual([
      expect.objectContaining({
        startToCloseTimeout: "960 seconds",
        heartbeatTimeout: "2 minutes",
      }),
    ]);
    expect(agentRunLimitSecondsMock).toHaveBeenCalledWith({
      agentId: "code-reviewer-agent",
      tenantId: "tenant-a",
    });
    expect(agentRunLimitSecondsMock).toHaveBeenCalledWith({
      agentId: "security-auditor-agent",
      tenantId: "tenant-a",
    });
    expect(
      activityMocks.executeParallelAgentsActivity.mock.calls[0][0].agents,
    ).toEqual([
      expect.objectContaining({
        agent: "code-reviewer-agent",
        timeoutSeconds: 900,
      }),
      expect.objectContaining({
        agent: "security-auditor-agent",
        timeoutSeconds: 600,
      }),
    ]);
  });

  it("does not retry the panel's activity, so a timed-out panel is never run again as a fresh one", async () => {
    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      blackboard: { tenant_id: "tenant-a" },
    });

    expect(proxiesBuiltByTheRun()).toEqual([
      expect.objectContaining({ retry: { maximumAttempts: 1 } }),
    ]);
    expect(activityMocks.executeParallelAgentsActivity).toHaveBeenCalledTimes(
      1,
    );
  });
});

// The Forge's EXECUTE_TESTS: a ContainerRun step's `repository` volume and its
// `network_mode` reach the orchestrator as the manifest names them; the
// orchestrator mounts the run's repository and picks the step network.
describe("a ContainerRun step's repository volume and network mode", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeContainerRunActivity.mockResolvedValue({
      exit_code: 0,
      stdout: "ok",
      stderr: "",
      duration_ms: 10,
      attempts: 1,
    });
  });

  it("passes `repository`, `repository:<label>` and `network_mode: egress` through unchanged", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_TESTS: {
            kind: "ContainerRun",
            container_run_image: "python:3.11-slim",
            container_run_command: ["pytest"],
            container_run_shell: true,
            container_run_volumes: [
              {
                name: "repository",
                mount_path: "/workspace",
                read_only: false,
              },
              {
                name: "repository:docs",
                mount_path: "/docs",
                read_only: true,
              },
            ],
            container_run_network_mode: "egress",
            transitions: [],
          },
        },
        "EXECUTE_TESTS",
      ),
    );

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      // A blackboard key of a volume-id's shape never renames the run's repository.
      blackboard: { repository_volume_id: "vol-other" },
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    expect(call.volumes).toEqual([
      { name: "repository", mount_path: "/workspace", read_only: false },
      { name: "repository:docs", mount_path: "/docs", read_only: true },
    ]);
    expect(call.network_mode).toBe("egress");
  });

  // EXECUTE_TESTS of run ec3f9b6b: with no tenant on the request, core read
  // the run in the worker's own tenant, found none, and refused the step.
  it("names the run's tenant on the ContainerRun request", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          EXECUTE_TESTS: {
            kind: "ContainerRun",
            container_run_image: "alpine:3.20",
            container_run_command: ["sh", "-c", "true"],
            container_run_volumes: [
              {
                name: "repository",
                mount_path: "/workspace/repository",
                read_only: false,
              },
            ],
            transitions: [],
          },
        },
        "EXECUTE_TESTS",
      ),
    );

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      tenant_id: "u-test",
    });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    expect(call.tenant_id).toBe("u-test");
  });

  it("names the run's tenant on the ParallelContainerRun activity", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          CHECKS: {
            kind: "ParallelContainerRun",
            parallel_container_steps: [
              { name: "a", image: "alpine:3.20", command: ["true"] },
            ],
            transitions: [],
          },
        },
        "CHECKS",
      ),
    );
    activityMocks.executeParallelContainerRunActivity.mockResolvedValue({
      results: [
        { name: "a", exit_code: 0, stdout: "", stderr: "", duration_ms: 1 },
      ],
      overall_success: true,
      succeeded: 1,
      failed: 0,
      completion: "all_succeed",
    });

    await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      tenant_id: "u-test",
    });

    const call =
      activityMocks.executeParallelContainerRunActivity.mock.calls[0][0];
    expect(call.tenant_id).toBe("u-test");
  });

  it("sends no network_mode when the state names none", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          BUILD: {
            kind: "ContainerRun",
            container_run_image: "node:24",
            container_run_command: ["npm", "test"],
            transitions: [],
          },
        },
        "BUILD",
      ),
    );

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    const call = activityMocks.executeContainerRunActivity.mock.calls[0][0];
    expect(call.network_mode).toBeUndefined();
  });
});

// The Forge's repository entry on the blackboard: the run's first prepared
// repository, with its path inside the step's container, and nothing more.
describe("the run's repository on the blackboard", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "{}",
      iterations: 1,
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          ANALYZE: {
            kind: "Agent",
            agent: "requirements-analyst",
            input: "read {{repository.path}} on {{repository.branch}}",
            transitions: [],
          },
        },
        "ANALYZE",
      ),
    );
  });

  it("writes the first prepared entry as `repository`, with no binding and no author", async () => {
    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      repositories: [
        {
          binding_id: "0b5f1c9e-2a54-4f43-9c43-7d3f1d6e2a10",
          branch: "aegis/run-exec-123",
          author: { name: "Jeshua", email: "jeshua@100monkeys.ai" },
          label: "aegis-examples",
          ref: "main",
          started_from: "352a61f3",
        },
        {
          binding_id: "6f1d0b8e-9c7a-4a8e-8b3e-2d4c5f6a7b8c",
          branch: "aegis/run-exec-123",
          label: "second",
          ref: "main",
          started_from: "0000000",
        },
      ],
    });

    expect(result.blackboard?.repository).toEqual({
      label: "aegis-examples",
      path: "/workspace/aegis-examples",
      branch: "aegis/run-exec-123",
      ref: "main",
      started_from: "352a61f3",
    });
    expect(activityMocks.executeAgentActivity.mock.calls[0][0].input).toBe(
      "read /workspace/aegis-examples on aegis/run-exec-123",
    );
  });

  it("writes nulls for the fields the entry does not carry", async () => {
    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: {},
      repositories: [
        {
          binding_id: "0b5f1c9e-2a54-4f43-9c43-7d3f1d6e2a10",
          branch: "aegis/run-exec-123",
        },
      ],
    });

    expect(result.blackboard?.repository).toEqual({
      label: null,
      path: null,
      branch: "aegis/run-exec-123",
      ref: null,
      started_from: null,
    });
  });

  it("writes no `repository` for a run that holds none", async () => {
    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.blackboard).not.toHaveProperty("repository");
  });
});

// The Forge's two gates: a person at a Human state reads the prompt rendered
// against the blackboard, and a "no" carries the person's words back into the
// state it re-enters, through the transition's `feedback:`.
describe("a Human gate shows its rendered prompt and carries the person's feedback", () => {
  /** The signals a person sends, one per Human state reached, in order:
   *  the response, the optional feedback (null when absent), and the
   *  optional sub of the person who answered. */
  let signals: Array<[string, (string | null)?, (string | null)?]>;

  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    vi.mocked(setHandler).mockClear();
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: { summary: "R1. The judge is code-quality-judge." },
      iterations: 1,
    });
    signals = [];
    // A Human state waits on its signal: deliver the next one to the
    // workflow's registered `humanInput` handler, then read the predicate.
    vi.mocked(condition).mockImplementation((async (
      predicate: () => boolean,
    ) => {
      const next = signals.shift();
      if (next) {
        const handler = vi.mocked(setHandler).mock.calls.at(-1)?.[1] as (
          ...args: Array<string | null | undefined>
        ) => void;
        handler(...next);
      }
      return predicate();
    }) as typeof condition);
  });

  function gateDefinition(analyzeInput: string) {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          ANALYZE: {
            kind: "Agent",
            agent: "requirements-analyst",
            input: analyzeInput,
            transitions: [{ condition: "on_success", target: "APPROVE" }],
          },
          APPROVE: {
            kind: "Human",
            timeout: "3600s",
            prompt:
              "Approve the requirements for {{input.task}}:\n{{ANALYZE.output.summary}}",
            transitions: [
              { condition: "input_equals_yes", target: "DONE" },
              {
                condition: "input_equals_no",
                target: "ANALYZE",
                feedback: "{{human.feedback}}",
              },
            ],
          },
          DONE: { kind: "System", command: "true", transitions: [] },
        },
        "ANALYZE",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "",
      stderr: "",
    });
  }

  function humanInputRequested() {
    return activityMocks.publishEventActivity.mock.calls
      .map((call) => call[0])
      .filter((event) => event.event_type === "HumanInputRequested");
  }

  it("emits HumanInputRequested with the prompt rendered against the blackboard and the prior states' outputs", async () => {
    gateDefinition("{{input.task}}");
    signals.push(["yes"]);

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    expect(result.status).toBe("completed");
    expect(humanInputRequested()).toHaveLength(1);
    expect(humanInputRequested()[0].prompt).toBe(
      "Approve the requirements for name the judge:\nR1. The judge is code-quality-judge.",
    );
  });

  it("puts a 'no' with feedback on the blackboard's human.feedback and in the re-entered state's rendered input", async () => {
    gateDefinition(
      "{{input.task}}{{#if human.feedback}}\nSent back: {{human.feedback}}{{/if}}",
    );
    signals.push(["no", "Name the judge in agents/forge only."], ["yes"]);

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    expect(result.status).toBe("completed");
    const inputs = activityMocks.executeAgentActivity.mock.calls.map(
      (call) => call[0].input,
    );
    expect(inputs).toEqual([
      "name the judge",
      "name the judge\nSent back: Name the judge in agents/forge only.",
    ]);
    expect(result.blackboard?.human).toEqual({
      response: "yes",
      feedback: "",
    });
    expect(humanInputRequested()).toHaveLength(2);
  });

  it("keeps the feedback a 'no' carried on the blackboard until the state re-entered has read it", async () => {
    gateDefinition("{{input.task}}");
    signals.push(["no", "Name the judge in agents/forge only."]);
    // The second visit to APPROVE times out with no default: the run fails
    // there, holding the blackboard as the re-entered ANALYZE left it.
    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    expect(result.status).toBe("failed");
    expect(result.blackboard?.APPROVE).toEqual({
      response: "no",
      feedback: "Name the judge in agents/forge only.",
      timeout: false,
    });
    expect(result.blackboard?.human).toEqual({
      response: "no",
      feedback: "Name the judge in agents/forge only.",
    });
  });

  it("answers a signal with no feedback with an empty human.feedback", async () => {
    gateDefinition("{{input.task}}");
    signals.push(["no"], ["yes"]);

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    expect(result.status).toBe("completed");
    expect(result.blackboard?.APPROVE).toEqual({
      response: "yes",
      timeout: false,
    });
    expect(activityMocks.executeAgentActivity).toHaveBeenCalledTimes(2);
  });

  // Run `9f279438` (2026-10-12): Zaru answered a yes/no gate "approved", no
  // transition matched, and the run completed with nothing done. A word the
  // gate does not take never leaves it: the gate is posted again, opening with
  // the refusal, and waits for an answer it takes.
  const GATE_PROMPT =
    "Approve the requirements for name the judge:\nR1. The judge is code-quality-judge.";

  it("keeps a yes/no gate signalled 'approved' at the gate and posts it again opening with the refusal", async () => {
    gateDefinition("{{input.task}}");
    signals.push(["approved", "Approved: proceed with the design."]);

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    // No answer the gate takes ever came: the gate times out there.
    expect(result.status).toBe("failed");
    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    expect(humanInputRequested()).toHaveLength(2);
    expect(humanInputRequested()[1].prompt).toBe(
      `"approved" is not an answer this gate takes; answer yes or no.\n\n${GATE_PROMPT}`,
    );
    // It waits again within what is left of the gate's timeout.
    const waits = vi.mocked(condition).mock.calls.map((call) => call[1]);
    expect(waits).toHaveLength(2);
    expect(waits[1]).toBeLessThanOrEqual(waits[0] as number);
  });

  it("advances to the yes transition's target on 'yes' after a refused word", async () => {
    gateDefinition("{{input.task}}");
    signals.push(["approved"], ["yes"]);

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("DONE");
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledTimes(1);
    expect(activityMocks.executeAgentActivity).toHaveBeenCalledTimes(1);
    expect(result.blackboard?.APPROVE).toEqual({
      response: "yes",
      timeout: false,
    });
    expect(humanInputRequested()).toHaveLength(2);
  });

  function shipGateDefinition() {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          APPROVE: {
            kind: "Human",
            timeout: "3600s",
            prompt: "Ship it?",
            transitions: [
              { condition: "input_equals", value: "ship", target: "DONE" },
            ],
          },
          DONE: { kind: "System", command: "true", transitions: [] },
        },
        "APPROVE",
      ),
    );
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "",
      stderr: "",
    });
  }

  it("accepts 'ship' at an input_equals: ship gate", async () => {
    shipGateDefinition();
    signals.push(["ship"]);

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("DONE");
    expect(humanInputRequested()).toHaveLength(1);
  });

  it("posts an input_equals: ship gate again on 'yes', and advances on 'ship'", async () => {
    shipGateDefinition();
    signals.push(["yes"], ["ship"]);

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("completed");
    expect(result.final_state).toBe("DONE");
    expect(humanInputRequested().map((event) => event.prompt)).toEqual([
      "Ship it?",
      '"yes" is not an answer this gate takes; answer ship.\n\nShip it?',
    ]);
  });

  // A control of the person's (the gate card, Zaru ADR-0058 Update
  // `gate-card-2026-10-13`) answers a gate from its event alone: the event
  // names the gate's state, the words it takes and when it stops waiting.
  describe("the gate's event carries its state, its words and its deadline", () => {
    const POSTED_AT = "2026-10-13T02:40:00.000Z";
    const EXPIRES_AT = "2026-10-13T03:40:00.000Z";

    beforeEach(() => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(new Date(POSTED_AT));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    /** The Forge's two gates, as `the-forge.yaml` writes them. */
    function forgeGatesDefinition() {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            ANALYZE: {
              kind: "Agent",
              agent: "requirements-analyst",
              input: "{{input.task}}",
              transitions: [
                {
                  condition: "on_success",
                  target: "AWAIT_REQUIREMENTS_APPROVAL",
                },
              ],
            },
            AWAIT_REQUIREMENTS_APPROVAL: {
              kind: "Human",
              timeout: "3600s",
              prompt: "# The Forge: approve the requirements",
              transitions: [
                { condition: "input_equals_yes", target: "ARCHITECT" },
                {
                  condition: "input_equals_no",
                  target: "ANALYZE",
                  feedback: "{{human.feedback}}",
                },
              ],
            },
            ARCHITECT: {
              kind: "Agent",
              agent: "architect-agent",
              input: "{{input.task}}",
              transitions: [
                { condition: "on_success", target: "AWAIT_ARCH_APPROVAL" },
              ],
            },
            AWAIT_ARCH_APPROVAL: {
              kind: "Human",
              timeout: "3600s",
              prompt: "# The Forge: approve the design",
              transitions: [
                { condition: "input_equals_yes", target: "DONE" },
                {
                  condition: "input_equals_no",
                  target: "ARCHITECT",
                  feedback: "{{human.feedback}}",
                },
              ],
            },
            DONE: { kind: "System", command: "true", transitions: [] },
          },
          "ANALYZE",
        ),
      );
      activityMocks.executeSystemCommandActivity.mockResolvedValue({
        status: "success",
        exit_code: 0,
        stdout: "",
        stderr: "",
      });
    }

    it("posts each of the Forge's two gates with its state name, responses yes and no, and a deadline one timeout after the post", async () => {
      forgeGatesDefinition();
      signals.push(["yes"], ["yes"]);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
      });

      expect(result.status).toBe("completed");
      const gates = humanInputRequested();
      expect(gates).toHaveLength(2);
      expect(
        gates.map(({ state_name, responses, expires_at }) => ({
          state_name,
          responses,
          expires_at,
        })),
      ).toEqual([
        {
          state_name: "AWAIT_REQUIREMENTS_APPROVAL",
          responses: ["yes", "no"],
          expires_at: EXPIRES_AT,
        },
        {
          state_name: "AWAIT_ARCH_APPROVAL",
          responses: ["yes", "no"],
          expires_at: EXPIRES_AT,
        },
      ]);
      for (const gate of gates) {
        expect(Date.parse(gate.expires_at) - Date.parse(gate.timestamp)).toBe(
          3600 * 1000,
        );
      }
    });

    it("re-posts a gate after a refused word with its state name, its words and its first post's deadline", async () => {
      gateDefinition("{{input.task}}");
      // Ten minutes pass before the refused word, and ten more before the
      // answer: the gate's deadline stays one timeout after its first post.
      const deliver = vi.mocked(condition).getMockImplementation()!;
      vi.mocked(condition).mockImplementation((async (
        predicate: () => boolean,
        timeout?: number | string,
      ) => {
        vi.setSystemTime(Date.now() + 10 * 60 * 1000);
        return deliver(predicate, timeout as number);
      }) as typeof condition);
      signals.push(["approved"], ["yes"]);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
      });

      expect(result.status).toBe("completed");
      const gates = humanInputRequested();
      expect(gates).toHaveLength(2);
      expect(gates[1].prompt).toMatch(
        /^"approved" is not an answer this gate takes; answer yes or no\./,
      );
      for (const gate of gates) {
        expect(gate.state_name).toBe("APPROVE");
        expect(gate.responses).toEqual(["yes", "no"]);
        expect(gate.expires_at).toBe(EXPIRES_AT);
      }
    });

    it("posts a gate with no word transitions with responses []", async () => {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            NOTE: {
              kind: "Human",
              timeout: "600s",
              prompt: "Anything to add?",
              transitions: [{ condition: "always", target: "DONE" }],
            },
            DONE: { kind: "System", command: "true", transitions: [] },
          },
          "NOTE",
        ),
      );
      activityMocks.executeSystemCommandActivity.mockResolvedValue({
        status: "success",
        exit_code: 0,
        stdout: "",
        stderr: "",
      });
      signals.push(["ship it on Friday"]);

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.status).toBe("completed");
      const gates = humanInputRequested();
      expect(gates).toHaveLength(1);
      expect(gates[0].state_name).toBe("NOTE");
      expect(gates[0].responses).toEqual([]);
      expect(gates[0].expires_at).toBe("2026-10-13T02:50:00.000Z");
    });
  });

  // Zaru ADR-0058, Update `gate-card-2026-10-13`, D5n: an answered gate's
  // history names who answered. The signal's optional third argument is the
  // answerer's sub; the gate's WorkflowStateExited carries it beside the
  // response, the feedback and the timeout flag.
  describe("the gate's exit event names who answered", () => {
    function gateExited() {
      return activityMocks.publishEventActivity.mock.calls
        .map((call) => call[0])
        .filter(
          (event) =>
            event.event_type === "WorkflowStateExited" &&
            event.state_name === "APPROVE",
        );
    }

    it("carries answered_by from a signal that names the answerer", async () => {
      gateDefinition("{{input.task}}");
      signals.push(["yes", null, "u-test"]);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
      });

      expect(result.status).toBe("completed");
      expect(gateExited()).toHaveLength(1);
      expect(gateExited()[0].output).toStrictEqual({
        response: "yes",
        timeout: false,
        answered_by: "u-test",
      });
    });

    it("carries answered_by beside the feedback of a signal that gives both", async () => {
      gateDefinition("{{input.task}}");
      signals.push(["no", "Name the judge only.", "u-test"], ["yes"]);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
      });

      expect(result.status).toBe("completed");
      expect(gateExited().map((event) => event.output)).toStrictEqual([
        {
          response: "no",
          feedback: "Name the judge only.",
          timeout: false,
          answered_by: "u-test",
        },
        { response: "yes", timeout: false },
      ]);
    });

    it("carries no answered_by from a signal that does not name the answerer", async () => {
      gateDefinition("{{input.task}}");
      signals.push(["yes"]);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
      });

      expect(result.status).toBe("completed");
      expect(gateExited()).toHaveLength(1);
      expect(gateExited()[0].output).toStrictEqual({
        response: "yes",
        timeout: false,
      });
    });

    it("carries no answered_by from a signal whose feedback and answerer are null", async () => {
      gateDefinition("{{input.task}}");
      signals.push(["yes", null, null]);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
      });

      expect(result.status).toBe("completed");
      expect(gateExited()).toHaveLength(1);
      expect(gateExited()[0].output).toStrictEqual({
        response: "yes",
        timeout: false,
      });
    });

    it("carries no answered_by when the gate times out on its default response", async () => {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            APPROVE: {
              kind: "Human",
              timeout: "600s",
              prompt: "Ship it?",
              default_response: "no",
              transitions: [
                { condition: "input_equals_yes", target: "DONE" },
                { condition: "input_equals_no", target: "DONE" },
              ],
            },
            DONE: { kind: "System", command: "true", transitions: [] },
          },
          "APPROVE",
        ),
      );
      activityMocks.executeSystemCommandActivity.mockResolvedValue({
        status: "success",
        exit_code: 0,
        stdout: "",
        stderr: "",
      });

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.status).toBe("completed");
      expect(gateExited()).toHaveLength(1);
      expect(gateExited()[0].output).toStrictEqual({
        response: "no",
        timeout: true,
      });
    });
  });
});

// An `Action` state is the workflow's own deterministic operation: the
// orchestrator sends it as `{kind: "Action", action, args}` with no `command`
// or `env`, and the worker renders `args` and dispatches on `action`, never
// through a shell.
describe("an Action state runs the workflow's own operation", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "",
      stderr: "",
    });
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "{}",
      iterations: 1,
    });
  });

  // The shape of `email-inbox-triage`'s SEED and RECONCILE states, as an action.
  function seedDefinition(): TemporalWorkflowDefinition {
    return {
      ...baseDefinition(
        {
          SEED: {
            kind: "Action",
            action: "update_blackboard",
            args: {
              mailbox: "{{input.mailbox}}",
              page_prefix:
                "{{default input.page_prefix workflow.context.page_prefix}}",
            },
            transitions: [{ condition: "on_success", target: "RECONCILE" }],
          },
          RECONCILE: {
            kind: "Agent",
            agent: "email-loop-clerk",
            input:
              "mailbox {{blackboard.mailbox}}, prefix {{blackboard.page_prefix}}",
            transitions: [],
          },
        },
        "SEED",
      ),
      context: { page_prefix: "outreach" },
    };
  }

  it("update_blackboard writes its rendered args onto the blackboard, and no command runs", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(seedDefinition());

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { mailbox: "jeshua@100monkeys.ai", page_prefix: "" },
    });

    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    expect(activityMocks.runRepositoryActionActivity).not.toHaveBeenCalled();
    expect(activityMocks.executeAgentActivity.mock.calls[0][0].input).toBe(
      "mailbox jeshua@100monkeys.ai, prefix outreach",
    );
    expect(result.status).toBe("completed");
    expect(result.blackboard?.mailbox).toBe("jeshua@100monkeys.ai");
    expect(result.blackboard?.SEED).toEqual({
      status: "success",
      exit_code: 0,
      updated: ["mailbox", "page_prefix"],
    });
  });

  it("renders args against the prior states' outputs, as an Agent state's input is rendered", async () => {
    activityMocks.runRepositoryActionActivity.mockResolvedValue({
      commit_sha: "abc1234",
      branch: "aegis/run-exec-123",
      ref: "main",
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          COMMIT: {
            kind: "Action",
            action: "repository_commit",
            args: { message: "the-forge: {{input.task}}" },
            transitions: [{ condition: "on_success", target: "COMPLETE" }],
          },
          COMPLETE: {
            kind: "Action",
            action: "update_blackboard",
            args: {
              outcome: "committed {{COMMIT.output.commit_sha}}",
              branch: "{{blackboard.COMMIT.output.branch}}",
            },
            transitions: [],
          },
        },
        "COMMIT",
      ),
    );

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { task: "name the judge" },
    });

    expect(result.status).toBe("completed");
    expect(result.blackboard?.outcome).toBe("committed abc1234");
    expect(result.blackboard?.branch).toBe("aegis/run-exec-123");
  });

  it.each([
    "input",
    "intent",
    "tenant_id",
    "attachments",
    "workflow",
    "blackboard",
    "SEED",
    "RECONCILE",
  ])(
    "update_blackboard refuses the reserved key '%s', fails the state and writes nothing",
    async (key) => {
      const definition = seedDefinition();
      definition.states.SEED.args = {
        mailbox: "{{input.mailbox}}",
        [key]: "overwritten",
      };
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(definition);

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { mailbox: "jeshua@100monkeys.ai" },
      });

      expect(result.status).toBe("failed");
      expect(result.error).toBe(
        `update_blackboard cannot write the reserved key '${key}'.`,
      );
      expect(result.final_state).toBe("SEED");
      expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
      expect(activityMocks.executeAgentActivity).not.toHaveBeenCalled();
      expect(result.blackboard?.mailbox).toBeUndefined();
      expect(result.blackboard?.SEED).toBeUndefined();
    },
  );

  it.each([
    ["repository_diff", "diff", undefined, undefined],
    [
      "repository_commit",
      "commit",
      { message: "the-forge: {{input.task}}" },
      "the-forge: name the judge",
    ],
    ["repository_land", "land", undefined, undefined],
  ])(
    "'%s' calls the repository action '%s' for the run, and never a system command",
    async (name, action, args, message) => {
      activityMocks.runRepositoryActionActivity.mockResolvedValue({
        commit_sha: action === "diff" ? undefined : "abc1234",
        branch: "aegis/run-exec-123",
        ref: "main",
        diff: action === "diff" ? "diff --git a/x b/x" : undefined,
      });
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            STEP: {
              kind: "Action",
              action: name,
              ...(args === undefined ? {} : { args }),
              transitions: [],
            },
          },
          "STEP",
        ),
      );

      const result = await aegis_workflow({
        workflow_id: "wf-1",
        input: { task: "name the judge" },
        tenant_id: "tenant-a",
      });

      expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
      expect(activityMocks.runRepositoryActionActivity).toHaveBeenCalledTimes(
        1,
      );
      expect(activityMocks.runRepositoryActionActivity).toHaveBeenCalledWith({
        workflow_execution_id: "exec-123",
        action,
        message,
        tenant_id: "tenant-a",
      });
      expect(result.status).toBe("completed");
      expect(result.blackboard?.STEP).toEqual({
        status: "success",
        exit_code: 0,
        output: {
          commit_sha: action === "diff" ? undefined : "abc1234",
          branch: "aegis/run-exec-123",
          ref: "main",
          diff: action === "diff" ? "diff --git a/x b/x" : undefined,
        },
      });
    },
  );

  it("a repository answer's sentence fails the state with that sentence, and on_failure takes it", async () => {
    const sentence =
      "the remote branch 'main' has commits this run does not have; nothing was landed";
    activityMocks.runRepositoryActionActivity.mockResolvedValue({
      branch: "aegis/run-exec-123",
      ref: "main",
      sentence,
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          LAND: {
            kind: "Action",
            action: "repository_land",
            transitions: [
              { condition: "on_success", target: "COMPLETE" },
              { condition: "on_failure", target: "LAND_REFUSED" },
            ],
          },
          COMPLETE: {
            kind: "Action",
            action: "update_blackboard",
            args: { outcome: "landed" },
            transitions: [],
          },
          LAND_REFUSED: {
            kind: "Action",
            action: "update_blackboard",
            args: { outcome: "{{{LAND.output.sentence}}}" },
            transitions: [],
          },
        },
        "LAND",
      ),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.final_state).toBe("LAND_REFUSED");
    expect(result.blackboard?.LAND).toEqual({
      status: "failed",
      exit_code: 1,
      error: sentence,
      output: {
        commit_sha: undefined,
        branch: "aegis/run-exec-123",
        ref: "main",
        diff: undefined,
        sentence,
      },
    });
    expect(result.blackboard?.outcome).toBe(sentence);
    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
  });

  it("an unknown action fails the state with a sentence naming the actions, and nothing runs", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          STEP: {
            kind: "Action",
            action: "repository_push",
            args: {},
            transitions: [],
          },
        },
        "STEP",
      ),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.status).toBe("failed");
    expect(result.final_state).toBe("STEP");
    expect(result.error).toBe(
      "unknown action 'repository_push'; the actions are update_blackboard, repository_diff, repository_commit, repository_land, image_generate",
    );
    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    expect(activityMocks.runRepositoryActionActivity).not.toHaveBeenCalled();
    expect(activityMocks.runImageGenerateActivity).not.toHaveBeenCalled();
    expect(result.blackboard?.STEP).toBeUndefined();
  });

  // `image_generate` runs through the orchestrator's RunImageGenerate: the
  // rendered args go to the run's volume, and the answer's files are the
  // state's output.
  const generatedFile = {
    volume_id: "vol-run-1",
    path: "generated/1.png",
    content_type: "image/png",
    size_bytes: 897323,
  };

  it("'image_generate' sends the rendered prompt, alias, size, count and path for the run, and records the answer's files", async () => {
    activityMocks.runImageGenerateActivity.mockResolvedValue({
      files: [generatedFile],
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          DRAW: {
            kind: "Action",
            action: "image_generate",
            args: {
              prompt: "a monkey reading {{input.book}}",
              alias: "{{input.alias}}",
              size: "1024x1024",
              count: "{{input.count}}",
              path: "covers/{{input.book}}/",
            },
            transitions: [{ condition: "on_success", target: "COMPLETE" }],
          },
          COMPLETE: {
            kind: "Action",
            action: "update_blackboard",
            args: { first: "{{DRAW.output.files.[0].path}}" },
            transitions: [],
          },
        },
        "DRAW",
      ),
    );

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { book: "walden", alias: "image", count: "2" },
      tenant_id: "tenant-a",
    });

    expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
    expect(activityMocks.runRepositoryActionActivity).not.toHaveBeenCalled();
    expect(activityMocks.runImageGenerateActivity).toHaveBeenCalledTimes(1);
    expect(activityMocks.runImageGenerateActivity.mock.calls[0][0]).toEqual({
      workflow_execution_id: "exec-123",
      prompt: "a monkey reading walden",
      alias: "image",
      size: "1024x1024",
      count: "2",
      path: "covers/walden/",
      tenant_id: "tenant-a",
    });
    expect(result.status).toBe("completed");
    expect(result.blackboard?.DRAW).toEqual({
      status: "success",
      exit_code: 0,
      output: { files: [generatedFile] },
    });
    expect(result.blackboard?.first).toBe("generated/1.png");
  });

  it("'image_generate' sends only the arguments the state names, and none rendered empty", async () => {
    activityMocks.runImageGenerateActivity.mockResolvedValue({
      files: [generatedFile],
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          DRAW: {
            kind: "Action",
            action: "image_generate",
            args: { prompt: "a monkey", size: "{{input.size}}" },
            transitions: [],
          },
        },
        "DRAW",
      ),
    );

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    // The definition's tenant is the run's when the input names none.
    expect(activityMocks.runImageGenerateActivity.mock.calls[0][0]).toEqual({
      workflow_execution_id: "exec-123",
      prompt: "a monkey",
      tenant_id: "local",
    });
  });

  it("an image answer's sentence fails the state with that sentence, and on_failure takes it", async () => {
    const sentence = "this node has no image generation model";
    activityMocks.runImageGenerateActivity.mockResolvedValue({
      files: [],
      sentence,
    });
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          DRAW: {
            kind: "Action",
            action: "image_generate",
            args: { prompt: "a monkey" },
            transitions: [
              { condition: "on_success", target: "COMPLETE" },
              { condition: "on_failure", target: "REFUSED" },
            ],
          },
          COMPLETE: {
            kind: "Action",
            action: "update_blackboard",
            args: { outcome: "drawn" },
            transitions: [],
          },
          REFUSED: {
            kind: "Action",
            action: "update_blackboard",
            args: { outcome: "{{{DRAW.output.sentence}}}" },
            transitions: [],
          },
        },
        "DRAW",
      ),
    );

    const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(result.final_state).toBe("REFUSED");
    expect(result.blackboard?.DRAW).toEqual({
      status: "failed",
      exit_code: 1,
      error: sentence,
      output: { files: [], sentence },
    });
    expect(result.blackboard?.outcome).toBe(sentence);
  });

  it.each([
    ["no prompt", undefined],
    ["an empty prompt", { prompt: "" }],
    ["a prompt that renders empty", { prompt: "{{input.nothing}}" }],
    ["a prompt of spaces", { prompt: "   " }],
  ])(
    "'image_generate' with %s fails the state before any call",
    async (_form, args) => {
      activityMocks.fetchWorkflowDefinition.mockResolvedValue(
        baseDefinition(
          {
            DRAW: {
              kind: "Action",
              action: "image_generate",
              ...(args === undefined ? {} : { args }),
              transitions: [],
            },
          },
          "DRAW",
        ),
      );

      const result = await aegis_workflow({ workflow_id: "wf-1", input: {} });

      expect(result.status).toBe("failed");
      expect(result.final_state).toBe("DRAW");
      expect(result.error).toBe("'prompt' is required: what the image shows");
      expect(activityMocks.runImageGenerateActivity).not.toHaveBeenCalled();
      expect(activityMocks.executeSystemCommandActivity).not.toHaveBeenCalled();
      expect(result.blackboard?.DRAW).toBeUndefined();
    },
  );
});

// A System state runs a shell command only. One whose rendered command is an
// action's name is refused before any process starts, and never reaches a
// shell or the orchestrator's repository action.
describe("a System state written as an action is refused", () => {
  beforeEach(() => {
    for (const fn of Object.values(activityMocks)) {
      fn.mockReset();
    }
    activityMocks.publishEventActivity.mockResolvedValue(undefined);
    activityMocks.executeSystemCommandActivity.mockResolvedValue({
      status: "success",
      exit_code: 0,
      stdout: "",
      stderr: "",
    });
    activityMocks.runRepositoryActionActivity.mockResolvedValue({
      commit_sha: "abc1234",
      branch: "aegis/run-exec-123",
      ref: "main",
    });
    activityMocks.executeAgentActivity.mockResolvedValue({
      status: "completed",
      output: "{}",
      iterations: 1,
    });
  });

  const names = [
    "update_blackboard",
    "repository_diff",
    "repository_commit",
    "repository_land",
    "image_generate",
  ];

  const forms: Array<
    [string, (name: string) => string, Record<string, unknown>]
  > = [
    ["written literally", (name) => name, {}],
    ["written with surrounding space", (name) => `  ${name}\n`, {}],
    ["rendered from a template", () => "{{blackboard.step_command}}", {}],
  ];

  for (const name of names) {
    it.each(forms)(
      `'${name}' %s fails the state with the refusal and starts no process`,
      async (_form, command) => {
        activityMocks.fetchWorkflowDefinition.mockResolvedValue(
          baseDefinition(
            {
              STEP: {
                kind: "System",
                command: command(name),
                env: { message: "the-forge: {{input.task}}" },
                transitions: [
                  { condition: "on_success", target: "AFTER" },
                  { condition: "on_failure", target: "AFTER" },
                ],
              },
              AFTER: {
                kind: "Agent",
                agent: "after",
                input: "after",
                transitions: [],
              },
            },
            "STEP",
          ),
        );

        const result = await aegis_workflow({
          workflow_id: "wf-1",
          input: { task: "name the judge" },
          blackboard: { step_command: name },
        });

        expect(result.status).toBe("failed");
        expect(result.final_state).toBe("STEP");
        expect(result.error).toBe(
          `a System state runs a shell command; '${name}' is an action: write kind: Action, action: ${name}`,
        );
        expect(
          activityMocks.executeSystemCommandActivity,
        ).toHaveBeenCalledTimes(0);
        expect(activityMocks.runRepositoryActionActivity).toHaveBeenCalledTimes(
          0,
        );
        expect(activityMocks.executeAgentActivity).toHaveBeenCalledTimes(0);
        expect(result.blackboard?.STEP).toBeUndefined();
        expect(result.blackboard?.message).toBeUndefined();
      },
    );
  }

  it("an ordinary command still runs through the system command activity", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          RUN: {
            kind: "System",
            command: "{{blackboard.step_command}} --stat",
            env: { mailbox: "{{input.mailbox}}" },
            transitions: [],
          },
        },
        "RUN",
      ),
    );

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { mailbox: "jeshua@100monkeys.ai" },
      blackboard: { step_command: "repository_diff" },
    });

    expect(result.status).toBe("completed");
    expect(activityMocks.runRepositoryActionActivity).toHaveBeenCalledTimes(0);
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledTimes(1);
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        command: "repository_diff --stat",
        env: { mailbox: "jeshua@100monkeys.ai" },
      }),
    );
  });

  it("a command that only begins with an action's name still runs: update_blackboard --dry-run", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          RUN: {
            kind: "System",
            command: "update_blackboard --dry-run",
            env: { mailbox: "{{input.mailbox}}" },
            transitions: [],
          },
        },
        "RUN",
      ),
    );

    const result = await aegis_workflow({
      workflow_id: "wf-1",
      input: { mailbox: "jeshua@100monkeys.ai" },
    });

    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        command: "update_blackboard --dry-run",
        env: { mailbox: "jeshua@100monkeys.ai" },
      }),
    );
    expect(result.blackboard?.mailbox).toBeUndefined();
  });

  it("a command that only begins with an action's name still runs: repository_diff --stat", async () => {
    activityMocks.fetchWorkflowDefinition.mockResolvedValue(
      baseDefinition(
        {
          RUN: {
            kind: "System",
            command: "repository_diff --stat",
            transitions: [],
          },
        },
        "RUN",
      ),
    );

    await aegis_workflow({ workflow_id: "wf-1", input: {} });

    expect(activityMocks.runRepositoryActionActivity).not.toHaveBeenCalled();
    expect(activityMocks.executeSystemCommandActivity).toHaveBeenCalledWith(
      expect.objectContaining({ command: "repository_diff --stat" }),
    );
  });
});
