import { beforeEach, describe, expect, it, vi } from "vitest";
import type { TemporalWorkflowDefinition } from "../types.js";

const { activityMocks, terminalActivityMocks, executeAgentRpcMock } =
  vi.hoisted(() => ({
    activityMocks: {
      executeAgentActivity: vi.fn(),
      executeSystemCommandActivity: vi.fn(),
      validateOutputActivity: vi.fn(),
      executeParallelAgentsActivity: vi.fn(),
      storeTrajectoryPatternActivity: vi.fn(),
      fetchWorkflowDefinition: vi.fn(),
      publishEventActivity: vi.fn(),
      executeContainerRunActivity: vi.fn(),
      executeParallelContainerRunActivity: vi.fn(),
    },
    terminalActivityMocks: {
      executeAgentActivity: vi.fn(),
    },
    executeAgentRpcMock: vi.fn(),
  }));

vi.mock("../activities/index.js", () => activityMocks);

vi.mock("../activities/workflow-activities.js", () => ({
  fetchWorkflowDefinition: vi.fn(),
}));

vi.mock("../grpc/client.js", () => ({
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
    (options?: { retry?: { maximumAttempts?: number } }) =>
      options?.retry?.maximumAttempts === 1
        ? terminalActivityMocks
        : activityMocks,
  ),
  setHandler: vi.fn(),
  defineSignal: vi.fn(() => Symbol("humanInput")),
  condition: vi.fn(async (predicate: () => boolean) => predicate()),
  workflowInfo: vi.fn(() => ({ workflowId: "exec-123" })),
}));

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
