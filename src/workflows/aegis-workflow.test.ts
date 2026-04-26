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
      executeOutputHandlerActivity: vi.fn(),
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
