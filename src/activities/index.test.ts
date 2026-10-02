import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ExecuteContainerRunResponse } from "../types.js";

const { executeContainerRunMock, executeAgentMock } = vi.hoisted(() => ({
  executeContainerRunMock: vi.fn(),
  executeAgentMock: vi.fn(),
}));

vi.mock("../grpc/client.js", () => ({
  aegisRuntimeClient: {
    executeContainerRun: executeContainerRunMock,
    executeAgent: executeAgentMock,
    executeSystemCommand: vi.fn(),
    validateWithJudges: vi.fn(),
    storeTrajectoryPattern: vi.fn(),
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

vi.mock("./workflow-activities.js", () => ({
  fetchWorkflowDefinition: vi.fn(),
}));

import {
  executeAgentActivity,
  executeParallelAgentsActivity,
  executeParallelContainerRunActivity,
} from "./index.js";

function ok(
  exit_code: number,
  name = "step",
): ExecuteContainerRunResponse & { name?: string } {
  return {
    exit_code,
    stdout: `${name}-stdout`,
    stderr: `${name}-stderr`,
    duration_ms: 10,
    attempts: 1,
  };
}

describe("Temporal activities", () => {
  beforeEach(() => {
    executeContainerRunMock.mockReset();
    executeAgentMock.mockReset();
  });

  it("sends workflow execution lineage without parent execution semantics", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-1",
        timestamp: "2026-03-22T08:32:12.572760Z",
        final_output: "done",
        total_iterations: 1,
      },
    ]);

    const result = await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "plan",
      context: {},
      workflowExecutionId: "wf-exec-1",
    });

    expect(result).toMatchObject({
      status: "completed",
      output: "done",
      iterations: 1,
    });

    expect(executeAgentMock).toHaveBeenCalledWith(
      expect.objectContaining({
        agent_id: "123e4567-e89b-12d3-a456-426614174000",
        workflow_execution_id: "wf-exec-1",
      }),
    );

    const request = executeAgentMock.mock.calls[0][0];
    expect(request.parent_execution_id).toBeUndefined();
  });

  it("returns completed result when the client synthesizes terminal completion from persisted state", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "IterationCompleted",
        execution_id: "child-exec-2",
        timestamp: "2026-03-22T08:32:12.572760Z",
        iteration_number: 1,
        output: '{"deployed":true}',
      },
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-2",
        timestamp: "2026-03-22T08:32:22.572760Z",
        final_output: '{"deployed":true}',
        total_iterations: 1,
      },
    ]);

    const result = await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "register workflow",
      context: {},
      workflowExecutionId: "wf-exec-2",
    });

    expect(result).toEqual({
      status: "completed",
      output: { deployed: true },
      iterations: 1,
      execution_id: "child-exec-2",
    });
  });

  it("returns failure result (no throw) for all_succeed when any step fails", async () => {
    executeContainerRunMock
      .mockResolvedValueOnce(ok(0, "unit"))
      .mockResolvedValueOnce(ok(2, "lint"));

    const result = await executeParallelContainerRunActivity({
      execution_id: "exec-1",
      state_name: "TEST",
      completion: "all_succeed",
      steps: [
        { name: "unit", image: "alpine", command: ["true"] },
        { name: "lint", image: "alpine", command: ["false"] },
      ],
    });

    expect(result.overall_success).toBe(false);
    expect(result.completion).toBe("all_succeed");
    expect(result.succeeded).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.results).toHaveLength(2);
  });

  it("returns failure result (no throw) for any_succeed when all steps fail", async () => {
    executeContainerRunMock.mockResolvedValue(ok(3));

    const result = await executeParallelContainerRunActivity({
      execution_id: "exec-2",
      state_name: "TEST",
      completion: "any_succeed",
      steps: [
        { name: "unit", image: "alpine", command: ["false"] },
        { name: "lint", image: "alpine", command: ["false"] },
      ],
    });

    expect(result.overall_success).toBe(false);
    expect(result.completion).toBe("any_succeed");
    expect(result.succeeded).toBe(0);
    expect(result.failed).toBe(2);
  });

  it("returns success for best_effort even when all steps fail", async () => {
    executeContainerRunMock.mockResolvedValue(ok(1));

    const result = await executeParallelContainerRunActivity({
      execution_id: "exec-3",
      state_name: "TEST",
      completion: "best_effort",
      steps: [
        { name: "unit", image: "alpine", command: ["false"] },
        { name: "lint", image: "alpine", command: ["false"] },
      ],
    });

    expect(result.overall_success).toBe(true);
    expect(result.succeeded).toBe(0);
    expect(result.failed).toBe(2);
  });

  it("converts rejected step call into non-zero step result and preserves aggregation", async () => {
    executeContainerRunMock
      .mockResolvedValueOnce(ok(0, "unit"))
      .mockRejectedValueOnce(new Error("grpc unavailable"));

    const result = await executeParallelContainerRunActivity({
      execution_id: "exec-4",
      state_name: "TEST",
      completion: "all_succeed",
      steps: [
        { name: "unit", image: "alpine", command: ["true"] },
        { name: "lint", image: "alpine", command: ["false"] },
      ],
    });

    expect(result.overall_success).toBe(false);
    const lint = result.results.find((r) => r.name === "lint");
    expect(lint).toBeDefined();
    expect(lint?.exit_code).toBe(1);
    expect(lint?.stderr).toContain("grpc unavailable");
  });

  // Regression: ADR-100 D5 — tenantId must be an explicit typed param, not read from Blackboard
  it("threads explicit tenantId param into the gRPC tenant_id field", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-tenant",
        timestamp: "2026-04-06T00:00:00.000Z",
        final_output: "ok",
        total_iterations: 1,
      },
    ]);

    await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "task",
      context: {},
      tenantId: "tenant-abc",
    });

    const request = executeAgentMock.mock.calls[0][0];
    expect(request.tenant_id).toBe("tenant-abc");
  });

  // ADR-113: structured attachment refs must land on ExecuteAgentRequest.attachments
  it("forwards non-empty attachments onto the gRPC request", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-attachments",
        timestamp: "2026-04-26T00:00:00.000Z",
        final_output: "ok",
        total_iterations: 1,
      },
    ]);

    const attachments = [
      {
        volume_id: "chat-attachments",
        path: "uploads/2026-04/abc.pdf",
        name: "abc.pdf",
        mime_type: "application/pdf",
        size: 12345,
        sha256: "deadbeef",
      },
    ];

    await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "task",
      context: {},
      attachments,
    });

    const request = executeAgentMock.mock.calls[0][0];
    expect(request.attachments).toEqual(attachments);
  });

  it("omits attachments from the gRPC request when none are provided", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-no-attachments",
        timestamp: "2026-04-26T00:00:00.000Z",
        final_output: "ok",
        total_iterations: 1,
      },
    ]);

    await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "task",
      context: {},
    });

    const request = executeAgentMock.mock.calls[0][0];
    expect(request.attachments).toBeUndefined();
  });

  it("omits attachments from the gRPC request when an empty array is provided", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-empty-attachments",
        timestamp: "2026-04-26T00:00:00.000Z",
        final_output: "ok",
        total_iterations: 1,
      },
    ]);

    await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "task",
      context: {},
      attachments: [],
    });

    const request = executeAgentMock.mock.calls[0][0];
    expect(request.attachments).toBeUndefined();
  });

  it("does not read tenant_id from Blackboard context when tenantId param is provided", async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: "ExecutionCompleted",
        execution_id: "child-exec-tenant-isolation",
        timestamp: "2026-04-06T00:00:00.000Z",
        final_output: "ok",
        total_iterations: 1,
      },
    ]);

    await executeAgentActivity({
      agentId: "123e4567-e89b-12d3-a456-426614174000",
      input: "task",
      context: { tenant_id: "blackboard-tenant" },
      tenantId: "explicit-tenant",
    });

    const request = executeAgentMock.mock.calls[0][0];
    // Must use the explicit param, not the opaque Blackboard value
    expect(request.tenant_id).toBe("explicit-tenant");
  });
});

describe("ParallelAgents consensus without judges_for_parallel", () => {
  const AGENT_A = "123e4567-e89b-12d3-a456-426614174001";
  const AGENT_B = "123e4567-e89b-12d3-a456-426614174002";

  beforeEach(() => {
    executeAgentMock.mockReset();
  });

  function completedWith(final_output: string) {
    return [
      {
        event_type: "ExecutionCompleted",
        execution_id: "child",
        timestamp: "2026-10-02T01:43:43Z",
        final_output,
        total_iterations: 1,
      },
    ];
  }

  function run(strategy = "weighted_average", threshold = 0.8) {
    return executeParallelAgentsActivity({
      agents: [
        { agent: AGENT_A, input: "{}", weight: 1.0 },
        { agent: AGENT_B, input: "{}", weight: 1.0 },
      ],
      consensus: { strategy, threshold },
    });
  }

  it("combines two agents' verdicts 0.93 and 0.95 into their weighted average", async () => {
    executeAgentMock
      .mockResolvedValueOnce(
        completedWith(
          '```json\n{"score": 0.93, "confidence": 0.9, "reasoning": "meets the rubric"}\n```',
        ),
      )
      .mockResolvedValueOnce(
        completedWith(
          '{"score": 0.95, "confidence": 0.9, "reasoning": "good"}',
        ),
      );

    const result = await run();

    expect(result.consensus.score).toBeCloseTo(0.94, 10);
    // The orchestrator's weighted_average: 0.7 x agreement + 0.3 x mean confidence.
    expect(result.consensus.confidence).toBeCloseTo(
      0.7 * (1 - 0.0001 / 0.25) + 0.3 * 0.9,
      10,
    );
    expect(result.consensus.strategy).toBe("weighted_average");
    expect(result.consensus.metadata.individual_results).toEqual([
      {
        agent: AGENT_A,
        score: 0.93,
        confidence: 0.9,
        reasoning: "meets the rubric",
      },
      { agent: AGENT_B, score: 0.95, confidence: 0.9, reasoning: "good" },
    ]);
    expect(result.consensus.metadata.reasoning).not.toContain(
      "No judge agents configured",
    );
  });

  it.each([
    ["majority_vote", 1.0],
    ["unanimous", 0.94],
    ["best_of_n", 0.94],
  ])("combines the verdicts by %s", async (strategy, expected) => {
    executeAgentMock
      .mockResolvedValueOnce(
        completedWith('{"score": 0.93, "confidence": 0.9}'),
      )
      .mockResolvedValueOnce(
        completedWith('{"score": 0.95, "confidence": 0.9}'),
      );

    const result = await run(strategy, 0.8);

    expect(result.consensus.score).toBeCloseTo(expected, 10);
    expect(result.consensus.strategy).toBe(strategy);
  });

  it("says there is no score to combine when no agent returns a verdict", async () => {
    executeAgentMock
      .mockResolvedValueOnce(completedWith("The code looks fine to me."))
      .mockResolvedValueOnce(completedWith('{"verdict": "pass"}'));

    const result = await run();

    expect(result.consensus.score).toBeUndefined();
    expect(result.consensus.confidence).toBeUndefined();
    expect(result.consensus.metadata.individual_results).toEqual([]);
    expect(result.consensus.metadata.agents_without_verdict).toEqual([
      AGENT_A,
      AGENT_B,
    ]);
    expect(result.consensus.metadata.reasoning).toBe(
      "No judge agents are configured for this ParallelAgents state and none of its agents returned a verdict (a JSON object with a score from 0.0 to 1.0): there is no score to combine.",
    );
  });
});
