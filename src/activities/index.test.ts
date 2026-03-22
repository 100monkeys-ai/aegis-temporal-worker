import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ExecuteContainerRunResponse } from '../types.js';

const { executeContainerRunMock, executeAgentMock } = vi.hoisted(() => ({
  executeContainerRunMock: vi.fn(),
  executeAgentMock: vi.fn(),
}));

vi.mock('../grpc/client.js', () => ({
  aegisRuntimeClient: {
    executeContainerRun: executeContainerRunMock,
    executeAgent: executeAgentMock,
    executeSystemCommand: vi.fn(),
    validateWithJudges: vi.fn(),
    storeTrajectoryPattern: vi.fn(),
  },
}));

vi.mock('../logger.js', () => ({
  logger: {
    info: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
  },
}));

vi.mock('./workflow-activities.js', () => ({
  fetchWorkflowDefinition: vi.fn(),
}));

import { executeAgentActivity, executeParallelContainerRunActivity } from './index.js';

function ok(exit_code: number, name = 'step'): ExecuteContainerRunResponse & { name?: string } {
  return {
    exit_code,
    stdout: `${name}-stdout`,
    stderr: `${name}-stderr`,
    duration_ms: 10,
    attempts: 1,
  };
}

describe('Temporal activities', () => {
  beforeEach(() => {
    executeContainerRunMock.mockReset();
    executeAgentMock.mockReset();
  });

  it('sends workflow execution lineage without parent execution semantics', async () => {
    executeAgentMock.mockResolvedValue([
      {
        event_type: 'ExecutionCompleted',
        execution_id: 'child-exec-1',
        timestamp: '2026-03-22T08:32:12.572760Z',
        final_output: 'done',
        total_iterations: 1,
      },
    ]);

    const result = await executeAgentActivity({
      agentId: '123e4567-e89b-12d3-a456-426614174000',
      input: 'plan',
      context: {},
      workflowExecutionId: 'wf-exec-1',
    });

    expect(result).toMatchObject({
      status: 'completed',
      output: 'done',
      iterations: 1,
    });

    expect(executeAgentMock).toHaveBeenCalledWith(
      expect.objectContaining({
        agent_id: '123e4567-e89b-12d3-a456-426614174000',
        workflow_execution_id: 'wf-exec-1',
      })
    );

    const request = executeAgentMock.mock.calls[0][0];
    expect(request.parent_execution_id).toBeUndefined();
  });

  it('returns failure result (no throw) for all_succeed when any step fails', async () => {
    executeContainerRunMock
      .mockResolvedValueOnce(ok(0, 'unit'))
      .mockResolvedValueOnce(ok(2, 'lint'));

    const result = await executeParallelContainerRunActivity({
      execution_id: 'exec-1',
      state_name: 'TEST',
      completion: 'all_succeed',
      steps: [
        { name: 'unit', image: 'alpine', command: ['true'] },
        { name: 'lint', image: 'alpine', command: ['false'] },
      ],
    });

    expect(result.overall_success).toBe(false);
    expect(result.completion).toBe('all_succeed');
    expect(result.succeeded).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.results).toHaveLength(2);
  });

  it('returns failure result (no throw) for any_succeed when all steps fail', async () => {
    executeContainerRunMock.mockResolvedValue(ok(3));

    const result = await executeParallelContainerRunActivity({
      execution_id: 'exec-2',
      state_name: 'TEST',
      completion: 'any_succeed',
      steps: [
        { name: 'unit', image: 'alpine', command: ['false'] },
        { name: 'lint', image: 'alpine', command: ['false'] },
      ],
    });

    expect(result.overall_success).toBe(false);
    expect(result.completion).toBe('any_succeed');
    expect(result.succeeded).toBe(0);
    expect(result.failed).toBe(2);
  });

  it('returns success for best_effort even when all steps fail', async () => {
    executeContainerRunMock.mockResolvedValue(ok(1));

    const result = await executeParallelContainerRunActivity({
      execution_id: 'exec-3',
      state_name: 'TEST',
      completion: 'best_effort',
      steps: [
        { name: 'unit', image: 'alpine', command: ['false'] },
        { name: 'lint', image: 'alpine', command: ['false'] },
      ],
    });

    expect(result.overall_success).toBe(true);
    expect(result.succeeded).toBe(0);
    expect(result.failed).toBe(2);
  });

  it('converts rejected step call into non-zero step result and preserves aggregation', async () => {
    executeContainerRunMock
      .mockResolvedValueOnce(ok(0, 'unit'))
      .mockRejectedValueOnce(new Error('grpc unavailable'));

    const result = await executeParallelContainerRunActivity({
      execution_id: 'exec-4',
      state_name: 'TEST',
      completion: 'all_succeed',
      steps: [
        { name: 'unit', image: 'alpine', command: ['true'] },
        { name: 'lint', image: 'alpine', command: ['false'] },
      ],
    });

    expect(result.overall_success).toBe(false);
    const lint = result.results.find((r) => r.name === 'lint');
    expect(lint).toBeDefined();
    expect(lint?.exit_code).toBe(1);
    expect(lint?.stderr).toContain('grpc unavailable');
  });
});
