import { EventEmitter } from 'node:events';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  executeAgentRpcMock: vi.fn(),
  closeMock: vi.fn(),
  createInsecureMock: vi.fn(() => 'insecure-creds'),
  runtimeCtorMock: vi.fn(),
  logger: {
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('@grpc/proto-loader', () => ({
  loadSync: vi.fn(() => ({})),
}));

vi.mock('@grpc/grpc-js', () => ({
  credentials: {
    createInsecure: mocks.createInsecureMock,
  },
  loadPackageDefinition: vi.fn(() => ({
    aegis: {
      runtime: {
        v1: {
          AegisRuntime: mocks.runtimeCtorMock,
        },
      },
    },
  })),
}));

vi.mock('../config.js', () => ({
  config: {
    grpc: {
      runtimeUrl: 'runtime:50051',
    },
  },
}));

vi.mock('../logger.js', () => ({
  logger: mocks.logger,
}));

describe('AegisRuntimeClient.executeAgent', () => {
  beforeEach(() => {
    vi.resetModules();
    mocks.executeAgentRpcMock.mockReset();
    mocks.closeMock.mockReset();
    mocks.createInsecureMock.mockClear();
    mocks.runtimeCtorMock.mockReset();
    mocks.runtimeCtorMock.mockImplementation(function () {
      return {
        ExecuteAgent: mocks.executeAgentRpcMock,
        close: mocks.closeMock,
      };
    });

    for (const fn of Object.values(mocks.logger)) {
      fn.mockReset();
    }
  });

  it('resolves when the runtime stream ends after a terminal event', async () => {
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import('./client.js');

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: 'agent-1',
      input: 'plan',
      context_json: '{}',
      timeout_seconds: 300,
    });

    call.emit('data', {
      event: 'execution_completed',
      execution_completed: {
        execution_id: 'exec-1',
        completed_at: '2026-03-22T08:32:12.572760Z',
        final_output: 'done',
        total_iterations: 1,
      },
    });
    call.emit('end');

    await expect(promise).resolves.toMatchObject([
      {
        event_type: 'ExecutionCompleted',
        execution_id: 'exec-1',
        final_output: 'done',
        total_iterations: 1,
      },
    ]);
  });

  it('resolves as soon as a terminal event arrives, before stream end', async () => {
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import('./client.js');

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: 'agent-1',
      input: 'plan',
      context_json: '{}',
      timeout_seconds: 300,
      parent_execution_id: 'parent-1',
    });

    call.emit('data', {
      event: 'execution_completed',
      execution_completed: {
        execution_id: 'exec-2',
        completed_at: '2026-03-22T08:32:12.572760Z',
        final_output: 'done',
        total_iterations: 1,
      },
    });

    await expect(
      Promise.race([
        promise,
        new Promise((resolve) => setTimeout(() => resolve('still-waiting'), 0)),
      ])
    ).resolves.not.toBe('still-waiting');
  });

  it('rejects when the runtime stream errors before completion', async () => {
    const call = new EventEmitter();
    mocks.executeAgentRpcMock.mockReturnValue(call);

    const { aegisRuntimeClient } = await import('./client.js');

    const promise = aegisRuntimeClient.executeAgent({
      agent_id: 'agent-1',
      input: 'plan',
      context_json: '{}',
      timeout_seconds: 300,
    });

    call.emit('error', new Error('stream failed'));

    await expect(promise).rejects.toThrow('stream failed');
  });
});
