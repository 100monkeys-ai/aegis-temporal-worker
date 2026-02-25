import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('./workflow-generator.js', () => ({
  createWorkflowFromDefinition: vi.fn((definition: { workflow_id: string }) => async () => ({
    status: 'completed',
    workflow_id: definition.workflow_id,
  })),
  getWorkflowName: vi.fn((definition: { name: string }) => `aegis_workflow_${definition.name}`),
}));

import type { TemporalWorkflowDefinition } from './types.js';

const definition: TemporalWorkflowDefinition = {
  workflow_id: 'wf-1',
  name: 'test-workflow',
  version: '1.0.0',
  initial_state: 'START',
  context: {},
  states: {
    START: {
      kind: 'System',
      command: 'echo ok',
      transitions: [],
    },
  },
};

describe('WorkflowRegistry', () => {
  let registry: {
    registerWorkflow: (definition: TemporalWorkflowDefinition) => Promise<void>;
    getWorkflow: (workflowId: string) => TemporalWorkflowDefinition | undefined;
    getWorkflowFunction: (workflowName: string) => unknown;
    unregisterWorkflow: (workflowId: string) => void;
  };

  beforeEach(async () => {
    vi.resetModules();
    process.env.DATABASE_URL = 'postgresql://temporal:temporal@localhost:5432/aegis';

    const module = await import('./workflow-registry.js');
    registry = new module.WorkflowRegistry();
  });

  it('registers and returns workflow definition and function', async () => {
    await registry.registerWorkflow(definition);

    expect(registry.getWorkflow('wf-1')).toEqual(definition);

    const workflowFn = registry.getWorkflowFunction('aegis_workflow_test-workflow');
    expect(workflowFn).toBeTypeOf('function');
  });

  it('unregisters workflow and removes generated function', async () => {
    await registry.registerWorkflow(definition);
    registry.unregisterWorkflow('wf-1');

    expect(registry.getWorkflow('wf-1')).toBeUndefined();
    expect(registry.getWorkflowFunction('aegis_workflow_test-workflow')).toBeUndefined();
  });
});
