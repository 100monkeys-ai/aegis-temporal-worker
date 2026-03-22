import { beforeEach, describe, expect, it, vi } from 'vitest';

describe('isWorkflowDefinitionRegistrationPayload', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.stubEnv('DATABASE_URL', 'postgresql://aegis:aegis@localhost:5432/aegis');
  });

  it('accepts workflow definitions that include tenant and version', async () => {
    const { isWorkflowDefinitionRegistrationPayload } = await import('./server.js');

    expect(
      isWorkflowDefinitionRegistrationPayload({
        workflow_id: 'wf-1',
        tenant_id: 'local',
        name: 'builtin-workflow-generator',
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
      })
    ).toBe(true);
  });

  it('rejects workflow definitions missing tenant_id', async () => {
    const { isWorkflowDefinitionRegistrationPayload } = await import('./server.js');

    expect(
      isWorkflowDefinitionRegistrationPayload({
        workflow_id: 'wf-1',
        name: 'builtin-workflow-generator',
        version: '1.0.0',
        initial_state: 'START',
        context: {},
        states: {},
      })
    ).toBe(false);
  });
});
