import { beforeEach, describe, expect, it, vi } from 'vitest';

const queryMock = vi.fn();

vi.mock('../database.js', () => ({
  database: {
    getWorkflowDefinition: queryMock,
    getWorkflowDefinitionByName: vi.fn(),
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

describe('fetchWorkflowDefinition', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
  });

  it('fetches workflow definitions by UUID', async () => {
    queryMock.mockResolvedValueOnce({
      workflow_id: 'wf-1',
      tenant_id: 'local',
      name: 'ci-workflow',
      version: '1.0.0',
      initial_state: 'START',
      context: {},
      states: {},
    });

    const { fetchWorkflowDefinition } = await import('./workflow-activities.js');
    const definition = await fetchWorkflowDefinition('wf-1');

    expect(queryMock).toHaveBeenCalledWith('wf-1');
    expect(definition.workflow_id).toBe('wf-1');
  });
});
