import { beforeEach, describe, expect, it, vi } from 'vitest';

const queryMock = vi.fn();
const onMock = vi.fn();
const endMock = vi.fn();
const connectMock = vi.fn();

vi.mock('pg', () => ({
  default: {
    Pool: vi.fn().mockImplementation(function MockPool() {
      return {
      query: queryMock,
      on: onMock,
      end: endMock,
      connect: connectMock,
      };
    }),
  },
}));

describe('Database.saveWorkflowDefinition', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.resetModules();
    vi.stubEnv('DATABASE_URL', 'postgresql://aegis:aegis@localhost:5432/aegis');
  });

  it('persists workflow definitions with tenant-aware upsert fields', async () => {
    const { Database } = await import('./database.js');

    queryMock.mockResolvedValueOnce({ rows: [] });

    const database = new Database();
    await database.saveWorkflowDefinition({
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
    });

    expect(queryMock).toHaveBeenCalledTimes(1);
    const [sql, params] = queryMock.mock.calls[0] as [string, unknown[]];

    expect(sql).toContain('tenant_id');
    expect(sql).toContain('version');
    expect(sql).toContain('ON CONFLICT (tenant_id, name, version)');
    expect(params).toEqual([
      'wf-1',
      'local',
      'builtin-workflow-generator',
      '1.0.0',
      expect.any(String),
      expect.any(String),
    ]);
  });
});
