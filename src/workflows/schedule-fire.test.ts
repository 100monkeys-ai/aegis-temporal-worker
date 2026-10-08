// aegis_schedule_fire (AEGIS ADR-139 N5, N6), run with the real
// fireScheduleActivity against a stand-in orchestrator.
//
// The worker's workflow tests mock @temporalio/workflow (as
// aegis-workflow.test.ts does); there is no Temporal test server in this
// harness. `proxyActivities` is replaced by `retryLikeTemporal`, which applies
// the retry policy the workflow passed the way Temporal's server does: the
// activity is attempted until it succeeds, throws a non-retryable
// ApplicationFailure, or reaches `maximumAttempts`, and the last failure comes
// back to the workflow inside an ActivityFailure. The backoff intervals are
// not waited.
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { proxyOptions, scheduledStart, activityCalls } = vi.hoisted(() => ({
  proxyOptions: [] as Array<Record<string, any>>,
  scheduledStart: { value: null as Date | null },
  activityCalls: [] as unknown[],
}));

vi.mock("../auth/token-manager.js", () => ({
  getServiceToken: vi.fn(async () => "service-token-1"),
}));

vi.mock("../logger.js", () => ({
  logger: { info: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn() },
}));

vi.mock("@temporalio/workflow", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@temporalio/workflow")>();
  const { fireScheduleActivity } =
    await import("../activities/schedule-activities.js");

  async function retryLikeTemporal(
    options: Record<string, any>,
    fn: (...args: any[]) => Promise<unknown>,
    args: unknown[],
  ): Promise<unknown> {
    const maximumAttempts: number = options.retry?.maximumAttempts ?? Infinity;
    let attempt = 0;
    for (;;) {
      attempt += 1;
      try {
        return await fn(...args);
      } catch (error) {
        const nonRetryable =
          error instanceof actual.ApplicationFailure && error.nonRetryable;
        if (nonRetryable || attempt >= maximumAttempts) {
          throw new actual.ActivityFailure(
            "Activity task failed",
            "fireScheduleActivity",
            "1",
            nonRetryable ? "NON_RETRYABLE_FAILURE" : "MAXIMUM_ATTEMPTS_REACHED",
            "test",
            error as Error,
          );
        }
      }
    }
  }

  return {
    ...actual,
    proxyActivities: vi.fn((options: Record<string, any>) => {
      proxyOptions.push(options);
      return {
        fireScheduleActivity: (...args: unknown[]) => {
          activityCalls.push(args);
          return retryLikeTemporal(options, fireScheduleActivity, args);
        },
      };
    }),
    workflowInfo: vi.fn(() => ({
      workflowId: "aegis-schedule-fire-1",
      typedSearchAttributes: {
        getAll: () =>
          scheduledStart.value === null
            ? []
            : [
                {
                  key: { name: "TemporalScheduledStartTime", type: "DATETIME" },
                  value: scheduledStart.value,
                },
              ],
      },
    })),
  };
});

import {
  aegis_schedule_fire,
  CATCHUP_WINDOW_MS,
  FIRE_MAXIMUM_ATTEMPTS,
  type ScheduleFireResult,
} from "./schedule-fire.js";

const SCHEDULE_ID = "6f1c2b9e-0a4d-4b8e-9a51-2f3c4d5e6f70";
const TENANT_ID = "tenant-a";

/**
 * Run the workflow and assert it ended, returning its result, rather than
 * failing: `why` names the outcome it must end after.
 */
async function runToEnd(why: string): Promise<ScheduleFireResult> {
  let failure: unknown = undefined;
  const result = await aegis_schedule_fire({
    schedule_id: SCHEDULE_ID,
    tenant_id: TENANT_ID,
  }).catch((e: unknown) => {
    failure = e;
    return undefined;
  });
  expect(
    failure,
    `the workflow must end after ${why}, not fail`,
  ).toBeUndefined();
  return result as ScheduleFireResult;
}

interface StandIn {
  url: string;
  requests: Array<{ url?: string; tenant?: string; body: string }>;
  close: () => Promise<void>;
}

/** A stand-in orchestrator: per request, a status to answer or "drop". */
async function startStandIn(
  answer: (n: number) => number | "drop",
): Promise<StandIn> {
  const requests: StandIn["requests"] = [];
  const server: Server = createServer((req, res) => {
    let body = "";
    req.on("data", (c: Buffer) => (body += c.toString()));
    req.on("end", () => {
      requests.push({
        url: req.url,
        tenant: req.headers["x-tenant-id"] as string | undefined,
        body,
      });
      const a = answer(requests.length);
      if (a === "drop") {
        req.socket.destroy();
        return;
      }
      res.statusCode = a;
      res.setHeader("Content-Type", "application/json");
      res.end(
        JSON.stringify(a < 300 ? { outcome: "started" } : { error: "no" }),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

describe("aegis_schedule_fire", () => {
  let standIn: StandIn | undefined;
  const savedUrl = process.env.AEGIS_ORCHESTRATOR_URL;

  beforeEach(() => {
    proxyOptions.length = 0;
    activityCalls.length = 0;
    scheduledStart.value = new Date(Date.now() - 5_000);
  });

  afterEach(async () => {
    await standIn?.close();
    standIn = undefined;
    if (savedUrl === undefined) delete process.env.AEGIS_ORCHESTRATOR_URL;
    else process.env.AEGIS_ORCHESTRATOR_URL = savedUrl;
  });

  async function serve(answer: (n: number) => number | "drop") {
    standIn = await startStandIn(answer);
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
    return standIn;
  }

  it("calls fireScheduleActivity once with the ids and Temporal's scheduled time", async () => {
    const server = await serve(() => 200);
    const scheduled = scheduledStart.value as Date;

    const result = await runToEnd("a fire the orchestrator took");

    expect(
      activityCalls,
      "the workflow calls the activity exactly once",
    ).toEqual([
      [
        {
          schedule_id: SCHEDULE_ID,
          tenant_id: TENANT_ID,
          scheduled_time: scheduled.toISOString(),
        },
      ],
    ]);
    expect(server.requests).toHaveLength(1);
    expect(server.requests[0].url).toBe(
      `/v1/internal/schedules/${SCHEDULE_ID}/fire`,
    );
    expect(server.requests[0].tenant).toBe(TENANT_ID);
    expect(JSON.parse(server.requests[0].body)).toEqual({
      scheduled_time: scheduled.toISOString(),
    });
    expect(
      result,
      "the workflow ends after a fire the orchestrator took",
    ).toEqual({
      schedule_id: SCHEDULE_ID,
      tenant_id: TENANT_ID,
      scheduled_time: scheduled.toISOString(),
      outcome: "fired",
      status: 200,
      answer: { outcome: "started" },
    });
  });

  it("bounds the activity to three attempts inside the catch-up window", async () => {
    await serve(() => 200);
    await runToEnd("a fire the orchestrator took");

    expect(proxyOptions).toHaveLength(1);
    const options = proxyOptions[0];
    expect(FIRE_MAXIMUM_ATTEMPTS).toBe(3);
    expect(options.retry.maximumAttempts, "at most three attempts").toBe(3);
    expect(CATCHUP_WINDOW_MS).toBe(600_000);
    // Scheduled 5 s ago: what is left of the 600 s window, no more.
    expect(options.scheduleToCloseTimeout).toBeLessThanOrEqual(
      CATCHUP_WINDOW_MS - 5_000,
    );
    expect(options.scheduleToCloseTimeout).toBeGreaterThan(
      CATCHUP_WINDOW_MS - 60_000,
    );
  });

  it("makes three attempts on a transport failure and ends", async () => {
    const server = await serve(() => "drop");

    const result = await runToEnd("exhausted retries");

    expect(
      server.requests,
      "three attempts reach the orchestrator's address",
    ).toHaveLength(3);
    expect(activityCalls).toHaveLength(1);
    expect(result.outcome, "the workflow ends after exhausted retries").toBe(
      "unreachable",
    );
    expect(result.status).toBeNull();
  });

  it("fires on the third attempt after two transport failures", async () => {
    const server = await serve((n) => (n < 3 ? "drop" : 200));
    const result = await runToEnd("a fire on the third attempt");
    expect(server.requests).toHaveLength(3);
    expect(result.outcome).toBe("fired");
  });

  it.each([400, 403, 404, 409])(
    "makes one attempt and ends when the orchestrator answers %i",
    async (status) => {
      const server = await serve(() => status);

      const result = await runToEnd(`a ${status}`);

      expect(server.requests, `a ${status} is not retried`).toHaveLength(1);
      expect(result.outcome, `the workflow ends after a ${status}`).toBe(
        "refused",
      );
      expect(result.status).toBe(status);
    },
  );

  it.each([502, 503, 504])(
    "makes three attempts on a %i and ends",
    async (status) => {
      const server = await serve(() => status);

      const result = await runToEnd(`three ${status} answers`);

      expect(
        server.requests,
        `a ${status} is retried to three attempts`,
      ).toHaveLength(3);
      expect(result.outcome).toBe("unreachable");
      expect(result.status).toBe(status);
    },
  );

  it("fires on the second attempt after a 503", async () => {
    const server = await serve((n) => (n === 1 ? 503 : 200));
    const result = await runToEnd("a fire on the second attempt");
    expect(server.requests).toHaveLength(2);
    expect(result.outcome).toBe("fired");
  });

  it.each([500, 501])(
    "makes one attempt and ends when the orchestrator answers %i",
    async (status) => {
      const server = await serve(() => status);
      const result = await runToEnd(`a ${status}`);
      expect(server.requests, `a ${status} is not retried`).toHaveLength(1);
      expect(result.outcome).toBe("refused");
      expect(result.status).toBe(status);
    },
  );

  it("posts nothing when it was not started by a Schedule", async () => {
    const server = await serve(() => 200);
    scheduledStart.value = null;

    const result = await runToEnd("no scheduled time");

    expect(activityCalls).toHaveLength(0);
    expect(server.requests).toHaveLength(0);
    expect(result.outcome).toBe("unscheduled");
  });
});

describe("the workflows module", () => {
  it("registers aegis_schedule_fire beside aegis_workflow", async () => {
    const workflows = await import("./index.js");
    expect(workflows.aegis_workflow).toBeTypeOf("function");
    expect(
      (workflows as Record<string, unknown>).aegis_schedule_fire,
      "src/workflows/index.ts must export aegis_schedule_fire",
    ).toBe(aegis_schedule_fire);
  });
});
