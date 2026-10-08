// The schedule fire activity against a stand-in for the orchestrator's
// `POST /v1/internal/schedules/{id}/fire` (AEGIS ADR-139 N6). The route is
// not built yet; the stand-in answers in the contract's shape.
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApplicationFailure } from "@temporalio/activity";

const { getServiceTokenMock } = vi.hoisted(() => ({
  getServiceTokenMock: vi.fn(),
}));

vi.mock("../auth/token-manager.js", () => ({
  getServiceToken: getServiceTokenMock,
}));

// The activities module's other imports, as index.test.ts stubs them, so it
// loads without the worker's configuration.
vi.mock("../grpc/client.js", () => ({ aegisRuntimeClient: {} }));
vi.mock("./workflow-activities.js", () => ({
  fetchWorkflowDefinition: vi.fn(),
}));

vi.mock("../logger.js", () => ({
  logger: { info: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn() },
}));

import {
  fireScheduleActivity,
  SCHEDULE_FIRE_REFUSED_ERROR,
  SCHEDULE_FIRE_TRANSPORT_ERROR,
} from "./schedule-activities.js";

interface SeenRequest {
  method: string | undefined;
  url: string | undefined;
  authorization: string | undefined;
  tenant: string | undefined;
  contentType: string | undefined;
  body: string;
}

interface StandIn {
  url: string;
  seen: SeenRequest[];
  close: () => Promise<void>;
}

/**
 * A stand-in orchestrator. `answer` is called per request: a number answers
 * with that status and a fire row; "drop" destroys the socket with no answer,
 * which `fetch` reports as a transport failure.
 */
async function startStandIn(
  answer: (n: number) => number | "drop",
): Promise<StandIn> {
  const seen: SeenRequest[] = [];
  const server: Server = createServer((req: IncomingMessage, res) => {
    let body = "";
    req.on("data", (c: Buffer) => (body += c.toString()));
    req.on("end", () => {
      seen.push({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization,
        tenant: req.headers["x-tenant-id"] as string | undefined,
        contentType: req.headers["content-type"],
        body,
      });
      const a = answer(seen.length);
      if (a === "drop") {
        req.socket.destroy();
        return;
      }
      res.statusCode = a;
      res.setHeader("Content-Type", "application/json");
      res.end(
        JSON.stringify(
          a < 300
            ? { outcome: "started", execution_id: "exec-1" }
            : { error: "schedule_not_found" },
        ),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    seen,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

const SCHEDULE_ID = "6f1c2b9e-0a4d-4b8e-9a51-2f3c4d5e6f70";
const TENANT_ID = "tenant-a";
const SCHEDULED_TIME = "2026-10-08T15:00:00.000Z";

describe("fireScheduleActivity", () => {
  let standIn: StandIn | undefined;
  const savedUrl = process.env.AEGIS_ORCHESTRATOR_URL;

  beforeEach(() => {
    getServiceTokenMock.mockReset();
    getServiceTokenMock.mockResolvedValue("service-token-1");
  });

  afterEach(async () => {
    await standIn?.close();
    standIn = undefined;
    if (savedUrl === undefined) delete process.env.AEGIS_ORCHESTRATOR_URL;
    else process.env.AEGIS_ORCHESTRATOR_URL = savedUrl;
  });

  it("posts the scheduled time to the fire route with the service token and the tenant header", async () => {
    standIn = await startStandIn(() => 200);
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;

    const result = await fireScheduleActivity({
      schedule_id: SCHEDULE_ID,
      tenant_id: TENANT_ID,
      scheduled_time: SCHEDULED_TIME,
    });

    expect(standIn.seen, "the activity sends exactly one request").toHaveLength(
      1,
    );
    const [req] = standIn.seen;
    expect(req.method, "the fire is a POST").toBe("POST");
    expect(req.url, "the fire goes to the schedule's internal fire route").toBe(
      `/v1/internal/schedules/${SCHEDULE_ID}/fire`,
    );
    expect(
      req.authorization,
      "the fire carries the worker's service token",
    ).toBe("Bearer service-token-1");
    expect(
      req.tenant,
      "the fire names the schedule's tenant in X-Tenant-Id",
    ).toBe(TENANT_ID);
    expect(req.contentType).toBe("application/json");
    expect(JSON.parse(req.body), "the body is the scheduled time only").toEqual(
      {
        scheduled_time: SCHEDULED_TIME,
      },
    );
    expect(result).toEqual({
      status: 200,
      answer: { outcome: "started", execution_id: "exec-1" },
    });
  });

  it("encodes the schedule id into the path", async () => {
    standIn = await startStandIn(() => 200);
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
    await fireScheduleActivity({
      schedule_id: "a/b",
      tenant_id: TENANT_ID,
      scheduled_time: SCHEDULED_TIME,
    });
    expect(standIn.seen[0].url).toBe("/v1/internal/schedules/a%2Fb/fire");
  });

  it.each([400, 403, 404, 409, 422])(
    "fails without retry when the orchestrator answers %i",
    async (status) => {
      standIn = await startStandIn(() => status);
      process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;

      const error = await fireScheduleActivity({
        schedule_id: SCHEDULE_ID,
        tenant_id: TENANT_ID,
        scheduled_time: SCHEDULED_TIME,
      }).catch((e: unknown) => e);

      expect(error, "a refusal is an ApplicationFailure").toBeInstanceOf(
        ApplicationFailure,
      );
      const failure = error as ApplicationFailure;
      expect(failure.nonRetryable, `a ${status} must not be retried`).toBe(
        true,
      );
      expect(failure.type).toBe(SCHEDULE_FIRE_REFUSED_ERROR);
      expect(failure.details?.[0]).toEqual({ status });
      expect(standIn.seen).toHaveLength(1);
    },
  );

  it.each([500, 501, 505])(
    "fails without retry when the orchestrator answers %i",
    async (status) => {
      standIn = await startStandIn(() => status);
      process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
      const error = (await fireScheduleActivity({
        schedule_id: SCHEDULE_ID,
        tenant_id: TENANT_ID,
        scheduled_time: SCHEDULED_TIME,
      }).catch((e: unknown) => e)) as ApplicationFailure;
      expect(error.nonRetryable, `a ${status} must not be retried`).toBe(true);
      expect(error.type).toBe(SCHEDULE_FIRE_REFUSED_ERROR);
    },
  );

  it.each([502, 503, 504])(
    "fails retryably when the orchestrator answers %i (the pod or its proxy unavailable)",
    async (status) => {
      standIn = await startStandIn(() => status);
      process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
      const error = await fireScheduleActivity({
        schedule_id: SCHEDULE_ID,
        tenant_id: TENANT_ID,
        scheduled_time: SCHEDULED_TIME,
      }).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(ApplicationFailure);
      const failure = error as ApplicationFailure;
      expect(failure.nonRetryable, `a ${status} must be retried`).toBe(false);
      expect(failure.type).toBe(SCHEDULE_FIRE_TRANSPORT_ERROR);
      expect(failure.details?.[0]).toEqual({ status });
      expect(standIn.seen).toHaveLength(1);
    },
  );

  it("fails retryably when the connection drops with no answer", async () => {
    standIn = await startStandIn(() => "drop");
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;

    const error = await fireScheduleActivity({
      schedule_id: SCHEDULE_ID,
      tenant_id: TENANT_ID,
      scheduled_time: SCHEDULED_TIME,
    }).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(ApplicationFailure);
    const failure = error as ApplicationFailure;
    expect(failure.nonRetryable, "a transport failure must be retryable").toBe(
      false,
    );
    expect(failure.type).toBe(SCHEDULE_FIRE_TRANSPORT_ERROR);
  });

  it("fails retryably when nothing listens at the orchestrator's address", async () => {
    standIn = await startStandIn(() => 200);
    const url = standIn.url;
    await standIn.close();
    standIn = undefined;
    process.env.AEGIS_ORCHESTRATOR_URL = url;

    const failure = (await fireScheduleActivity({
      schedule_id: SCHEDULE_ID,
      tenant_id: TENANT_ID,
      scheduled_time: SCHEDULED_TIME,
    }).catch((e: unknown) => e)) as ApplicationFailure;
    expect(failure.nonRetryable, "a refused connection must be retryable").toBe(
      false,
    );
    expect(failure.type).toBe(SCHEDULE_FIRE_TRANSPORT_ERROR);
  });

  it("retries a token fetch that did not reach Keycloak, and not one Keycloak refused", async () => {
    standIn = await startStandIn(() => 200);
    process.env.AEGIS_ORCHESTRATOR_URL = standIn.url;
    const params = {
      schedule_id: SCHEDULE_ID,
      tenant_id: TENANT_ID,
      scheduled_time: SCHEDULED_TIME,
    };

    getServiceTokenMock.mockRejectedValueOnce(new TypeError("fetch failed"));
    const unreachable = (await fireScheduleActivity(params).catch(
      (e: unknown) => e,
    )) as ApplicationFailure;
    expect(unreachable.nonRetryable).toBe(false);

    getServiceTokenMock.mockRejectedValueOnce(
      new Error("Keycloak token fetch failed: HTTP 401"),
    );
    const refused = (await fireScheduleActivity(params).catch(
      (e: unknown) => e,
    )) as ApplicationFailure;
    expect(refused.nonRetryable).toBe(true);
    expect(standIn.seen, "no fire is posted without a token").toHaveLength(0);
  });
});

describe("the activities module", () => {
  it("exports fireScheduleActivity for the worker to register", async () => {
    const activities = await import("./index.js");
    expect(
      (activities as Record<string, unknown>).fireScheduleActivity,
      "src/activities/index.ts must export fireScheduleActivity",
    ).toBe(fireScheduleActivity);
  });
});
