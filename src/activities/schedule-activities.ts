/**
 * Schedule activities
 *
 * A Temporal Schedule starts `aegis_schedule_fire` for each of a person's
 * schedules (AEGIS ADR-139 N5). Its one activity tells the orchestrator that
 * the schedule fired; the orchestrator, not the worker, starts the person's
 * run (N7), so nothing of the run's input, contexts or credentials passes
 * through Temporal.
 *
 * The retry contract (N6, N6a): a transport failure (the orchestrator or
 * Keycloak unreachable, the connection reset) and an answered 502, 503 or 504
 * (the orchestrator's pod or its proxy unavailable, as during a `core`
 * redeploy) are retryable, and the workflow's retry policy bounds them to
 * three attempts inside the catch-up window. Every other answer that is not
 * 2xx is final: a 4xx is the orchestrator refusing this fire.
 */

import { ApplicationFailure } from "@temporalio/activity";
import { getServiceToken } from "../auth/token-manager.js";
import { logger } from "../logger.js";

/** The error type of a fire that may be tried again. */
export const SCHEDULE_FIRE_TRANSPORT_ERROR = "ScheduleFireTransportError";
/** The answers that mean the orchestrator is unavailable, not refusing (N6a). */
const UNAVAILABLE_STATUSES = new Set([502, 503, 504]);

/** The error type of a fire the orchestrator answered and refused. */
export const SCHEDULE_FIRE_REFUSED_ERROR = "ScheduleFireRefused";

export interface FireScheduleParams {
  schedule_id: string;
  tenant_id: string;
  /** Temporal's scheduled time for this action, RFC 3339. */
  scheduled_time: string;
}

export interface FireScheduleResult {
  /** The orchestrator's HTTP status. */
  status: number;
  /** The orchestrator's answer (the fire's row), or null when it sent none. */
  answer: unknown;
}

function scheduleFireUrl(scheduleId: string): string {
  const orchestratorUrl =
    process.env.AEGIS_ORCHESTRATOR_URL || "http://localhost:8088";
  return `${orchestratorUrl}/v1/internal/schedules/${encodeURIComponent(scheduleId)}/fire`;
}

/**
 * `fetch` rejects with a TypeError when no HTTP answer arrived (refused,
 * reset, DNS); an HTTP answer of any status resolves. That is the line
 * between a transport failure and an answer.
 */
function isTransportFailure(error: unknown): boolean {
  return error instanceof TypeError;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    const cause = (error as { cause?: unknown }).cause;
    return cause instanceof Error
      ? `${error.message}: ${cause.message}`
      : error.message;
  }
  return String(error);
}

/**
 * Post `{scheduled_time}` to `POST /v1/internal/schedules/{id}/fire` with the
 * worker's service token and the schedule's tenant in `X-Tenant-Id`, the form
 * `fetchPersistedExecutionStatus` in `grpc/client.ts` uses.
 */
export async function fireScheduleActivity(
  params: FireScheduleParams,
): Promise<FireScheduleResult> {
  const url = scheduleFireUrl(params.schedule_id);

  let token: string;
  try {
    token = await getServiceToken();
  } catch (error) {
    if (isTransportFailure(error)) {
      throw ApplicationFailure.retryable(
        `The service token for schedule ${params.schedule_id} could not be fetched: ${errorMessage(error)}`,
        SCHEDULE_FIRE_TRANSPORT_ERROR,
      );
    }
    throw ApplicationFailure.nonRetryable(
      `The service token for schedule ${params.schedule_id} was refused: ${errorMessage(error)}`,
      SCHEDULE_FIRE_REFUSED_ERROR,
    );
  }

  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        "X-Tenant-Id": params.tenant_id,
      },
      body: JSON.stringify({ scheduled_time: params.scheduled_time }),
    });
  } catch (error) {
    logger.warn(
      { schedule_id: params.schedule_id, error: errorMessage(error) },
      "Schedule fire did not reach the orchestrator",
    );
    throw ApplicationFailure.retryable(
      `The fire of schedule ${params.schedule_id} did not reach the orchestrator: ${errorMessage(error)}`,
      SCHEDULE_FIRE_TRANSPORT_ERROR,
    );
  }

  const text = await response.text();
  if (UNAVAILABLE_STATUSES.has(response.status)) {
    logger.warn(
      { schedule_id: params.schedule_id, status: response.status },
      "The orchestrator was unavailable for a schedule fire",
    );
    throw ApplicationFailure.retryable(
      `The orchestrator was unavailable for the fire of schedule ${params.schedule_id}: HTTP ${response.status}`,
      SCHEDULE_FIRE_TRANSPORT_ERROR,
      { status: response.status },
    );
  }
  if (!response.ok) {
    logger.warn(
      { schedule_id: params.schedule_id, status: response.status },
      "The orchestrator refused a schedule fire",
    );
    throw ApplicationFailure.nonRetryable(
      `The orchestrator answered the fire of schedule ${params.schedule_id} with HTTP ${response.status}: ${text.slice(0, 500)}`,
      SCHEDULE_FIRE_REFUSED_ERROR,
      { status: response.status },
    );
  }

  let answer: unknown = null;
  if (text.length > 0) {
    try {
      answer = JSON.parse(text);
    } catch {
      answer = text;
    }
  }
  logger.info(
    { schedule_id: params.schedule_id, status: response.status },
    "Schedule fired",
  );
  return { status: response.status, answer };
}
