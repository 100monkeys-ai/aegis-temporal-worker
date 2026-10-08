/**
 * aegis_schedule_fire
 *
 * The workflow a person's Temporal Schedule starts (AEGIS ADR-139 N5, N6).
 * Its input is `{schedule_id, tenant_id}` only; the scheduled time is the one
 * Temporal records on every workflow a Schedule starts, the search attribute
 * `TemporalScheduledStartTime`. It runs one activity, `fireScheduleActivity`,
 * and ends after it whatever the activity's outcome: the AEGIS run the
 * orchestrator starts, not this workflow, is what a person sees.
 */

import {
  ApplicationFailure,
  proxyActivities,
  workflowInfo,
} from "@temporalio/workflow";
import type {
  FireScheduleResult,
  fireScheduleActivity as FireScheduleActivity,
} from "../activities/schedule-activities.js";

export interface ScheduleFireInput {
  schedule_id: string;
  tenant_id: string;
}

export interface ScheduleFireResult {
  schedule_id: string;
  tenant_id: string;
  /** Temporal's scheduled time, RFC 3339; null when Temporal recorded none. */
  scheduled_time: string | null;
  /**
   * `fired`: the orchestrator answered 2xx. `refused`: it answered with
   * anything else but 502, 503 or 504. `unreachable`: no attempt reached it,
   * or it answered unavailable (502, 503, 504), until the attempts or the
   * window ran out. Nothing more is tried for this time after any of
   * them. `unscheduled`: the workflow was not started by a Schedule, so there is
   * no scheduled time to fire for, and nothing was posted.
   */
  outcome: "fired" | "refused" | "unreachable" | "unscheduled";
  /** The orchestrator's HTTP status, when it answered. */
  status: number | null;
  /** The orchestrator's answer on `fired`; the failure's message otherwise. */
  answer: unknown;
}

/** The activity's error type for an answered refusal (schedule-activities.ts). */
const SCHEDULE_FIRE_REFUSED_ERROR = "ScheduleFireRefused";

/** The search attribute Temporal sets on a workflow a Schedule starts. */
export const SCHEDULED_START_TIME_ATTRIBUTE = "TemporalScheduledStartTime";

/** N5's catch-up window: a fire later than this is a different run. */
export const CATCHUP_WINDOW_MS = 600_000;

/** N6: at most three attempts, all inside the catch-up window. */
export const FIRE_MAXIMUM_ATTEMPTS = 3;

/** One attempt's bound; the orchestrator answers a fire without waiting on the run. */
const FIRE_ATTEMPT_TIMEOUT_MS = 30_000;

/**
 * The gaps between attempts: 30 s, then 60 s, so the three attempts span
 * about a minute and a half, the length of a `core` redeploy measured for
 * N5's window, and stay well inside the window.
 */
const FIRE_RETRY = {
  initialInterval: "30 seconds",
  backoffCoefficient: 2,
  maximumInterval: "60 seconds",
  maximumAttempts: FIRE_MAXIMUM_ATTEMPTS,
} as const;

function scheduledStartTime(): Date | null {
  const pair = workflowInfo()
    .typedSearchAttributes.getAll()
    .find((p) => p.key.name === SCHEDULED_START_TIME_ATTRIBUTE);
  const value = pair?.value;
  return value instanceof Date ? value : null;
}

interface FireFailure {
  outcome: "refused" | "unreachable";
  status: number | null;
  message: string;
}

/** Read the activity's own failure out of Temporal's ActivityFailure. */
function fireFailure(error: unknown): FireFailure {
  let cause: unknown = error;
  while (cause instanceof Error) {
    if (cause instanceof ApplicationFailure) {
      const details = cause.details?.[0] as { status?: number } | undefined;
      return {
        outcome:
          cause.type === SCHEDULE_FIRE_REFUSED_ERROR
            ? "refused"
            : "unreachable",
        status: typeof details?.status === "number" ? details.status : null,
        message: cause.message,
      };
    }
    cause = (cause as { cause?: unknown }).cause;
  }
  // A timeout or a cancellation: no answer came back in time.
  return {
    outcome: "unreachable",
    status: null,
    message: error instanceof Error ? error.message : String(error),
  };
}

export async function aegis_schedule_fire(
  input: ScheduleFireInput,
): Promise<ScheduleFireResult> {
  const scheduled = scheduledStartTime();
  if (scheduled === null) {
    return {
      schedule_id: input.schedule_id,
      tenant_id: input.tenant_id,
      scheduled_time: null,
      outcome: "unscheduled",
      status: null,
      answer: null,
    };
  }

  // Every attempt starts before the window closes. Date.now() is the
  // workflow's own clock here, so this is deterministic on replay.
  const windowLeftMs = scheduled.getTime() + CATCHUP_WINDOW_MS - Date.now();
  const { fireScheduleActivity } = proxyActivities<{
    fireScheduleActivity: typeof FireScheduleActivity;
  }>({
    startToCloseTimeout: FIRE_ATTEMPT_TIMEOUT_MS,
    scheduleToCloseTimeout: Math.max(windowLeftMs, FIRE_ATTEMPT_TIMEOUT_MS),
    retry: FIRE_RETRY,
  });

  const base = {
    schedule_id: input.schedule_id,
    tenant_id: input.tenant_id,
    scheduled_time: scheduled.toISOString(),
  };
  try {
    const result: FireScheduleResult = await fireScheduleActivity(base);
    return {
      ...base,
      outcome: "fired",
      status: result.status,
      answer: result.answer,
    };
  } catch (error) {
    const { outcome, status, message } = fireFailure(error);
    return { ...base, outcome, status, answer: message };
  }
}
