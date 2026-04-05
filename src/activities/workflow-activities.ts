import { database } from "../database.js";
import { logger } from "../logger.js";
import type { TemporalWorkflowDefinition } from "../types.js";

/**
 * Fetch a workflow definition by UUID from the database.
 * This activity runs in the Node.js environment (outside the sandbox),
 * so it has access to the database.
 */
export async function fetchWorkflowDefinition(
  workflowId: string,
): Promise<TemporalWorkflowDefinition> {
  logger.info(
    { workflow_id: workflowId },
    "Fetching workflow definition from database",
  );

  try {
    const definition = await database.getWorkflowDefinition(workflowId);

    if (!definition) {
      throw new Error(`Workflow definition not found: ${workflowId}`);
    }

    return definition;
  } catch (error) {
    logger.error(
      { error, workflow_id: workflowId },
      "Failed to fetch workflow definition",
    );
    throw error;
  }
}
