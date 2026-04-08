import { logger } from "../logger.js";
import { aegisRuntimeClient } from "../grpc/client.js";

export interface OutputHandlerActivityInput {
  executionId: string;
  tenantId: string;
  finalOutput: string;
  handlerConfigJson: string;
}

/**
 * Invokes the output handler for an agent execution or workflow state.
 * Delegates to the orchestrator gRPC InvokeOutputHandler RPC (ADR-103).
 */
export async function executeOutputHandlerActivity(
  input: OutputHandlerActivityInput,
): Promise<void> {
  const config = JSON.parse(input.handlerConfigJson) as { type: string };
  logger.info(
    {
      execution_id: input.executionId,
      tenant_id: input.tenantId,
      handler_type: config.type,
    },
    "[OutputHandler] Invoking handler",
  );

  const response = await aegisRuntimeClient.invokeOutputHandler({
    execution_id: input.executionId,
    tenant_id: input.tenantId,
    final_output: input.finalOutput,
    handler_config_json: input.handlerConfigJson,
  });

  if (!response.success) {
    throw new Error(
      `Output handler failed for execution ${input.executionId}: ${response.error}`,
    );
  }

  logger.info(
    {
      execution_id: input.executionId,
      handler_type: config.type,
      result: response.result,
    },
    "[OutputHandler] Handler completed successfully",
  );
}
