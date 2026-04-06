import { logger } from "../logger.js";

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
  // TODO: wire gRPC call once proto is regenerated (ADR-103 InvokeOutputHandler RPC)
}
