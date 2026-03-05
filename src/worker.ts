/**
 * Temporal Worker Initialization
 * Connects to Temporal Server and starts processing workflow/activity tasks
 */

import { NativeConnection, Worker } from '@temporalio/worker';
import { config } from './config.js';
import { logger } from './logger.js';
import * as activities from './activities/index.js';

export async function startWorker(): Promise<void> {
  logger.info('Initializing Temporal worker...');

  try {
    // Connect to Temporal Server
    const connection = await NativeConnection.connect({
      address: config.temporal.address,
    });

    logger.info({ address: config.temporal.address }, 'Connected to Temporal Server');

    // Create worker pointing to workflows directory
    // Using fileURLToPath for ESM compatibility
    const worker = await Worker.create({
      connection,
      namespace: config.temporal.namespace,
      taskQueue: config.temporal.taskQueue,
      workflowsPath: new URL('./workflows/index.js', import.meta.url).pathname,
      activities,
      maxConcurrentActivityTaskExecutions: config.worker.maxConcurrentActivityTaskExecutions,
      maxConcurrentWorkflowTaskExecutions: config.worker.maxConcurrentWorkflowTaskExecutions,
      bundlerOptions: {
        // Ignore non-deterministic modules that are imported by dependencies but not used at runtime
        ignoreModules: ['fs', 'path', 'os', 'crypto'],
      },
    });

    logger.info(
      {
        namespace: config.temporal.namespace,
        taskQueue: config.temporal.taskQueue,
      },
      'Temporal worker created successfully'
    );

    // Start worker
    await worker.run();

    logger.info('Temporal worker is running');
  } catch (error) {
    logger.error({ 
      error,
      message: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined
    }, 'Failed to start Temporal worker');
    throw error;
  }
}


