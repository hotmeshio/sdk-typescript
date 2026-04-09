import { ConnectorService } from '../connector/factory';
import { EngineService } from '../engine';
import { ILogger } from '../logger';
import { QuorumService } from '../quorum';
import { WorkerService } from '../worker';
import { HotMeshConfig } from '../../types/hotmesh';
import { ProviderConfig, ProvidersConfig } from '../../types/provider';
import { RetryPolicy } from '../../types/stream';
import { DEFAULT_TASK_QUEUE } from '../../modules/enums';

interface InitContext {
  namespace: string;
  appId: string;
  guid: string;
  engine: EngineService | null;
  quorum: QuorumService | null;
  workers: WorkerService[];
}

/**
 * @private
 */
export async function initEngine(
  instance: InitContext,
  config: HotMeshConfig,
  logger: ILogger,
): Promise<void> {
  if (config.engine) {
    //connections that are 'readonly' transfer
    //this property directly to the engine,
    //and ALWAYS take precendence.
    if (config.engine.connection.readonly) {
      config.engine.readonly = true;
    }

    // Apply retry policy to stream connection if provided
    if (config.engine.retryPolicy) {
      applyRetryPolicy(config.engine.connection, config.engine.retryPolicy);
    }

    // Initialize task queue for engine
    config.engine.taskQueue = initTaskQueue(
      config.engine.taskQueue,
      config.taskQueue,
    );

    await ConnectorService.initClients(config.engine);
    instance.engine = await EngineService.init(
      instance.namespace,
      instance.appId,
      instance.guid,
      config,
      logger,
    );
  }
}

/**
 * @private
 */
export async function initQuorum(
  instance: InitContext,
  config: HotMeshConfig,
  engine: EngineService,
  logger: ILogger,
): Promise<void> {
  if (engine) {
    instance.quorum = await QuorumService.init(
      instance.namespace,
      instance.appId,
      instance.guid,
      config,
      engine,
      logger,
    );
  }
}

/**
 * @private
 */
export async function doWork(
  instance: InitContext,
  config: HotMeshConfig,
  logger: ILogger,
) {
  // Initialize task queues for workers
  if (config.workers) {
    for (const worker of config.workers) {
      // Apply retry policy to stream connection if provided
      if (worker.retryPolicy) {
        applyRetryPolicy(worker.connection, worker.retryPolicy);
      }

      worker.taskQueue = initTaskQueue(
        worker.taskQueue,
        config.taskQueue,
      );
    }
  }

  instance.workers = await WorkerService.init(
    instance.namespace,
    instance.appId,
    instance.guid,
    config,
    logger,
  );
}

/**
 * Initialize task queue with proper precedence:
 * 1. Use component-specific queue if set (engine/worker)
 * 2. Use global config queue if set
 * 3. Use default queue as fallback
 * @private
 */
export function initTaskQueue(
  componentQueue?: string,
  globalQueue?: string,
): string {
  // Component-specific queue takes precedence
  if (componentQueue) {
    return componentQueue;
  }

  // Global config queue is next
  if (globalQueue) {
    return globalQueue;
  }

  // Default queue as fallback
  return DEFAULT_TASK_QUEUE;
}

/**
 * Apply retry policy to the stream connection within a ProviderConfig or ProvidersConfig.
 * Handles both short-form (ProviderConfig) and long-form (ProvidersConfig) connection configs.
 * @private
 */
export function applyRetryPolicy(
  connection: ProviderConfig | ProvidersConfig,
  retryPolicy: RetryPolicy,
): void {
  // Check if this is ProvidersConfig (has 'stream' property)
  if ('stream' in connection && connection.stream) {
    // Long-form: apply to the stream sub-config
    connection.stream.retryPolicy = retryPolicy;
  } else {
    // Short-form: apply directly to the connection
    (connection as ProviderConfig).retryPolicy = retryPolicy;
  }
}
