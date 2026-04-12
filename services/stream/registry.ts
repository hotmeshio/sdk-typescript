import { ILogger } from '../logger';
import { Router } from '../router';
import { StreamService } from './index';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import {
  StreamData,
  StreamDataResponse,
  StreamRole,
} from '../../types/stream';
import { KeyType } from '../../modules/key';

type WorkerCallback = (data: StreamData) => Promise<StreamDataResponse | void>;

interface WorkerConsumerEntry {
  router: Router<StreamService<ProviderClient, ProviderTransaction>>;
  callbacks: Map<string, WorkerCallback>;
  stream: StreamService<ProviderClient, ProviderTransaction>;
  logger: ILogger;
}

interface EngineConsumerEntry {
  router: Router<StreamService<ProviderClient, ProviderTransaction>>;
  callbacks: WorkerCallback[];
  stream: StreamService<ProviderClient, ProviderTransaction>;
  logger: ILogger;
}

/**
 * Process-wide singleton registry that manages one consumer per task queue (workers)
 * and one per appId (engines). Instead of N consumers each polling independently,
 * one consumer fetches batches from the stream and dispatches to registered callbacks
 * based on the `workflow_name` column (workers) or round-robin (engines).
 */
class StreamConsumerRegistry {
  private static workerConsumers: Map<string, WorkerConsumerEntry> = new Map();
  private static engineConsumers: Map<string, EngineConsumerEntry> = new Map();

  /**
   * Register a worker callback for a (taskQueue, workflowName) pair.
   * If no consumer exists for this taskQueue, a singleton Router is created.
   */
  static async registerWorker(
    namespace: string,
    appId: string,
    guid: string,
    taskQueue: string,
    workflowName: string,
    callback: WorkerCallback,
    stream: StreamService<ProviderClient, ProviderTransaction>,
    store: { mintKey: (type: KeyType, params: any) => string; getThrottleRate: (topic?: string) => Promise<number> },
    logger: ILogger,
    config?: {
      reclaimDelay?: number;
      reclaimCount?: number;
      retry?: any;
    },
  ): Promise<void> {
    const key = `${namespace}:${appId}:worker:${taskQueue}`;
    let entry = StreamConsumerRegistry.workerConsumers.get(key);

    if (!entry) {
      // Create the singleton consumer for this task queue
      const throttle = await store.getThrottleRate(taskQueue);
      const router = new Router(
        {
          namespace,
          appId,
          guid,
          role: StreamRole.WORKER,
          topic: taskQueue,
          reclaimDelay: config?.reclaimDelay,
          reclaimCount: config?.reclaimCount,
          throttle,
          retry: config?.retry,
        },
        stream,
        logger,
      );

      entry = {
        router,
        callbacks: new Map(),
        stream,
        logger,
      };
      StreamConsumerRegistry.workerConsumers.set(key, entry);

      // Create the dispatch callback that routes by workflow_name
      const dispatchCallback = StreamConsumerRegistry.createWorkerDispatcher(key);

      // Start consuming from the task queue stream
      const streamKey = stream.mintKey(KeyType.STREAMS, {
        appId,
        topic: taskQueue,
      });
      router.consumeMessages(streamKey, 'WORKER', guid, dispatchCallback);
    }

    // Register the callback for this workflow name
    entry.callbacks.set(workflowName, callback);
    logger.info('stream-consumer-registry-worker-registered', {
      taskQueue,
      workflowName,
      totalCallbacks: entry.callbacks.size,
    });
  }

  /**
   * Register an engine callback for an appId.
   * If no consumer exists for this appId, a singleton Router is created.
   */
  static async registerEngine(
    namespace: string,
    appId: string,
    guid: string,
    callback: WorkerCallback,
    stream: StreamService<ProviderClient, ProviderTransaction>,
    store: { mintKey: (type: KeyType, params: any) => string; getThrottleRate: (topic?: string) => Promise<number> },
    logger: ILogger,
    config?: {
      reclaimDelay?: number;
      reclaimCount?: number;
    },
  ): Promise<void> {
    const key = `${namespace}:${appId}:engine`;
    let entry = StreamConsumerRegistry.engineConsumers.get(key);

    if (!entry) {
      const throttle = await store.getThrottleRate();
      const router = new Router(
        {
          namespace,
          appId,
          guid,
          role: StreamRole.ENGINE,
          reclaimDelay: config?.reclaimDelay,
          reclaimCount: config?.reclaimCount,
          throttle,
        },
        stream,
        logger,
      );

      entry = {
        router,
        callbacks: [],
        stream,
        logger,
      };
      StreamConsumerRegistry.engineConsumers.set(key, entry);

      // Create the dispatch callback
      const dispatchCallback = StreamConsumerRegistry.createEngineDispatcher(key);

      // Start consuming from the engine stream
      const streamKey = stream.mintKey(KeyType.STREAMS, { appId });
      router.consumeMessages(streamKey, 'ENGINE', guid, dispatchCallback);
    }

    entry.callbacks.push(callback);
    logger.info('stream-consumer-registry-engine-registered', {
      appId,
      totalCallbacks: entry.callbacks.length,
    });
  }

  /**
   * Creates a dispatch callback for worker consumers.
   * Routes messages to the registered callback based on metadata.wfn (workflow_name).
   */
  private static createWorkerDispatcher(
    key: string,
  ): (data: StreamData) => Promise<StreamDataResponse | void> {
    return async (data: StreamData): Promise<StreamDataResponse | void> => {
      const entry = StreamConsumerRegistry.workerConsumers.get(key);
      if (!entry) return;

      const wfn = data.metadata?.wfn;
      if (!wfn) {
        entry.logger.warn('stream-consumer-registry-no-wfn', {
          key,
          metadata: data.metadata,
        });
        // Fall back to first registered callback if only one exists
        if (entry.callbacks.size === 1) {
          const [, callback] = entry.callbacks.entries().next().value;
          return callback(data);
        }
        return;
      }

      const callback = entry.callbacks.get(wfn);
      if (!callback) {
        entry.logger.info('stream-consumer-registry-replay', {
          key,
          wfn,
          registered: [...entry.callbacks.keys()],
          reason: 'worker-not-yet-registered',
          action: 'republish-with-delay',
          delayMs: 500,
        });
        // Worker not registered yet. Re-publish with short visibility delay
        // so it retries after the worker has time to register.
        // This avoids consuming the error handler's retry budget.
        const replayData = { ...data } as any;
        replayData._visibilityDelayMs = 500;
        const streamKey = entry.stream.mintKey(KeyType.STREAMS, {
          topic: data.metadata?.topic,
        });
        await entry.stream.publishMessages(
          streamKey,
          [JSON.stringify(replayData)],
        );
        // Return void — the original message will be ack'd,
        // but a new copy is queued with a delay.
        return;
      }

      return callback(data);
    };
  }

  /**
   * Creates a dispatch callback for engine consumers.
   * Engines are generic processors — the first registered callback handles the message.
   */
  private static createEngineDispatcher(
    key: string,
  ): (data: StreamData) => Promise<StreamDataResponse | void> {
    return async (data: StreamData): Promise<StreamDataResponse | void> => {
      const entry = StreamConsumerRegistry.engineConsumers.get(key);
      if (!entry || entry.callbacks.length === 0) return;

      // Engine callbacks are all equivalent — use the first one
      return entry.callbacks[0](data);
    };
  }

  /**
   * Unregister a worker callback.
   */
  static async unregisterWorker(
    namespace: string,
    appId: string,
    taskQueue: string,
    workflowName: string,
  ): Promise<void> {
    const key = `${namespace}:${appId}:worker:${taskQueue}`;
    const entry = StreamConsumerRegistry.workerConsumers.get(key);
    if (!entry) return;

    entry.callbacks.delete(workflowName);

    if (entry.callbacks.size === 0) {
      await entry.router.stopConsuming();
      StreamConsumerRegistry.workerConsumers.delete(key);
    }
  }

  /**
   * Unregister an engine callback.
   */
  static async unregisterEngine(
    namespace: string,
    appId: string,
    callback: WorkerCallback,
  ): Promise<void> {
    const key = `${namespace}:${appId}:engine`;
    const entry = StreamConsumerRegistry.engineConsumers.get(key);
    if (!entry) return;

    entry.callbacks = entry.callbacks.filter(cb => cb !== callback);

    if (entry.callbacks.length === 0) {
      await entry.router.stopConsuming();
      StreamConsumerRegistry.engineConsumers.delete(key);
    }
  }

  /**
   * Stop all consumers and clear the registry.
   */
  static async shutdown(): Promise<void> {
    for (const [, entry] of StreamConsumerRegistry.workerConsumers) {
      await entry.router.stopConsuming();
    }
    for (const [, entry] of StreamConsumerRegistry.engineConsumers) {
      await entry.router.stopConsuming();
    }
    StreamConsumerRegistry.workerConsumers.clear();
    StreamConsumerRegistry.engineConsumers.clear();
  }
}

export { StreamConsumerRegistry };
