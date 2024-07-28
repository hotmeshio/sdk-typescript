import ms from 'ms';

import { HotMesh } from '../hotmesh';
import { HMSH_LOGLEVEL, HMSH_QUORUM_DELAY_MS } from '../../modules/enums';
import { hashOptions, sleepFor } from '../../modules/utils';
import {
  MeshCallConnectParams,
  MeshCallCronParams,
  MeshCallExecParams,
  MeshCallFlushParams,
  MeshCallInterruptParams,
} from '../../types/meshcall';
import { RedisConfig, StreamData } from '../../types';
import { HMNS } from '../../modules/key';

import { getWorkflowYAML } from './schemas/factory';

/**
 * MeshCall connects your functions to the Redis-backed mesh,
 * exposing them as idempotent endpoints. Call functions
 * from anywhere on the network with a connection to Redis. Function
 * responses are cacheable and functions can even
 * run as idempotent cron jobs (this one runs once a day).
 *
 * @example
 * ```typescript
 * MeshCall.cron({
 *   topic: 'my.cron.function',
 *   redis: {
 *     class: Redis,
 *     options: { url: 'redis://:key_admin@redis:6379' }
 *   },
 *   callback: async () => {
 *     //your code here...
 *   },
 *   options: { id: 'myDailyCron123', interval: '1 day' }
 * });
 * ```
 */
class MeshCall {
  /**
   * @private
   */
  static workers = new Map<string, HotMesh | Promise<HotMesh>>();

  /**
   * @private
   */
  static engines = new Map<string, HotMesh | Promise<HotMesh>>();

  /**
   * @private
   */
  static connections = new Map<string, any>();

  /**
   * @private
   */
  constructor() {}

  /**
   * iterates cached worker/engine instances to locate the first match
   * with the provided namespace and connection options
   * @private
   */
  static async findFirstMatching(
    workers: Map<string, HotMesh | Promise<HotMesh>>,
    namespace = HMNS,
    config: RedisConfig,
  ): Promise<HotMesh | void> {
    for (const [id, hotMeshInstance] of workers) {
      if ((await hotMeshInstance).namespace === namespace) {
        if (id.startsWith(hashOptions(config.options))) {
          return hotMeshInstance;
        }
      }
    }
  }

  /**
   * @private
   */
  static getHotMeshClient = async (
    namespace: string,
    connection: RedisConfig,
  ): Promise<HotMesh> => {
    //namespace isolation requires the connection options to be hashed
    //as multiple intersecting databases can be used by the same service
    const optionsHash = hashOptions(connection.options);
    const targetNS = namespace ?? HMNS;
    const connectionNS = `${optionsHash}.${targetNS}`;
    if (MeshCall.engines.has(connectionNS)) {
      const hotMeshClient = await MeshCall.engines.get(connectionNS);
      await this.verifyWorkflowActive(hotMeshClient, targetNS);
      return hotMeshClient;
    }

    //create and cache an instance
    const hotMeshClient = HotMesh.init({
      appId: targetNS,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: {
          class: connection.class,
          options: connection.options,
        },
      },
    });
    MeshCall.engines.set(connectionNS, hotMeshClient);
    await this.activateWorkflow(await hotMeshClient, targetNS);
    return hotMeshClient;
  };

  /**
   * @private
   */
  static async verifyWorkflowActive(
    hotMesh: HotMesh,
    appId = HMNS,
    count = 0,
  ): Promise<boolean> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if (isNaN(appVersion)) {
      if (count > 10) {
        throw new Error('Workflow failed to activate');
      }
      await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
      return await MeshCall.verifyWorkflowActive(hotMesh, appId, count + 1);
    }
    return true;
  }

  /**
   * @private
   */
  static async activateWorkflow(
    hotMesh: HotMesh,
    appId = HMNS,
    version = '1',
  ): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as string;
    if (appVersion === version && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-activate-err', { error });
        throw error;
      }
    } else if (isNaN(Number(appVersion)) || appVersion < version) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', {
          ...error,
        });
        throw error;
      }
    }
  }

  /**
   * Returns a cached worker instance or creates a new one
   * @private
   */
  static async getInstance(
    namespace: string,
    redis: RedisConfig,
  ): Promise<HotMesh> {
    let hotMeshInstance = await MeshCall.findFirstMatching(
      MeshCall.workers,
      namespace,
      redis,
    );
    if (!hotMeshInstance) {
      hotMeshInstance = await MeshCall.findFirstMatching(
        MeshCall.engines,
        namespace,
        redis,
      );
      if (!hotMeshInstance) {
        hotMeshInstance = (await MeshCall.getHotMeshClient(
          namespace,
          redis,
        )) as unknown as HotMesh;
      }
    }
    return hotMeshInstance as HotMesh;
  }

  /**
   * Connects and links a worker function to the mesh
   * @example
   * ```typescript
   * MeshCall.connect({
   *   topic: 'my.function',
   *   redis: {
   *     class: Redis,
   *     options: { url: 'redis://:key_admin@redis:6379' }
   *   },
   *   callback: async (arg1: any) => {
   *     //your code here...
   *   }
   * });
   * ```
   */
  static async connect(params: MeshCallConnectParams): Promise<HotMesh> {
    const targetNamespace = params.namespace ?? HMNS;
    const optionsHash = hashOptions(params.redis?.options);
    const targetTopic = `${optionsHash}.${targetNamespace}.${params.topic}`;
    const hotMeshWorker = await HotMesh.init({
      guid: params.guid,
      logLevel: params.logLevel ?? HMSH_LOGLEVEL,
      appId: params.namespace ?? HMNS,
      engine: { redis: params.redis },
      workers: [
        {
          topic: params.topic,
          redis: params.redis,
          callback: async function (input: StreamData) {
            const response = await params.callback.apply(this, input.data.args);
            return {
              metadata: { ...input.metadata },
              data: { response },
            };
          },
        },
      ],
    });
    MeshCall.workers.set(targetTopic, hotMeshWorker);
    await MeshCall.activateWorkflow(hotMeshWorker, targetNamespace);
    return hotMeshWorker;
  }

  /**
   * Calls a function and returns the response.
   *
   * @template U - the return type of the linked worker function
   *
   * @example
   * ```typescript
   * const response = await MeshCall.exec({
   *   topic: 'my.function',
   *   args: [{ my: 'args' }],
   *   redis: {
   *     class: Redis,
   *     options: { url: 'redis://:key_admin@redis:6379' }
   *   }
   * });
   * ```
   */
  static async exec<U>(params: MeshCallExecParams): Promise<U> {
    const TOPIC = `${params.namespace ?? HMNS}.call`;
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      params.redis,
    );

    let id = params.options?.id;
    if (id) {
      if (params.options?.flush) {
        await hotMeshInstance.scrub(id);
      } else if (params.options?.ttl) {
        //check cache
        try {
          const cached = await hotMeshInstance.getState(TOPIC, id);
          if (cached) {
            //todo: check if present; await if not (subscribe)
            return cached.data.response as U;
          }
        } catch (error) {
          //just swallow error; it means the cache is empty (no doc by that id)
        }
      }
    } else {
      id = HotMesh.guid();
    }

    let expire = 1;
    if (params.options?.ttl) {
      expire = ms(params.options.ttl) / 1000;
    }
    const jobOutput = await hotMeshInstance.pubsub(
      TOPIC,
      { id, expire, topic: params.topic, args: params.args },
      null,
      30_000, //local timeout
    );
    return jobOutput?.data?.response as U;
  }

  /**
   * Clears a cached function response.
   *
   * @example
   * ```typescript
   * MeshCall.flush({
   *   topic: 'my.function',
   *   redis: {
   *     class: Redis,
   *     options: { url: 'redis://:key_admin@redis:6379' }
   *   },
   *   options: { id: 'myCachedExecFunctionId' }
   * });
   * ```
   */
  static async flush(params: MeshCallFlushParams): Promise<void> {
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      params.redis,
    );
    await hotMeshInstance.scrub(params.id ?? params?.options?.id);
  }

  /**
   * Schedules a cron job to run at a specified interval
   * with optional args. Provided arguments are passed to the
   * callback function each time the cron job runs. The `id`
   * option is used to uniquely identify the cron job, allowing
   * it to be interrupted at any time.
   *
   * @example
   * ```typescript
   * MeshCall.cron({
   *   topic: 'my.cron.function',
   *   args: ['arg1', 'arg2'], //optionally pass args
   *   redis: {
   *     class: Redis,
   *     options: { url: 'redis://:key_admin@redis:6379' }
   *   },
   *   callback: async (arg1: any, arg2: any) => {
   *     //your code here...
   *   },
   *   options: { id: 'myDailyCron123', interval: '1 day' }
   * });
   * ```
   */
  static async cron(params: MeshCallCronParams): Promise<boolean> {
    try {
      //connect the cron worker
      await MeshCall.connect({
        logLevel: params.logLevel,
        guid: params.guid,
        topic: params.topic,
        redis: params.redis,
        callback: params.callback,
        namespace: params.namespace,
      });

      //start the cron job
      const TOPIC = `${params.namespace ?? HMNS}.cron`;
      const interval = ms(params.options.interval) / 1000;
      const delay = params.options.delay
        ? ms(params.options.delay) / 1000
        : undefined;
      const maxCycles = params.options.maxCycles ?? 1_000_000;
      const hotMeshInstance = await MeshCall.getInstance(
        params.namespace,
        params.redis,
      );
      await hotMeshInstance.pub(TOPIC, {
        id: params.options.id,
        topic: params.topic,
        args: params.args,
        interval,
        maxCycles,
        delay,
      });
      return true;
    } catch (error) {
      if (error.message.includes(`Duplicate job: ${params.options.id}`)) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Interrupts a running cron job.
   *
   * @example
   * ```typescript
   * MeshCall.interrupt({
   *   topic: 'my.cron.function',
   *   redis: {
   *     class: Redis,
   *     options: { url: 'redis://:key_admin@redis:6379' }
   *   },
   *   options: { id: 'myDailyCron123' }
   * });
   * ```
   */
  static async interrupt(params: MeshCallInterruptParams): Promise<void> {
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      params.redis,
    );
    await hotMeshInstance.interrupt(
      `${params.namespace ?? HMNS}.cron`,
      params.options.id,
      { throw: false, expire: 60 },
    );
  }

  /**
   * Shuts down all meshcall instances. Call this method
   * from the SIGTERM handler in your application.
   */
  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of MeshCall.workers) {
      (await hotMeshInstance).stop();
    }
    for (const [_, hotMeshInstance] of MeshCall.engines) {
      (await hotMeshInstance).stop();
    }
    await HotMesh.stop();
  }
}

export { MeshCall };
