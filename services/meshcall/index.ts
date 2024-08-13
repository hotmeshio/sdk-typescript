import { HotMesh } from '../hotmesh';
import {
  HMSH_FIDELITY_SECONDS,
  HMSH_LOGLEVEL,
  HMSH_QUORUM_DELAY_MS,
} from '../../modules/enums';
import { hashOptions, isValidCron, s, sleepFor } from '../../modules/utils';
import {
  MeshCallConnectParams,
  MeshCallCronParams,
  MeshCallExecParams,
  MeshCallFlushParams,
  MeshCallInstanceOptions,
  MeshCallInterruptParams,
} from '../../types/meshcall';
import { RedisConfig, StreamData } from '../../types';
import { HMNS } from '../../modules/key';
import { CronHandler } from '../pipe/functions/cron';

import { getWorkflowYAML, VERSION } from './schemas/factory';

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
 *   options: { id: 'myDailyCron123', interval: '0 0 * * *' }
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
    targets: Map<string, HotMesh | Promise<HotMesh>>,
    namespace = HMNS,
    config: RedisConfig,
    options: MeshCallInstanceOptions = {},
  ): Promise<HotMesh | void> {
    for (const [id, hotMeshInstance] of targets) {
      const hotMesh = await hotMeshInstance;
      const appId = hotMesh.engine.appId;
      if (appId === namespace) {
        if (id.startsWith(hashOptions(config.options))) {
          if (
            Boolean(options.readonly) == Boolean(hotMesh.engine.router.readonly)
          ) {
            return hotMeshInstance;
          }
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
    options: MeshCallInstanceOptions = {},
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
      guid: options.guid,
      appId: targetNS,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: {
          class: connection.class,
          options: connection.options,
        },
        readonly: options.readonly,
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
    version = VERSION,
  ): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as string;
    if (appVersion === version && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('meshcall-client-activate-err', { error });
        throw error;
      }
    } else if (isNaN(Number(appVersion)) || appVersion < version) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('meshcall-client-deploy-activate-err', {
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
    options: MeshCallInstanceOptions = {},
  ): Promise<HotMesh> {
    let hotMeshInstance: HotMesh | void;

    if (!options.readonly) {
      hotMeshInstance = await MeshCall.findFirstMatching(
        MeshCall.workers,
        namespace,
        redis,
        options,
      );
    }
    if (!hotMeshInstance) {
      hotMeshInstance = await MeshCall.findFirstMatching(
        MeshCall.engines,
        namespace,
        redis,
        options,
      );
      if (!hotMeshInstance) {
        hotMeshInstance = (await MeshCall.getHotMeshClient(
          namespace,
          redis,
          options,
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
      expire = s(params.options.ttl);
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
   *   options: { id: 'myDailyCron123', interval: '0 0 * * *' }
   * });
   * ```
   */
  static async cron(params: MeshCallCronParams): Promise<boolean> {
    if (params.callback) {
      //always connect cron worker if provided
      await MeshCall.connect({
        logLevel: params.logLevel,
        guid: params.guid,
        topic: params.topic,
        redis: params.redis,
        callback: params.callback,
        namespace: params.namespace,
      });
    }

    //configure job inputs
    const TOPIC = `${params.namespace ?? HMNS}.cron`;
    const maxCycles = params.options.maxCycles ?? 100_000;
    let interval = HMSH_FIDELITY_SECONDS;
    let delay: number | undefined;
    let cron: string;
    if (isValidCron(params.options.interval)) {
      //cron syntax
      cron = params.options.interval;
      const nextDelay = new CronHandler().nextDelay(cron);
      delay = nextDelay > 0 ? nextDelay : undefined;
    } else {
      const seconds = s(params.options.interval);
      interval = Math.max(seconds, HMSH_FIDELITY_SECONDS);
      delay = params.options.delay ? s(params.options.delay) : undefined;
    }

    //spawn the job (ok if it's a duplicate)
    try {
      const hotMeshInstance = await MeshCall.getInstance(
        params.namespace,
        params.redis,
        { readonly: params.callback ? false : true, guid: params.guid },
      );
      await hotMeshInstance.pub(TOPIC, {
        id: params.options.id,
        topic: params.topic,
        args: params.args,
        interval,
        cron,
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
   * Interrupts a running cron job. Returns `true` if the job
   * was successfully interrupted, or `false` if the job was not
   * found.
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
  static async interrupt(params: MeshCallInterruptParams): Promise<boolean> {
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      params.redis,
    );
    try {
      await hotMeshInstance.interrupt(
        `${params.namespace ?? HMNS}.cron`,
        params.options.id,
        { throw: false, expire: 1 },
      );
    } catch (error) {
      //job doesn't exist; is already stopped
      return false;
    }
    return true;
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
