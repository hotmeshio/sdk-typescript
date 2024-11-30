import { HotMesh } from '../hotmesh';
import {
  HMSH_FIDELITY_SECONDS,
  HMSH_LOGLEVEL,
  HMSH_QUORUM_DELAY_MS,
} from '../../modules/enums';
import {
  hashOptions,
  isValidCron,
  polyfill,
  s,
  sleepFor,
} from '../../modules/utils';
import {
  MeshCallConnectParams,
  MeshCallCronParams,
  MeshCallExecParams,
  MeshCallFlushParams,
  MeshCallInstanceOptions,
  MeshCallInterruptParams,
} from '../../types/meshcall';
import { StreamData } from '../../types/stream';
import { ProviderConfig, ProvidersConfig } from '../../types/provider';
import { HMNS, KeyType } from '../../modules/key';
import { CronHandler } from '../pipe/functions/cron';

import { getWorkflowYAML, VERSION } from './schemas/factory';

/**
 * MeshCall connects any function as an idempotent endpoint.
 * Call functions from anywhere on the network connected to the
 * target backend (Redis, Postgres, NATS, etc). Function
 * responses are cacheable and invocations can be scheduled to
 * run as idempotent cron jobs (this one runs nightly at midnight
 * and uses Redis as the backend provider).
 *
 * @example
 * ```typescript
 * import { Client as Postgres } from 'pg';
 * import { MeshCall } from '@hotmesh/meshcall';
 *
 * MeshCall.cron({
 *   topic: 'my.cron.function',
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   callback: async () => {
 *     //your code here...anything goes
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
    config: ProviderConfig | ProvidersConfig,
    options: MeshCallInstanceOptions = {},
  ): Promise<HotMesh | void> {
    for (const [id, hotMeshInstance] of targets) {
      const hotMesh = await hotMeshInstance;
      const appId = hotMesh.engine.appId;
      if (appId === namespace) {
        if (id.startsWith(MeshCall.hashOptions(config))) {
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
    connection: ProviderConfig | ProvidersConfig,
    options: MeshCallInstanceOptions = {},
  ): Promise<HotMesh> => {
    //namespace isolation requires the connection options to be hashed
    //as multiple intersecting databases can be used by the same service
    const optionsHash = MeshCall.hashOptions(connection);
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
        connection,
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
    providerConfig: ProviderConfig | ProvidersConfig,
    options: MeshCallInstanceOptions = {},
  ): Promise<HotMesh> {
    let hotMeshInstance: HotMesh | void;

    if (!options.readonly) {
      hotMeshInstance = await MeshCall.findFirstMatching(
        MeshCall.workers,
        namespace,
        providerConfig,
        options,
      );
    }
    if (!hotMeshInstance) {
      hotMeshInstance = await MeshCall.findFirstMatching(
        MeshCall.engines,
        namespace,
        providerConfig,
        options,
      );
      if (!hotMeshInstance) {
        hotMeshInstance = (await MeshCall.getHotMeshClient(
          namespace,
          providerConfig,
          options,
        )) as unknown as HotMesh;
      }
    }
    return hotMeshInstance as HotMesh;
  }

  /**
   * connection re-use is important when making repeated calls, but
   * only if the connection options are an exact match. this method
   * hashes the connection options to ensure that the same connection
   */
  static hashOptions(connection: ProviderConfig | ProvidersConfig): string {
    if ('options' in connection) {
      //shorthand format
      return hashOptions(connection.options);
    } else {
      //longhand format (sub, store, stream, pub, search)
      const response = [];
      for (const p in connection) {
        if (connection[p].options) {
          response.push(hashOptions(connection[p].options));
        }
      }
      return response.join('');
    }
  }

  /**
   * Connects and links a worker function to the mesh
   * @example
   * ```typescript
   * import { Client as Postgres } from 'pg';
   * import { MeshCall } from '@hotmesh/meshcall';
   *
   * MeshCall.connect({
   *   topic: 'my.function',
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   callback: async (arg1: any) => {
   *     //your code here...
   *   }
   * });
   * ```
   */
  static async connect(params: MeshCallConnectParams): Promise<HotMesh> {
    const targetNamespace = params.namespace ?? HMNS;
    const optionsHash = MeshCall.hashOptions(polyfill.providerConfig(params));

    const targetTopic = `${optionsHash}.${targetNamespace}.${params.topic}`;
    const connection = polyfill.providerConfig(params);

    const hotMeshWorker = await HotMesh.init({
      guid: params.guid,
      logLevel: params.logLevel ?? HMSH_LOGLEVEL,
      appId: params.namespace ?? HMNS,
      engine: { connection },
      workers: [
        {
          topic: params.topic,
          connection,
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
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   }
   * });
   * ```
   */
  static async exec<U>(params: MeshCallExecParams): Promise<U> {
    const TOPIC = `${params.namespace ?? HMNS}.call`;
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      polyfill.providerConfig(params),
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
   * import { Client as Postgres } from 'pg';
   * import { MeshCall } from '@hotmesh/meshcall';
   *
   * MeshCall.flush({
   *   topic: 'my.function',
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   options: { id: 'myCachedExecFunctionId' }
   * });
   * ```
   */
  static async flush(params: MeshCallFlushParams): Promise<void> {
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      polyfill.providerConfig(params),
    );
    await hotMeshInstance.scrub(params.id ?? params?.options?.id);
  }

  /**
   * Creates a stream where messages can be published to ensure there is a
   * channel in place when the message arrives (a race condition for those
   * platforms without implicit topic setup).
   * @private
   */
  static createStream = async (
    hotMeshClient: HotMesh,
    workflowTopic: string,
    namespace?: string,
  ) => {
    const params = { appId: namespace ?? HMNS, topic: workflowTopic };
    const streamKey = hotMeshClient.engine.store.mintKey(
      KeyType.STREAMS,
      params,
    );
    try {
      await hotMeshClient.engine.stream.createConsumerGroup(
        streamKey,
        'WORKER',
      );
    } catch (err) {
      //ignore if already exists
    }
  };

  /**
   * Schedules a cron job to run at a specified interval
   * with optional args. Provided arguments are passed to the
   * callback function each time the cron job runs. The `id`
   * option is used to uniquely identify the cron job, allowing
   * it to be interrupted at any time.
   *
   * @example
   * ```typescript
   * import { Client as Postgres } from 'pg';
   * import { MeshCall } from '@hotmesh/meshcall';
   *
   * MeshCall.cron({
   *   topic: 'my.cron.function',
   *   args: ['arg1', 'arg2'], //optionally pass args
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   callback: async (arg1: any, arg2: any) => {
   *     //your code here...
   *   },
   *   options: { id: 'myDailyCron123', interval: '0 0 * * *' }
   * });
   * ```
   */
  static async cron(params: MeshCallCronParams): Promise<boolean> {
    let hotMeshInstance: HotMesh | void;
    let readonly = true;
    if (params.callback) {
      //always connect cron worker if provided
      hotMeshInstance = await MeshCall.connect({
        logLevel: params.logLevel,
        guid: params.guid,
        topic: params.topic,
        connection: polyfill.providerConfig(params),
        callback: params.callback,
        namespace: params.namespace,
      });
      readonly = false;
    } else {
      //this is a readonly cron connection which means
      //it is only being created to connect as a readonly member
      //of the mesh network that the cron is running on. it
      //can start a job, but it cannot run the job itself in RO mode.
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

    try {
      if (!hotMeshInstance) {
        //get or create a read-only engine instance to start the cron
        hotMeshInstance = await MeshCall.getInstance(
          params.namespace,
          polyfill.providerConfig(params),
          { readonly, guid: params.guid },
        );
        await MeshCall.createStream(
          hotMeshInstance,
          params.topic,
          params.namespace,
        );
      }

      //spawn the job (ok if it's a duplicate)
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
   * import { Client as Postgres } from 'pg';
   * import { MeshCall } from '@hotmesh/meshcall';
   *
   * MeshCall.interrupt({
   *   topic: 'my.cron.function',
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   options: { id: 'myDailyCron123' }
   * });
   * ```
   */
  static async interrupt(params: MeshCallInterruptParams): Promise<boolean> {
    const hotMeshInstance = await MeshCall.getInstance(
      params.namespace,
      polyfill.providerConfig(params),
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
