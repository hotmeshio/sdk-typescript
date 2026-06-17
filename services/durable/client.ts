import {
  HMSH_LOGLEVEL,
  HMSH_EXPIRE_JOB_SECONDS,
  HMSH_QUORUM_DELAY_MS,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_DURABLE_MAX_INTERVAL,
} from '../../modules/enums';
import { hashOptions, s, sleepFor } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import {
  ClientConfig,
  ClientWorkflow,
  Connection,
  HookOptions,
  WorkflowOptions,
} from '../../types/durable';
import { JobState } from '../../types/job';
import { KeyType } from '../../modules/key';
import { StreamStatus, StringAnyType } from '../../types';

import { Search } from './search';
import { Entity } from './entity';
import { WorkflowHandleService } from './handle';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './schemas/factory';

/**
 * Workflow client. Starts workflows, sends signals, and reads results.
 *
 * @example
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const client = new Durable.Client({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 *   },
 * });
 *
 * // Start a workflow and await its result
 * const handle = await client.workflow.start({
 *   args: ['order-123'],
 *   taskQueue: 'orders',
 *   workflowName: 'orderWorkflow',
 *   workflowId: Durable.guid(),
 * });
 * const result = await handle.result();
 *
 * // Send a signal to a running workflow
 * await handle.signal('approval', { approved: true });
 *
 * // Cancel a running workflow
 * await handle.cancel();
 * ```
 */
export class ClientService {
  /**
   * @private
   */
  connection: Connection;
  /**
   * @private
   */
  options: WorkflowOptions;
  /**
   * @private
   */
  static topics: string[] = [];
  /**
   * @private
   */
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  /**
   * @private
   */
  constructor(config: ClientConfig) {
    this.connection = config.connection;
  }

  /**
   * @private
   */
  getHotMeshClient = async (taskQueue: string | null, namespace?: string) => {
    //namespace isolation requires the connection options to be hashed
    //as multiple intersecting databases can be used by the same service
    //hashing options allows for reuse of the same connection without risk of
    //overwriting data in another namespace.
    const optionsHash = this.hashOptions();
    const targetNS = namespace ?? APP_ID;
    const connectionNS = `${optionsHash}.${targetNS}`;
    if (ClientService.instances.has(connectionNS)) {
      const hotMeshClient = await ClientService.instances.get(connectionNS);
      await this.verifyWorkflowActive(hotMeshClient, targetNS);
      return hotMeshClient;
    }

    //init, but don't await
    const readonly = this.connection.readonly ?? undefined;
    const hotMeshClient = HotMesh.init({
      appId: targetNS,
      taskQueue,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        readonly,
        connection: this.connection,
      },
    });

    //synchronously cache the promise (before awaiting)
    ClientService.instances.set(connectionNS, hotMeshClient);

    //resolve, activate, and return the client
    const resolvedClient = await hotMeshClient;
    if (!readonly) {
      resolvedClient.engine.logger.info('durable-readonly-client', {
        guid: resolvedClient.engine.guid,
        appId: targetNS,
      });
      await this.activateWorkflow(resolvedClient, targetNS);
    }
    return resolvedClient;
  };

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
    const params = { appId: namespace ?? APP_ID, topic: workflowTopic };
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

  hashOptions(): string {
    if ('options' in this.connection) {
      //shorthand format
      return hashOptions(this.connection.options);
    } else {
      //longhand format (sub, store, stream, pub, search)
      const response = [];
      for (const p in this.connection) {
        if (this.connection[p].options) {
          response.push(hashOptions(this.connection[p].options));
        }
      }
      return response.join('');
    }
  }

  /**
   * It is possible for a client to invoke a workflow without first
   * creating the stream. This method will verify that the stream
   * exists and if not, create it.
   * @private
   */
  verifyStream = async (
    hotMeshClient: HotMesh,
    workflowTopic: string,
    namespace?: string,
  ) => {
    const optionsHash = this.hashOptions();
    const targetNS = namespace ?? APP_ID;
    const targetTopic = `${optionsHash}.${targetNS}.${workflowTopic}`;
    if (!ClientService.topics.includes(targetTopic)) {
      ClientService.topics.push(targetTopic);
      await ClientService.createStream(hotMeshClient, workflowTopic, namespace);
    }
  };

  /**
   * @private
   */
  search = async (
    hotMeshClient: HotMesh,
    index: string,
    query: string[],
  ): Promise<string[]> => {
    const searchClient = hotMeshClient.engine.search;
    return await searchClient.sendIndexedQuery(index, query);
  };

  /**
   * The Durable `Client` service provides methods for
   * starting, signaling, and querying workflows.
   * Starting a workflow is the primary use case and
   * is accessed by calling workflow.start().
   */
  workflow: ClientWorkflow = {
    /**
     * Starts a workflow, verifies the idempotent id, and
     * adds searchable data to the record.
     */
    start: async (options: WorkflowOptions): Promise<WorkflowHandleService> => {
      const taskQueueName = options.taskQueue ?? options.entity;
      const workflowName = options.taskQueue
        ? options.workflowName
        : options.entity ?? options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      //hotmesh `topic` is equivalent to `queue+workflowname` pattern in other systems
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(
        taskQueueName,
        options.namespace,
      );
      //verify that the stream channel exists before enqueueing
      await this.verifyStream(hotMeshClient, workflowTopic, options.namespace);
      const payload = {
        arguments: [...options.args],
        originJobId: options.originJobId,
        expire: options.expire ?? HMSH_EXPIRE_JOB_SECONDS,
        persistent: options.persistent,
        signalIn: options.signalIn,
        parentWorkflowId: options.parentWorkflowId,
        workflowId: options.workflowId || HotMesh.guid(),
        workflowTopic: workflowTopic,
        taskQueue: taskQueueName,
        workflowName: workflowName,
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
        maximumInterval: s(
          options.config?.maximumInterval || HMSH_DURABLE_MAX_INTERVAL,
        ),
      };

      const context = { metadata: { trc, spn }, data: {} };
      const jobId = await hotMeshClient.pub(
        `${options.namespace ?? APP_ID}.execute`,
        payload,
        context as JobState,
        {
          search: options?.search?.data,
          marker: options?.marker,
          pending: options?.pending,
          entity: options?.entity,
        },
      );
      return new WorkflowHandleService(hotMeshClient, workflowTopic, jobId);
    },

    /**
     * Sends a message payload to a running workflow that is paused and awaiting the signal.
     *
     * If the signal arrives before the workflow has registered its hook
     * (race condition under load), it is buffered as a pending signal
     * for up to `expire` (default 10 minutes). Use a longer duration
     * when signaling "early on purpose" (e.g., depositing a payload
     * hours before the workflow starts).
     */
    signal: async (
      signalId: string,
      data: StringAnyType,
      namespace?: string,
      expire?: string,
    ): Promise<string> => {
      const ns = namespace ?? APP_ID;
      const payload = {
        id: signalId,
        data,
        ...(expire ? { $expire: expire } : {}),
      };
      //send collator topic first (creates pending if no collator),
      //then inline waiter topic (delivers and cleans up collator pending)
      try {
        const signalTopic = `${ns}.wfs.signal`;
        await (
          await this.getHotMeshClient(signalTopic, namespace)
        ).signal(signalTopic, payload);
      } catch {
        //no hook rule — ignore
      }
      try {
        const waitTopic = `${ns}.wfs.wait`;
        return await (
          await this.getHotMeshClient(waitTopic, namespace)
        ).signal(waitTopic, payload);
      } catch {
        //no hook rule — ignore
      }
    },

    /**
     * Spawns an a new, isolated execution cycle within the same job.
     * Similar to `worker` functions, `hook` functions have a linked
     * function. But hooks do not start a new job and instead read/write
     * their isolated activity data to an existing Job record (HASH).
     *
     * This example spawns a hook that will update workflow `guid123`.
     *
     * @example
     * ```typescript
     * await client.workflow.hook({
     *   namespace: 'demo',
     *   taskQueue: 'default',
     *   workflowName: 'myDemoFunction',
     *   workflowId: 'guid123',
     *   args: ['Hello'],
     * });
     * ```
     */
    hook: async (options: HookOptions): Promise<string> => {
      const taskQueue = options.taskQueue ?? options.entity;
      const hookWorkflowName = options.entity ?? options.workflowName;
      const workflowTopic = `${taskQueue}-${hookWorkflowName}`;
      const payload = {
        arguments: [...options.args],
        id: options.workflowId,
        workflowTopic,
        taskQueue,
        workflowName: hookWorkflowName,
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
        maximumInterval: s(
          options.config?.maximumInterval || HMSH_DURABLE_MAX_INTERVAL,
        ),
      };
      //seed search data before entering
      const hotMeshClient = await this.getHotMeshClient(
        taskQueue,
        options.namespace,
      );
      const msgId = await hotMeshClient.signal(
        `${hotMeshClient.appId}.flow.signal`,
        payload,
        StreamStatus.PENDING,
        202,
      );
      //todo: commit search data BEFORE enqueuing hook
      if (options.search?.data) {
        const searchSessionId = `-search-${HotMesh.guid()}-0`;
        const search = new Search(
          options.workflowId,
          hotMeshClient,
          searchSessionId,
        );
        const entries = Object.entries(options.search.data).flat();
        await search.set(...entries);
      }
      //todo: support context as well as search (-context-)
      return msgId;
    },

    /**
     * Returns a reference to a running workflow,
     * allowing callers to check the status of the workflow,
     * interrupt it, and even await its eventual response
     * if still in a pending state.
     *
     * @example
     * ```typescript
     * const handle = await client.workflow.getHandle(
     *  'default',
     *  'myFunction',
     *  'someGuid123',
     *  'demo',
     * );
     * ```
     */
    getHandle: async (
      taskQueue: string,
      workflowName: string,
      workflowId: string,
      namespace?: string,
    ): Promise<WorkflowHandleService> => {
      const workflowTopic = `${taskQueue}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(taskQueue, namespace);
      return new WorkflowHandleService(
        hotMeshClient,
        workflowTopic,
        workflowId,
      );
    },

    /**
     * Provides direct access to the SEARCH backend
     *
     * @example
     * ```typescript
     * await client.workflow.search(
     *   'someTaskQueue'
     *   'someWorkflowName',
     *   'durable',
     *   'user',
     *   ...args,
     * );
     * //returns [count, [id, fields[]], [id, fields[]], [id, fields[]], ...]]
     * ```
     */
    search: async (
      taskQueue: string,
      workflowName: string,
      namespace: null | string,
      index: string,
      ...query: string[]
    ): Promise<string[]> => {
      const workflowTopic = `${taskQueue}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(taskQueue, namespace);
      try {
        return await this.search(hotMeshClient, index, query);
      } catch (error) {
        hotMeshClient.engine.logger.error('durable-client-search-err', {
          error,
        });
        throw error;
      }
    },
  };

  /**
   * Signal queue API for managing paused-workflow task records.
   * Operations: list, get, claim, claimByMetadata, release, resolve,
   * resolveByMetadata, releaseExpired.
   *
   * Requires a Postgres store provider. Methods are no-ops (return null/false)
   * when called against a non-Postgres store.
   *
   * @example
   * ```typescript
   * // Claim a pending task by metadata key
   * const task = await client.signalQueue.claimByMetadata({
   *   key: 'orderId', value: 'RX-123',
   *   assignee: 'pharmacist-jane',
   *   durationMinutes: 30,
   * });
   *
   * if (task) {
   *   await client.signalQueue.resolve({
   *     id: task.id,
   *     resolverPayload: { approved: true },
   *   });
   *   // → paused workflow resumes with { approved: true }
   * }
   * ```
   */
  signalQueue = {
    list: async (params: {
      namespace?: string;
      taskQueue?: string;
      status?: 'pending' | 'claimed' | 'resolved' | 'expired' | 'released';
      role?: string;
      limit?: number;
      offset?: number;
    } = {}) => {
      const hotMesh = await this.getHotMeshClient(null, params.namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.listSignals !== 'function') return [];
      const ns = params.namespace ?? APP_ID;
      return store.listSignals({
        namespace: ns,
        appId: store.appId,
        status: params.status,
        role: params.role,
        taskQueue: params.taskQueue,
        limit: params.limit,
        offset: params.offset,
      });
    },

    get: async (id: string, namespace?: string) => {
      const hotMesh = await this.getHotMeshClient(null, namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.getSignal !== 'function') return null;
      const ns = namespace ?? APP_ID;
      return store.getSignal({ namespace: ns, appId: store.appId, id });
    },

    claim: async (params: {
      id: string;
      namespace?: string;
      assignee?: string;
      durationMinutes?: number;
    }): Promise<import('../../types/signal').ClaimSignalResult> => {
      const hotMesh = await this.getHotMeshClient(null, params.namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.claimSignal !== 'function') {
        return { ok: false, reason: 'not-found' };
      }
      const ns = params.namespace ?? APP_ID;
      return store.claimSignal({
        namespace: ns,
        appId: store.appId,
        id: params.id,
        assignee: params.assignee,
        durationMinutes: params.durationMinutes,
      });
    },

    claimByMetadata: async (params: {
      key: string;
      value: unknown;
      namespace?: string;
      assignee?: string;
      durationMinutes?: number;
    }): Promise<import('../../types/signal').ClaimSignalResult> => {
      const hotMesh = await this.getHotMeshClient(null, params.namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.claimSignalByMetadata !== 'function') {
        return { ok: false, reason: 'not-found' };
      }
      const ns = params.namespace ?? APP_ID;
      return store.claimSignalByMetadata({
        namespace: ns,
        appId: store.appId,
        key: params.key,
        value: params.value,
        assignee: params.assignee,
        durationMinutes: params.durationMinutes,
      });
    },

    release: async (params: {
      id: string;
      namespace?: string;
    }): Promise<import('../../types/signal').ReleaseSignalResult> => {
      const hotMesh = await this.getHotMeshClient(null, params.namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.releaseSignal !== 'function') {
        return { ok: false, reason: 'not-found' };
      }
      const ns = params.namespace ?? APP_ID;
      return store.releaseSignal({
        namespace: ns,
        appId: store.appId,
        id: params.id,
      });
    },

    resolve: async (params: {
      id: string;
      namespace?: string;
      resolverPayload?: Record<string, unknown>;
    }): Promise<import('../../types/signal').ResolveSignalResult> => {
      const hotMesh = await this.getHotMeshClient(null, params.namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.resolveSignal !== 'function') {
        return { ok: false, reason: 'not-found' };
      }
      const ns = params.namespace ?? APP_ID;
      const storeResult = await store.resolveSignal({
        namespace: ns,
        appId: store.appId,
        id: params.id,
        resolverPayload: params.resolverPayload,
      });
      if (!storeResult.ok) return storeResult;
      try {
        await this.workflow.signal(
          storeResult.signalKey,
          params.resolverPayload ?? {},
          params.namespace,
        );
        return { ok: true };
      } catch {
        return { ok: false, reason: 'signal-failed', signalKey: storeResult.signalKey };
      }
    },

    resolveByMetadata: async (params: {
      key: string;
      value: unknown;
      namespace?: string;
      resolverPayload?: Record<string, unknown>;
    }): Promise<import('../../types/signal').ResolveSignalResult> => {
      const hotMesh = await this.getHotMeshClient(null, params.namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.resolveSignalByMetadata !== 'function') {
        return { ok: false, reason: 'not-found' };
      }
      const ns = params.namespace ?? APP_ID;
      const storeResult = await store.resolveSignalByMetadata({
        namespace: ns,
        appId: store.appId,
        key: params.key,
        value: params.value,
        resolverPayload: params.resolverPayload,
      });
      if (!storeResult.ok) return storeResult;
      try {
        await this.workflow.signal(
          storeResult.signalKey,
          params.resolverPayload ?? {},
          params.namespace,
        );
        return { ok: true };
      } catch {
        return { ok: false, reason: 'signal-failed', signalKey: storeResult.signalKey };
      }
    },

    releaseExpired: async (namespace?: string): Promise<number> => {
      const hotMesh = await this.getHotMeshClient(null, namespace);
      const store = hotMesh.engine.store as any;
      if (typeof store.releaseExpiredSignals !== 'function') return 0;
      const ns = namespace ?? APP_ID;
      return store.releaseExpiredSignals({ namespace: ns, appId: store.appId });
    },
  };

  /**
   * Any router can be used to deploy and activate the HotMesh
   * distributed executable to the active quorum EXCEPT for
   * those routers in `readonly` mode.
   */
  async deployAndActivate(
    namespace = APP_ID,
    version = APP_VERSION,
  ): Promise<void> {
    if (isNaN(Number(version))) {
      throw new Error('Invalid version number');
    }
    const hotMesh = await this.getHotMeshClient('', namespace);
    await this.activateWorkflow(hotMesh, namespace, version);
  }

  /**
   * @private
   */
  async verifyWorkflowActive(
    hotMesh: HotMesh,
    appId = APP_ID,
    count = 0,
  ): Promise<boolean> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if (isNaN(appVersion)) {
      if (count > 10) {
        throw new Error('Workflow failed to activate');
      }
      await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
      return await this.verifyWorkflowActive(hotMesh, appId, count + 1);
    }
    return true;
  }

  /**
   * @private
   */
  async activateWorkflow(
    hotMesh: HotMesh,
    appId = APP_ID,
    version = APP_VERSION,
  ): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as string;
    if (appVersion === version && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-activate-err', {
          error,
        });
        throw error;
      }
    } else if (isNaN(Number(appVersion)) || appVersion < version) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', {
          error,
        });
        throw error;
      }
    }
  }

  /**
   * @private
   */
  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of ClientService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
