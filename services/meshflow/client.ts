import {
  HMSH_LOGLEVEL,
  HMSH_EXPIRE_JOB_SECONDS,
  HMSH_QUORUM_DELAY_MS,
  HMSH_MESHFLOW_EXP_BACKOFF,
  HMSH_MESHFLOW_MAX_ATTEMPTS,
  HMSH_MESHFLOW_MAX_INTERVAL,
} from '../../modules/enums';
import { hashOptions, s, sleepFor } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import {
  ClientConfig,
  ClientWorkflow,
  Connection,
  HookOptions,
  WorkflowOptions,
} from '../../types/meshflow';
import { JobState } from '../../types/job';
import { KeyType } from '../../modules/key';
import { StreamStatus, StringAnyType } from '../../types';

import { Search } from './search';
import { WorkflowHandleService } from './handle';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './schemas/factory';

/**
 * The MeshFlow `Client` service is functionally
 * equivalent to the Temporal `Client` service.
 * Start a new workflow execution by calling
 * `workflow.start`. Note the direct connection to
 * Postgres.
 * 
 * NATS can be used as the message broker if advanced
 * messaging is required (i.e, patterned subscriptions).
 * @example

 * ```typescript
 * //client.ts
 * import { Client, HotMesh } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';

 * async function run(): Promise<string> {
 *   const client = new Client({
 *     connection: {
 *       class: Postgres,
 *       options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *     }
 *   });

 *   const handle = await client.workflow.start({
 *     args: ['HotMesh'],
 *     taskQueue: 'default',
 *     workflowName: 'example',
 *     workflowId: HotMesh.guid()
 *   });

 *   return await handle.result();
 *   //returns ['Hello HotMesh', '¡Hola, HotMesh!']
 * }
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
  getHotMeshClient = async (
    workflowTopic: string | null,
    namespace?: string,
  ) => {
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

    //create and cache an instance
    const connectionType =
      'options' in this.connection ? 'connection' : 'connections';
    const hotMeshClient = HotMesh.init({
      appId: targetNS,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        [connectionType]: this.connection,
      },
    });
    ClientService.instances.set(connectionNS, hotMeshClient);
    await this.activateWorkflow(await hotMeshClient, targetNS);
    return hotMeshClient;
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
        if (!response.includes(this.connection[p])) {
          response.push(hashOptions(this.connection[p]));
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
   * The MeshFlow `Client` service is functionally
   * equivalent to the Temporal `Client` service.
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
      const workflowName = options.entity ?? options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      //hotmesh `topic` is equivalent to `queue+workflowname` pattern in other systems
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(
        workflowTopic,
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
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_MESHFLOW_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_MESHFLOW_MAX_ATTEMPTS,
        maximumInterval: s(
          options.config?.maximumInterval || HMSH_MESHFLOW_MAX_INTERVAL,
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
        },
      );
      return new WorkflowHandleService(hotMeshClient, workflowTopic, jobId);
    },

    /**
     * Sends a message payload to a running workflow that is paused and awaiting the signal
     */
    signal: async (
      signalId: string,
      data: StringAnyType,
      namespace?: string,
    ): Promise<string> => {
      const topic = `${namespace ?? APP_ID}.wfs.signal`;
      return await (
        await this.getHotMeshClient(topic, namespace)
      ).hook(topic, { id: signalId, data });
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
      const workflowTopic = `${options.taskQueue ?? options.entity}-${options.entity ?? options.workflowName}`;
      const payload = {
        arguments: [...options.args],
        id: options.workflowId,
        workflowTopic,
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_MESHFLOW_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_MESHFLOW_MAX_ATTEMPTS,
        maximumInterval: s(
          options.config?.maximumInterval || HMSH_MESHFLOW_MAX_INTERVAL,
        ),
      };
      //seed search data before entering
      const hotMeshClient = await this.getHotMeshClient(
        workflowTopic,
        options.namespace,
      );
      const msgId = await hotMeshClient.hook(
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
      const hotMeshClient = await this.getHotMeshClient(
        workflowTopic,
        namespace,
      );
      return new WorkflowHandleService(
        hotMeshClient,
        workflowTopic,
        workflowId,
      );
    },

    /**
     * Provides direct access to the SEARCH backend when making
     * queries. Taskqueues and workflow names are
     * used to identify the point of presence to use. `...args` is
     * the tokenized query. When querying Redis/FTSEARCH, the trailing
     * ...args might be `'@_custom1:meshflow'`. For postgres,
     * the trailing ...args would be: `'_custom', 'meshflow'`. In each case,
     * the query looks for all job data where the field `_custom` is
     * equal to `meshflow`.
     *
     * @example
     * ```typescript
     * await client.workflow.search(
     *   'someTaskQueue'
     *   'someWorkflowName',
     *   'meshflow',
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
      const hotMeshClient = await this.getHotMeshClient(
        workflowTopic,
        namespace,
      );
      try {
        return await this.search(hotMeshClient, index, query);
      } catch (error) {
        hotMeshClient.engine.logger.error('meshflow-client-search-err', {
          ...error,
        });
        throw error;
      }
    },
  };

  /**
   * Any point of presence can be used to deploy and activate the HotMesh
   * distributed executable to the active quorum.
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
        hotMesh.engine.logger.error('meshflow-client-activate-err', { error });
        throw error;
      }
    } else if (isNaN(Number(appVersion)) || appVersion < version) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('meshflow-client-deploy-activate-err', {
          ...error,
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
