import { APP_ID, APP_VERSION, DEFAULT_COEFFICIENT, getWorkflowYAML } from './factory';
import { WorkflowHandleService } from './handle';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ClientConfig,
  Connection,
  HookOptions,
  WorkflowOptions, 
  WorkflowSearchOptions} from '../../types/durable';
import { JobState } from '../../types/job';
import { KeyService, KeyType } from '../../modules/key';
import { Search } from './search';
import { StreamStatus } from '../../types';
import { HMSH_LOGLEVEL, HMSH_EXPIRE_JOB_SECONDS } from '../../modules/enums';

export class ClientService {

  connection: Connection;
  topics: string[] = [];
  options: WorkflowOptions;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  constructor(config: ClientConfig) {
    this.connection = config.connection;
  }

  getHotMeshClient = async (workflowTopic: string, namespace?: string) => {
    //use the cached instance
    const instanceId = 'SINGLETON';
    if (ClientService.instances.has(instanceId)) {
      const hotMeshClient = await ClientService.instances.get(instanceId);
      if (!this.topics.includes(workflowTopic)) {
        this.topics.push(workflowTopic);
        await this.createStream(hotMeshClient, workflowTopic, namespace);
      }
      return hotMeshClient;
    }

    //create and cache an instance
    const hotMeshClient = HotMesh.init({
      appId: namespace ?? APP_ID,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: {
          class: this.connection.class,
          options: this.connection.options,
        }
      }
    });
    ClientService.instances.set(instanceId, hotMeshClient);
    await this.createStream(await hotMeshClient, workflowTopic, namespace);
    await this.activateWorkflow(await hotMeshClient, namespace ?? APP_ID);
    return hotMeshClient;
  }

  /**
   * Creates a stream (Redis `XGROUP.CREATE`) where events can be published (XADD).
   * It is possible that the worker that will read from this stream channel
   * has not yet been initialized, so this call ensures that the channel
   * exists and is ready to serve as a container for events.
   */
  createStream = async(hotMeshClient: HotMesh, workflowTopic: string, namespace?: string) => {
    const store = hotMeshClient.engine.store;
    const params = { appId: namespace ?? APP_ID, topic: workflowTopic };
    const streamKey = store.mintKey(KeyType.STREAMS, params);
    try {
      await store.xgroup('CREATE', streamKey, 'WORKER', '$', 'MKSTREAM');
    } catch (err) {
      //ignore if already exists
    }
  }

  /**
   * For those deployments with a redis stack backend (with the FT module),
   * this method will configure the search index for the workflow.
   */
  configureSearchIndex = async (hotMeshClient: HotMesh, search?: WorkflowSearchOptions): Promise<void> => {
    if (search?.schema) {
      const store = hotMeshClient.engine.store;
      const schema: string[] = [];
      for (const [key, value] of Object.entries(search.schema)) {
        //prefix with an underscore (avoids collisions with hotmesh reserved symbols)
        schema.push(`_${key}`);
        schema.push(value.type);
        if (value.sortable) {
          schema.push('SORTABLE');
        }
      }
      try {
        const keyParams = {
          appId: hotMeshClient.appId,
          jobId: ''
        }
        const hotMeshPrefix = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
        const prefixes = search.prefix.map((prefix) => `${hotMeshPrefix}${prefix}`);
        await store.exec('FT.CREATE', `${search.index}`, 'ON', 'HASH', 'PREFIX', prefixes.length, ...prefixes, 'SCHEMA', ...schema);
      } catch (err) {
        hotMeshClient.engine.logger.info('durable-client-search-err', { err });
      }
    }
  }

  search = async (hotMeshClient: HotMesh, index: string, query: string[]): Promise<string[]> => {
    const store = hotMeshClient.engine.store;
    if (query[0]?.startsWith('FT.')) {
      return await store.exec(...query) as string[];
    }
    return await store.exec('FT.SEARCH', index, ...query) as string[];
  }

  workflow = {
    start: async (options: WorkflowOptions): Promise<WorkflowHandleService> => {
      const taskQueueName = options.entity ?? options.taskQueue;
      const workflowName = options.entity ?? options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      //NOTE: HotMesh 'workflowTopic' is a created by concatenating
      //     the taskQueue and workflowName used by the Durable module
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(workflowTopic, options.namespace);
      this.configureSearchIndex(hotMeshClient, options.search);
      const payload = {
        arguments: [...options.args],
        originJobId: options.originJobId,
        expire: options.expire ?? HMSH_EXPIRE_JOB_SECONDS,
        parentWorkflowId: options.parentWorkflowId,
        workflowId: options.workflowId || HotMesh.guid(),
        workflowTopic: workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || DEFAULT_COEFFICIENT,
      }
      const context = { metadata: { trc, spn }, data: {}};
      const jobId = await hotMeshClient.pub(
        `${options.namespace ?? APP_ID}.execute`,
        payload,
        context as JobState
      );
      // Seed search data
      if (jobId && options.search?.data) {
        const searchSessionId = `-search-0`;
        const search = new Search(jobId, hotMeshClient, searchSessionId);
        const entries = Object.entries(options.search.data).flat();
        await search.set(...entries);
      }
      return new WorkflowHandleService(hotMeshClient, workflowTopic, jobId);
    },

    /**
     * send a message to a running workflow that is paused and awaiting the signal
     */
    signal: async (signalId: string, data: Record<any, any>, namespace?: string): Promise<string> => {
      const topic = `${namespace ?? APP_ID}.wfs.signal`;
      return await (await this.getHotMeshClient(topic, namespace)).hook(topic, { id: signalId, data });
    },

    /**
     * send a message to spawn an parallel in-process thread of execution
     * with the same job state as the main thread but bound to a different
     * handler function. All job state will be journaled to the same hash
     * as is used by the main thread.
     */
    hook: async (options: HookOptions): Promise<string> => {
      const workflowTopic = `${options.taskQueue}-${options.workflowName}`;
      const payload = {
        arguments: [...options.args],
        id: options.workflowId,
        workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || DEFAULT_COEFFICIENT,
      }
      //seed search data if presentthe hook before entering
      const hotMeshClient = await this.getHotMeshClient(workflowTopic, options.namespace);
      const msgId = await hotMeshClient.hook(`${hotMeshClient.appId}.flow.signal`, payload, StreamStatus.PENDING, 202);
      if (options.search?.data) {
        const searchSessionId = `-search-${HotMesh.guid()}-0`;
        const search = new Search(options.workflowId, hotMeshClient, searchSessionId);
        const entries = Object.entries(options.search.data).flat();
        await search.set(...entries);
      }
      return msgId;
    },

    getHandle: async (taskQueue: string, workflowName: string, workflowId: string, namespace?: string): Promise<WorkflowHandleService> => {
      const workflowTopic = `${taskQueue}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(workflowTopic, namespace);
      return new WorkflowHandleService(hotMeshClient, workflowTopic, workflowId);
    },

    search: async (taskQueue: string, workflowName: string, namespace: null | string, index: string, ...query: string[]): Promise<string[]> => {
      const workflowTopic = `${taskQueue}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(workflowTopic, namespace);
      try {
        return await this.search(hotMeshClient, index, query);
      } catch (err) {
        hotMeshClient.engine.logger.error('durable-client-search-err', { err });
        throw err;
      }
    }
  }

  async activateWorkflow(hotMesh: HotMesh, appId = APP_ID, version = APP_VERSION): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if(isNaN(appVersion)) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', { error });
        throw error;
      }
    } else if(app && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-activate-err', { error});
        throw error;
      }
    }
  }

  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of ClientService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
