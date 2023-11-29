import { nanoid } from 'nanoid';
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

export class ClientService {

  connection: Connection;
  options: WorkflowOptions;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  constructor(config: ClientConfig) {
    this.connection = config.connection;
  }

  getHotMeshClient = async (worflowTopic: string, namespace?: string) => {
    //NOTE: every unique topic inits a new engine
    if (ClientService.instances.has(worflowTopic)) {
      return await ClientService.instances.get(worflowTopic);
    }

    const hotMeshClient = HotMesh.init({
      appId: namespace ?? APP_ID,
      engine: {
        redis: {
          class: this.connection.class,
          options: this.connection.options,
        }
      }
    });
    ClientService.instances.set(worflowTopic, hotMeshClient);

    //since the YAML topic is dynamic, it MUST be manually created before use
    const store = (await hotMeshClient).engine.store;
    const params = { appId: namespace ?? APP_ID, topic: worflowTopic };
    const streamKey = store.mintKey(KeyType.STREAMS, params);
    try {
      await store.xgroup('CREATE', streamKey, 'WORKER', '$', 'MKSTREAM');
    } catch (err) {
      //ignore if already exists
    }
    await this.activateWorkflow(await hotMeshClient, namespace ?? APP_ID);
    return hotMeshClient;
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
    return await store.exec('FT.SEARCH', index, ...query) as string[];
  }

  workflow = {
    start: async (options: WorkflowOptions): Promise<WorkflowHandleService> => {
      const taskQueueName = options.taskQueue;
      const workflowName = options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      //topic is concat of taskQueue and workflowName
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(workflowTopic, options.namespace);
      this.configureSearchIndex(hotMeshClient, options.search)
      const payload = {
        arguments: [...options.args],
        parentWorkflowId: options.parentWorkflowId,
        workflowId: options.workflowId || nanoid(),
        workflowTopic: workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || DEFAULT_COEFFICIENT,
      }
      const context = { metadata: { trc, spn }, data: {}};
      const jobId = await hotMeshClient.pub(
        `${options.namespace ?? APP_ID}.execute`,
        payload,
        context as JobState);
       if (jobId && options.search?.data) {
        //job successfully kicked off; there is default job data to persist
        const searchSessionId = `-search-0`;
        const search = new Search(jobId, hotMeshClient, searchSessionId);
        for (const [key, value] of Object.entries(options.search.data)) {
          search.set(key, value);
        }
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
      const hotMeshClient = await this.getHotMeshClient(workflowTopic, options.namespace);
      return await hotMeshClient.hook(`${hotMeshClient.appId}.flow.signal`, payload, StreamStatus.PENDING, 202);
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
    for (const [key, value] of ClientService.instances) {
      const hotMesh = await value;
      await hotMesh.stop();
    }
  }
}
