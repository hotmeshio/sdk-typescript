import ms from 'ms';

import {
  APP_ID,
  APP_VERSION,
  getWorkflowYAML } from './schemas/factory';
import {
  HMSH_LOGLEVEL,
  HMSH_EXPIRE_JOB_SECONDS,
  HMSH_QUORUM_DELAY_MS,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_DURABLE_MAX_INTERVAL } from '../../modules/enums';
import { sleepFor } from '../../modules/utils';
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
  static topics: string[] = [];
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  constructor(config: ClientConfig) {
    this.connection = config.connection;
  }

  getHotMeshClient = async (workflowTopic: string, namespace?: string) => {
    const targetNS = namespace ?? APP_ID;
    if (ClientService.instances.has(targetNS)) {
      const hotMeshClient = await ClientService.instances.get(targetNS);
      await this.verifyWorkflowActive(hotMeshClient, targetNS);
      if (!ClientService.topics.includes(workflowTopic)) {
        ClientService.topics.push(workflowTopic);
        await ClientService.createStream(hotMeshClient, workflowTopic, namespace);
      }
      return hotMeshClient;
    }

    //create and cache an instance
    const hotMeshClient = HotMesh.init({
      appId: targetNS,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: {
          class: this.connection.class,
          options: this.connection.options,
        }
      }
    });
    ClientService.instances.set(targetNS, hotMeshClient);
    await ClientService.createStream(await hotMeshClient, workflowTopic, namespace);
    await this.activateWorkflow(await hotMeshClient, targetNS);
    return hotMeshClient;
  }

  /**
   * Creates a stream (Redis `XGROUP.CREATE`) where events can be published (XADD).
   * It is possible that the worker that will read from this stream channel
   * has not yet been initialized, so this call ensures that the channel
   * exists and is ready to serve as a container for events.
   */
  static createStream = async(hotMeshClient: HotMesh, workflowTopic: string, namespace?: string) => {
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
   * It is possible for a client to invoke a workflow without first
   * creating the stream. This method will verify that the stream
   * exists and if not, create it.
   */
  static verifyStream = async(workflowTopic: string, namespace?: string) => {
    const targetNS = namespace ?? APP_ID;
    if (ClientService.instances.has(targetNS)) {
      const hotMeshClient = await ClientService.instances.get(targetNS);
      if (!ClientService.topics.includes(workflowTopic)) {
        ClientService.topics.push(workflowTopic);
        await ClientService.createStream(hotMeshClient, workflowTopic, namespace);
      }
      return hotMeshClient;
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
      const payload = {
        arguments: [...options.args],
        originJobId: options.originJobId,
        expire: options.expire ?? HMSH_EXPIRE_JOB_SECONDS,
        parentWorkflowId: options.parentWorkflowId,
        workflowId: options.workflowId || HotMesh.guid(),
        workflowTopic: workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
        maximumAttempts: options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
        maximumInterval: ms(options.config?.maximumInterval || HMSH_DURABLE_MAX_INTERVAL) / 1000,
      }

      const context = { metadata: { trc, spn }, data: {}};
      const jobId = await hotMeshClient.pub(
        `${options.namespace ?? APP_ID}.execute`,
        payload,
        context as JobState,
        { search: options?.search?.data, marker: options?.marker},
      );
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
        backoffCoefficient: options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
        maximumAttempts: options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
        maximumInterval: ms(options.config?.maximumInterval || HMSH_DURABLE_MAX_INTERVAL) / 1000,
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
      } catch (error) {
        hotMeshClient.engine.logger.error('durable-client-search-err', { ...error });
        throw error;
      }
    }
  }

  async verifyWorkflowActive(hotMesh: HotMesh, appId = APP_ID, count = 0): Promise<boolean> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if(isNaN(appVersion)) {
      if (count > 10) {
        throw new Error('Workflow failed to activate');
      }
      await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
      return await this.verifyWorkflowActive(hotMesh, appId, count + 1);
    }
    return true;
  }

  async activateWorkflow(hotMesh: HotMesh, appId = APP_ID, version = APP_VERSION): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if(isNaN(appVersion)) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', { ...error });
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
