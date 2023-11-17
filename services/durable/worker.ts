import {
  DurableFatalError,
  DurableIncompleteSignalError,
  DurableMaxedError,
  DurableRetryError,
  DurableSleepError,
  DurableTimeoutError, 
  DurableWaitForSignalError} from '../../modules/errors';
import { asyncLocalStorage } from './asyncLocalStorage';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './factory';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ActivityWorkflowDataType,
  Connection,
  Registry,
  WorkerConfig,
  WorkerOptions,
  WorkflowDataType, 
  WorkflowSearchOptions} from "../../types/durable";
import { RedisClass, RedisOptions } from '../../types/redis';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus } from '../../types/stream';
import { KeyService, KeyType } from '../../modules/key';

export class WorkerService {
  static activityRegistry: Registry = {}; //user's activities
  static connection: Connection;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();
  workflowRunner: HotMesh;
  activityRunner: HotMesh;

  static getHotMesh = async (worflowTopic: string, options?: WorkerOptions) => {
    if (WorkerService.instances.has(worflowTopic)) {
      return await WorkerService.instances.get(worflowTopic);
    }
    const hotMeshClient = HotMesh.init({
      appId: APP_ID,
      engine: { redis: { ...WorkerService.connection } }
    });
    WorkerService.instances.set(worflowTopic, hotMeshClient);
    await WorkerService.activateWorkflow(await hotMeshClient);
    return hotMeshClient;
  }

  static async activateWorkflow(hotMesh: HotMesh) {
    const app = await hotMesh.engine.store.getApp(APP_ID);
    const appVersion = app?.version;
    if(!appVersion) {
      try {
        await hotMesh.deploy(getWorkflowYAML(APP_ID, APP_VERSION));
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-deploy-activate-err', err);
        throw err;
      }
    } else if(app && !app.active) {
      try {
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-activate-err', err);
        throw err;
      }
    }
  }

  /**
   * NOTE: Because the worker imports the workflows dynamically AFTER
   * the activities are loaded, there will be items in the registry,
   * allowing proxyActivities to succeed.
   */
  static registerActivities<ACT>(activities: ACT): Registry  {
    if (typeof activities === 'function' && typeof WorkerService.activityRegistry[activities.name] !== 'function') {
      WorkerService.activityRegistry[activities.name] = activities as Function;
    } else {
      Object.keys(activities).forEach(key => {
        if (activities[key].name && typeof WorkerService.activityRegistry[activities[key].name] !== 'function') {
          WorkerService.activityRegistry[activities[key].name] = (activities as any)[key] as Function;
        }
      });
    }
    return WorkerService.activityRegistry;
  }

  /**
   * For those deployments with a redis stack backend (with the FT module),
   * this method will configure the search index for the workflow.
   */
  //todo: bind this to the Search service; update constructor to expect hotMeshClient as first param (id is optional
  //refactor and delete other one as well)
  static async configureSearchIndex(hotMeshClient: HotMesh, search?: WorkflowSearchOptions): Promise<void> {
    if (search?.schema) {
      const store = hotMeshClient.engine.store;
      const schema: string[] = [];
      for (const [key, value] of Object.entries(search.schema)) {
        //prefix with a comma (avoids collisions with hotmesh reserved words)
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

  static async create(config: WorkerConfig) {
    //always call `registerActivities` before `import`
    WorkerService.connection = config.connection;
    const workflow = config.workflow;
    const [workflowFunctionName, workflowFunction] = WorkerService.resolveWorkflowTarget(workflow);
    const baseTopic = `${config.taskQueue}-${workflowFunctionName}`;
    const activityTopic = `${baseTopic}-activity`;
    const workflowTopic = `${baseTopic}`;

    //initialize supporting workflows
    const worker = new WorkerService();
    worker.activityRunner = await worker.initActivityWorker(config, activityTopic);
    worker.workflowRunner = await worker.initWorkflowWorker(config, workflowTopic, workflowFunction);
    WorkerService.configureSearchIndex(worker.workflowRunner, config.search)
    await WorkerService.activateWorkflow(worker.workflowRunner);
    return worker;
  }

  static resolveWorkflowTarget(workflow: object | Function): [string, Function] {
    let workflowFunction: Function;
    if (typeof workflow === 'function') {
      workflowFunction = workflow;
    } else {
      const workflowFunctionNames = Object.keys(workflow);
      workflowFunction = workflow[workflowFunctionNames[workflowFunctionNames.length - 1]];
      return WorkerService.resolveWorkflowTarget(workflowFunction);
    }
    return [workflowFunction.name, workflowFunction];
  }

  async run() {
    this.workflowRunner.engine.logger.info('WorkerService is running');
  }

  async initActivityWorker(config: WorkerConfig, activityTopic: string): Promise<HotMesh> {
    const redisConfig = {
      class: config.connection.class as RedisClass,
      options: config.connection.options as RedisOptions
    };
    const hotMeshWorker = await HotMesh.init({
      appId: APP_ID,
      engine: { redis: redisConfig },
      workers: [
        { topic: activityTopic,
          redis: redisConfig,
          callback: this.wrapActivityFunctions().bind(this)
        }
      ]
    });
    WorkerService.instances.set(activityTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  wrapActivityFunctions(): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      try {
        //always run the activity function when instructed; return the response
        const activityInput = data.data as unknown as ActivityWorkflowDataType;
        const activityName = activityInput.activityName;
        const activityFunction = WorkerService.activityRegistry[activityName];
        const pojoResponse = await activityFunction.apply(this, activityInput.arguments);

        return {
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: pojoResponse }
        };
      } catch (err) {
        this.activityRunner.engine.logger.error('durable-worker-activity-err', err);
        if (!(err instanceof DurableTimeoutError) &&
          !(err instanceof DurableMaxedError) &&
          !(err instanceof DurableFatalError)) {
          err = new DurableRetryError(err.message); 
        }
        return {
          status: StreamStatus.ERROR,
          code: err.code,
          metadata: { ...data.metadata },
          data: { message: err.message }
        } as StreamDataResponse;
      }
    }
  }

  async initWorkflowWorker(config: WorkerConfig, workflowTopic: string, workflowFunction: Function): Promise<HotMesh> {
    const redisConfig = {
      class: config.connection.class as RedisClass,
      options: config.connection.options as RedisOptions
    };
    const hotMeshWorker = await HotMesh.init({
      appId: APP_ID,
      engine: { redis: redisConfig },
      workers: [{
        topic: workflowTopic,
        redis: redisConfig,
        callback: this.wrapWorkflowFunction(workflowFunction, workflowTopic).bind(this)
      }]
    });
    WorkerService.instances.set(workflowTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  static Context = {
    info: () => {
      return {
        workflowId: '',
        workflowTopic: '',
      }
    },
  };

  wrapWorkflowFunction(workflowFunction: Function, workflowTopic: string): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
     const counter = { counter: 0 };
      try {
        //incoming data payload has arguments and workflowId
        const workflowInput = data.data as unknown as WorkflowDataType;
        const context = new Map();
        context.set('counter', counter);
        context.set('workflowId', workflowInput.workflowId);
        context.set('workflowTopic', workflowTopic);
        context.set('workflowName', workflowTopic.split('-').pop());
        context.set('workflowTrace', data.metadata.trc);
        context.set('workflowSpan', data.metadata.spn);
        const workflowResponse = await asyncLocalStorage.run(context, async () => {
          return await workflowFunction.apply(this, workflowInput.arguments);
        });

        return {
          code: 200,
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: workflowResponse, done: true }
        };
      } catch (err) {

        //not an error...just a trigger to sleep
        if (err instanceof DurableSleepError) {
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({ duration: err.duration, index: err.index }),
              duration: err.duration,
              index: err.index
            }
          } as StreamDataResponse;

        //not an error...just a trigger to wait for a signal
        } else if (err instanceof DurableWaitForSignalError) {
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              signals: err.signals,
              index: err.signals[0].index
            }
          } as StreamDataResponse;

        //not an error...still waiting for all the signals to arrive
        } else if (err instanceof DurableIncompleteSignalError) {
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: { code: err.code }
          } as StreamDataResponse;
        }

        // all other errors are fatal (598, 597, 596) or will be retried (599)
        return {
          status: StreamStatus.ERROR,
          code: err.code || new DurableRetryError(err.message).code,
          metadata: { ...data.metadata },
          data: { message: err.message, type: err.name }
        } as StreamDataResponse;
      }
    }
  }

  static async shutdown(): Promise<void> {
    for (const [key, value] of WorkerService.instances) {
      const hotMesh = await value;
      await hotMesh.stop();
    }
  }
}
