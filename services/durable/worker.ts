import {
  DurableFatalError,
  DurableIncompleteSignalError,
  DurableMaxedError,
  DurableRetryError,
  DurableSleepError,
  DurableSleepForError,
  DurableTimeoutError, 
  DurableWaitForSignalError} from '../../modules/errors';
import { asyncLocalStorage } from '../../modules/storage';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './factory';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ActivityWorkflowDataType,
  Connection,
  Registry,
  WorkerConfig,
  WorkerOptions,
  WorkflowDataType } from '../../types/durable';
import { RedisClass, RedisOptions } from '../../types/redis';
import { Search } from './search';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus } from '../../types/stream';

export class WorkerService {
  static activityRegistry: Registry = {}; //user's activities
  static connection: Connection;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();
  workflowRunner: HotMesh;
  activityRunner: HotMesh;

  static getHotMesh = async (workflowTopic: string, config?: Partial<WorkerConfig>, options?: WorkerOptions) => {
    if (WorkerService.instances.has(workflowTopic)) {
      return await WorkerService.instances.get(workflowTopic);
    }
    const hotMeshClient = HotMesh.init({
      logLevel: options?.logLevel as 'debug' ?? 'info',
      appId: config.namespace ?? APP_ID,
      engine: { redis: { ...WorkerService.connection } }
    });
    WorkerService.instances.set(workflowTopic, hotMeshClient);
    await WorkerService.activateWorkflow(await hotMeshClient);
    return hotMeshClient;
  }

  static async activateWorkflow(hotMesh: HotMesh) {
    const app = await hotMesh.engine.store.getApp(hotMesh.engine.appId);
    const appVersion = app?.version;
    if(!appVersion) {
      try {
        await hotMesh.deploy(getWorkflowYAML(hotMesh.engine.appId, APP_VERSION));
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

  static registerActivities<ACT>(activities: ACT): Registry  {
    if (typeof activities === 'function' && typeof WorkerService.activityRegistry[activities.name] !== 'function') {
      WorkerService.activityRegistry[activities.name] = activities as Function;
    } else {
      Object.keys(activities).forEach(key => {
        if (activities[key].name && typeof WorkerService.activityRegistry[activities[key].name] !== 'function') {
          WorkerService.activityRegistry[activities[key].name] = (activities as any)[key] as Function;
        } else if (typeof (activities as any)[key] === 'function') {
          WorkerService.activityRegistry[key] = (activities as any)[key] as Function;
        }
      });
    }
    return WorkerService.activityRegistry;
  }

  static async create(config: WorkerConfig): Promise<WorkerService> {
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
    Search.configureSearchIndex(worker.workflowRunner, config.search)
    await WorkerService.activateWorkflow(worker.workflowRunner);
    return worker;
  }

  static resolveWorkflowTarget(workflow: object | Function, name?: string): [string, Function] {
    let workflowFunction: Function;
    if (typeof workflow === 'function') {
      workflowFunction = workflow;
      return [workflowFunction.name ?? name, workflowFunction];
    } else {
      const workflowFunctionNames = Object.keys(workflow);
      const lastFunctionName = workflowFunctionNames[workflowFunctionNames.length - 1];
      workflowFunction = workflow[lastFunctionName];
      return WorkerService.resolveWorkflowTarget(workflowFunction, lastFunctionName);
    }
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
      logLevel: config.options?.logLevel as 'debug' ?? 'info',
      appId: config.namespace ?? APP_ID,
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
      logLevel: config.options?.logLevel as 'debug' ?? 'info',
      appId: config.namespace ?? APP_ID,
      engine: { redis: redisConfig },
      workers: [{
        topic: workflowTopic,
        redis: redisConfig,
        callback: this.wrapWorkflowFunction(workflowFunction, workflowTopic, config).bind(this)
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

  wrapWorkflowFunction(workflowFunction: Function, workflowTopic: string, config: WorkerConfig): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
     const counter = { counter: 0 };
      try {
        //incoming data payload has arguments and workflowId
        const workflowInput = data.data as unknown as WorkflowDataType;
        const context = new Map();
        context.set('raw', data);
        context.set('namespace', config.namespace ?? APP_ID);
        context.set('counter', counter);
        context.set('workflowId', workflowInput.workflowId);
        context.set('workflowId', workflowInput.workflowId);
        if (workflowInput.originJobId) {
          //if present there is an origin job to which this job is subordinated; 
          // garbage collect (expire) this job when originJobId is expired
          context.set('originJobId', workflowInput.originJobId);
        }
        if (workflowInput.workflowDimension) {
          //every hook function runs in an isolated dimension controlled
          //by the index assigned when the signal was received; even if the
          //hook function re-runs, its scope will always remain constant
          context.set('workflowDimension', workflowInput.workflowDimension);
        }
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
        if (err instanceof DurableSleepForError) {
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({ duration: err.duration, index: err.index, dimension: err.dimension }),
              duration: err.duration,
              index: err.index,
              dimension: err.dimension
            }
          } as StreamDataResponse;

        //deprecated format; not an error...just a trigger to sleep
        } else if (err instanceof DurableSleepError) {
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({ duration: err.duration, index: err.index, dimension: err.dimension }),
              duration: err.duration,
              index: err.index,
              dimension: err.dimension
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
    for (const [_, hotMeshInstance] of WorkerService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
