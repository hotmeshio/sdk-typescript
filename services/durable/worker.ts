import ms from 'ms';

import {
  HMSH_CODE_DURABLE_ALL,
  HMSH_CODE_DURABLE_RETRYABLE,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_MAX_INTERVAL,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_LOGLEVEL,
} from '../../modules/enums';
import {
  DurableChildError,
  DurableFatalError,
  DurableMaxedError,
  DurableProxyError,
  DurableRetryError,
  DurableSleepError,
  DurableTimeoutError,
  DurableWaitForError,
} from '../../modules/errors';
import { asyncLocalStorage } from '../../modules/storage';
import { formatISODate, guid } from '../../modules/utils';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ActivityWorkflowDataType,
  Connection,
  Registry,
  WorkerConfig,
  WorkerOptions,
  WorkflowDataType,
} from '../../types/durable';
import { RedisClass, RedisOptions } from '../../types/redis';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../types/stream';

import { Search } from './search';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './schemas/factory';

export class WorkerService {
  static activityRegistry: Registry = {}; //user's activities
  static connection: Connection;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();
  workflowRunner: HotMesh;
  activityRunner: HotMesh;

  static getHotMesh = async (
    workflowTopic: string,
    config?: Partial<WorkerConfig>,
    options?: WorkerOptions,
  ) => {
    if (WorkerService.instances.has(workflowTopic)) {
      return await WorkerService.instances.get(workflowTopic);
    }
    const hotMeshClient = HotMesh.init({
      logLevel: options?.logLevel ?? HMSH_LOGLEVEL,
      appId: config.namespace ?? APP_ID,
      engine: { redis: { ...WorkerService.connection } },
    });
    WorkerService.instances.set(workflowTopic, hotMeshClient);
    await WorkerService.activateWorkflow(await hotMeshClient);
    return hotMeshClient;
  };

  static async activateWorkflow(hotMesh: HotMesh) {
    const app = await hotMesh.engine.store.getApp(hotMesh.engine.appId);
    const appVersion = app?.version;
    if (!appVersion) {
      try {
        await hotMesh.deploy(
          getWorkflowYAML(hotMesh.engine.appId, APP_VERSION),
        );
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-deploy-activate-err', err);
        throw err;
      }
    } else if (app && !app.active) {
      try {
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-activate-err', err);
        throw err;
      }
    }
  }

  static registerActivities<ACT>(activities: ACT): Registry {
    if (
      typeof activities === 'function' &&
      typeof WorkerService.activityRegistry[activities.name] !== 'function'
    ) {
      WorkerService.activityRegistry[activities.name] = activities as Function;
    } else {
      Object.keys(activities).forEach((key) => {
        if (
          activities[key].name &&
          typeof WorkerService.activityRegistry[activities[key].name] !==
            'function'
        ) {
          WorkerService.activityRegistry[activities[key].name] = (
            activities as any
          )[key] as Function;
        } else if (typeof (activities as any)[key] === 'function') {
          WorkerService.activityRegistry[key] = (activities as any)[
            key
          ] as Function;
        }
      });
    }
    return WorkerService.activityRegistry;
  }

  static async create(config: WorkerConfig): Promise<WorkerService> {
    WorkerService.connection = config.connection;
    const workflow = config.workflow;
    const [workflowFunctionName, workflowFunction] =
      WorkerService.resolveWorkflowTarget(workflow);
    const baseTopic = `${config.taskQueue}-${workflowFunctionName}`;
    const activityTopic = `${baseTopic}-activity`;
    const workflowTopic = `${baseTopic}`;

    //initialize supporting workflows
    const worker = new WorkerService();
    worker.activityRunner = await worker.initActivityWorker(
      config,
      activityTopic,
    );
    worker.workflowRunner = await worker.initWorkflowWorker(
      config,
      workflowTopic,
      workflowFunction,
    );
    Search.configureSearchIndex(worker.workflowRunner, config.search);
    await WorkerService.activateWorkflow(worker.workflowRunner);
    return worker;
  }

  static resolveWorkflowTarget(
    workflow: object | Function,
    name?: string,
  ): [string, Function] {
    let workflowFunction: Function;
    if (typeof workflow === 'function') {
      workflowFunction = workflow;
      return [workflowFunction.name ?? name, workflowFunction];
    } else {
      const workflowFunctionNames = Object.keys(workflow);
      const lastFunctionName =
        workflowFunctionNames[workflowFunctionNames.length - 1];
      workflowFunction = workflow[lastFunctionName];
      return WorkerService.resolveWorkflowTarget(
        workflowFunction,
        lastFunctionName,
      );
    }
  }

  async run() {
    this.workflowRunner.engine.logger.info('durable-worker-running');
  }

  async initActivityWorker(
    config: WorkerConfig,
    activityTopic: string,
  ): Promise<HotMesh> {
    const redisConfig = {
      class: config.connection.class as RedisClass,
      options: config.connection.options as RedisOptions,
    };
    const hotMeshWorker = await HotMesh.init({
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: config.namespace ?? APP_ID,
      engine: { redis: redisConfig },
      workers: [
        {
          topic: activityTopic,
          redis: redisConfig,
          callback: this.wrapActivityFunctions().bind(this),
        },
      ],
    });
    WorkerService.instances.set(activityTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  //this is the linked worker function in the reentrant workflow test
  wrapActivityFunctions(): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      try {
        //always run the activity function when instructed; return the response
        const activityInput = data.data as unknown as ActivityWorkflowDataType;
        const activityName = activityInput.activityName;
        const activityFunction = WorkerService.activityRegistry[activityName];
        const pojoResponse = await activityFunction.apply(
          this,
          activityInput.arguments,
        );

        return {
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: pojoResponse },
        };
      } catch (err) {
        this.activityRunner.engine.logger.error('durable-worker-activity-err', {
          name: err.name,
          message: err.message,
          stack: err.stack,
        });
        if (
          !(err instanceof DurableTimeoutError) &&
          !(err instanceof DurableMaxedError) &&
          !(err instanceof DurableFatalError)
        ) {
          //use code 599 as a proxy for all retryable errors
          // (basically anything not 596, 597, 598)
          return {
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_DURABLE_RETRYABLE,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                stack: err.stack,
                timestamp: formatISODate(new Date()),
              },
            },
          } as StreamDataResponse;
        }

        return {
          //always returrn success (the Durable module is just fine);
          //  it's the user's function that has failed
          status: StreamStatus.SUCCESS,
          code: err.code,
          stack: err.stack,
          metadata: { ...data.metadata },
          data: {
            $error: {
              message: err.message,
              stack: err.stack,
              timestamp: formatISODate(new Date()),
              code: err.code,
            },
          },
        } as StreamDataResponse;
      }
    };
  }

  async initWorkflowWorker(
    config: WorkerConfig,
    workflowTopic: string,
    workflowFunction: Function,
  ): Promise<HotMesh> {
    const redisConfig = {
      class: config.connection.class as RedisClass,
      options: config.connection.options as RedisOptions,
    };
    const hotMeshWorker = await HotMesh.init({
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: config.namespace ?? APP_ID,
      engine: { redis: redisConfig },
      workers: [
        {
          topic: workflowTopic,
          redis: redisConfig,
          callback: this.wrapWorkflowFunction(
            workflowFunction,
            workflowTopic,
            config,
          ).bind(this),
        },
      ],
    });
    WorkerService.instances.set(workflowTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  static Context = {
    info: () => {
      return {
        workflowId: '',
        workflowTopic: '',
      };
    },
  };

  wrapWorkflowFunction(
    workflowFunction: Function,
    workflowTopic: string,
    config: WorkerConfig,
  ): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      const counter = { counter: 0 };
      const interruptionRegistry: any[] = [];
      let isProcessing = false;
      try {
        //incoming data payload has arguments and workflowId
        const workflowInput = data.data as unknown as WorkflowDataType;
        const context = new Map();
        context.set('canRetry', workflowInput.canRetry);
        context.set('counter', counter);
        context.set('interruptionRegistry', interruptionRegistry);
        context.set('namespace', config.namespace ?? APP_ID);
        context.set('raw', data);
        context.set('workflowId', workflowInput.workflowId);
        if (workflowInput.originJobId) {
          //if present there is an origin job to which this job is subordinated;
          // garbage collect (expire) this job when originJobId is expired
          context.set('originJobId', workflowInput.originJobId);
        }
        let replayQuery = '';
        if (workflowInput.workflowDimension) {
          //every hook function runs in an isolated dimension controlled
          //by the index assigned when the signal was received; even if the
          //hook function re-runs, its scope will always remain constant
          context.set('workflowDimension', workflowInput.workflowDimension);
          replayQuery = `-*${workflowInput.workflowDimension}-*`;
        } else {
          //last letter of words like 'hook', 'sleep', 'wait', 'signal', 'search', 'start', 'proxy', 'child', 'collator'
          replayQuery = '-*[ehklptydr]-*';
        }
        context.set('workflowTopic', workflowTopic);
        context.set('workflowName', workflowTopic.split('-').pop());
        context.set('workflowTrace', data.metadata.trc);
        context.set('workflowSpan', data.metadata.spn);
        const store = this.workflowRunner.engine.store;
        const [cursor, replay] = await store.findJobFields(
          workflowInput.workflowId,
          replayQuery,
          50_000,
          5_000,
        );
        context.set('replay', replay);
        context.set('cursor', cursor); // if != 0, more remain
        const workflowResponse = await asyncLocalStorage.run(
          context,
          async () => {
            return await workflowFunction.apply(this, workflowInput.arguments);
          },
        );

        return {
          code: 200,
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: workflowResponse, done: true },
        };
      } catch (err) {
        if (isProcessing) {
          return;
        }
        if (
          err instanceof DurableWaitForError ||
          interruptionRegistry.length > 1
        ) {
          isProcessing = true;

          //NOTE: this type is spawned when `Promise.all` is used OR if the interruption is a `waitFor`
          const workflowInput = data.data as unknown as WorkflowDataType;
          const execIndex = counter.counter - interruptionRegistry.length + 1;
          const { workflowId, workflowTopic, workflowDimension, originJobId } =
            workflowInput;
          const collatorFlowId = `${guid()}$C`;
          return {
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_DURABLE_ALL,
            metadata: { ...data.metadata },
            data: {
              code: HMSH_CODE_DURABLE_ALL,
              items: [...interruptionRegistry],
              size: interruptionRegistry.length,
              workflowDimension: workflowDimension || '',
              index: execIndex,
              originJobId: originJobId || workflowId,
              parentWorkflowId: workflowId,
              workflowId: collatorFlowId,
              workflowTopic: workflowTopic,
            },
          } as StreamDataResponse;
        } else if (err instanceof DurableSleepError) {
          //return the sleep interruption
          isProcessing = true;
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({
                duration: err.duration,
                index: err.index,
                workflowDimension: err.workflowDimension,
              }),
              duration: err.duration,
              index: err.index,
              workflowDimension: err.workflowDimension,
            },
          } as StreamDataResponse;
        } else if (err instanceof DurableProxyError) {
          //return the proxyActivity interruption
          isProcessing = true;
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({
                message: err.message,
                workflowId: err.workflowId,
                activityName: err.activityName,
                dimension: err.workflowDimension,
              }),
              arguments: err.arguments,
              workflowDimension: err.workflowDimension,
              index: err.index,
              originJobId: err.originJobId,
              parentWorkflowId: err.parentWorkflowId,
              workflowId: err.workflowId,
              workflowTopic: err.workflowTopic,
              activityName: err.activityName,
              backoffCoefficient: err.backoffCoefficient,
              maximumAttempts: err.maximumAttempts,
              maximumInterval: err.maximumInterval,
            },
          } as StreamDataResponse;
        } else if (err instanceof DurableChildError) {
          //return the child interruption
          isProcessing = true;
          const msg = {
            message: err.message,
            workflowId: err.workflowId,
            dimension: err.workflowDimension,
          };
          return {
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              arguments: err.arguments,
              await: err.await,
              backoffCoefficient:
                err.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
              code: err.code,
              index: err.index,
              message: JSON.stringify(msg),
              maximumAttempts: err.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
              maximumInterval:
                err.maximumInterval || ms(HMSH_DURABLE_MAX_INTERVAL) / 1000,
              originJobId: err.originJobId,
              parentWorkflowId: err.parentWorkflowId,
              workflowDimension: err.workflowDimension,
              workflowId: err.workflowId,
              workflowTopic: err.workflowTopic,
            },
          } as StreamDataResponse;
        }

        // ALL other errors are actual fatal errors (598, 597, 596)
        //  OR will be retried (599)
        isProcessing = true;
        return {
          status: StreamStatus.SUCCESS,
          code: err.code || new DurableRetryError(err.message).code,
          metadata: { ...data.metadata },
          data: {
            $error: {
              message: err.message,
              type: err.name,
              name: err.name,
              stack: err.stack,
              code: err.code || new DurableRetryError(err.message).code,
            },
          },
        } as StreamDataResponse;
      }
    };
  }

  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of WorkerService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
