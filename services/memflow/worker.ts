import {
  HMSH_CODE_MEMFLOW_ALL,
  HMSH_CODE_MEMFLOW_RETRYABLE,
  HMSH_MEMFLOW_EXP_BACKOFF,
  HMSH_MEMFLOW_MAX_INTERVAL,
  HMSH_MEMFLOW_MAX_ATTEMPTS,
  HMSH_LOGLEVEL,
} from '../../modules/enums';
import {
  MemFlowChildError,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowProxyError,
  MemFlowRetryError,
  MemFlowSleepError,
  MemFlowTimeoutError,
  MemFlowWaitForError,
} from '../../modules/errors';
import { asyncLocalStorage } from '../../modules/storage';
import { formatISODate, guid, hashOptions, s } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import {
  ActivityWorkflowDataType,
  Connection,
  Registry,
  WorkerConfig,
  WorkerOptions,
  WorkflowDataType,
} from '../../types/memflow';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../types/stream';

import { Search } from './search';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './schemas/factory';

/**
 * The *Worker* service Registers worker functions and connects them to the mesh,
 * using the target backend provider/s (Redis, Postgres, NATS, etc).
 *
 * @example
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 * import * as workflows from './workflows';
 *
 * async function run() {
 *   const worker = await MemFlow.Worker.create({
 *     connection: {
 *       class: Postgres,
 *       options: { connectionString: 'postgres://user:password@localhost:5432/db' }
 *     },
 *     taskQueue: 'default',
 *     workflow: workflows.example,
 *   });
 *
 *   await worker.run();
 * }
 * ```
 */
export class WorkerService {
  /**
   * @private
   */
  static activityRegistry: Registry = {}; //user's activities
  /**
   * @private
   */
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();
  /**
   * @private
   */
  workflowRunner: HotMesh;
  /**
   * @private
   */
  activityRunner: HotMesh;

  /**
   * @private
   */
  static getHotMesh = async (
    workflowTopic: string,
    config?: Partial<WorkerConfig>,
    options?: WorkerOptions,
  ) => {
    const targetNamespace = config?.namespace ?? APP_ID;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${workflowTopic}`;

    if (WorkerService.instances.has(targetTopic)) {
      return await WorkerService.instances.get(targetTopic);
    }
    const hotMeshClient = HotMesh.init({
      logLevel: options?.logLevel ?? HMSH_LOGLEVEL,
      appId: targetNamespace,
      engine: {
        connection: { ...config?.connection },
      },
    });
    WorkerService.instances.set(targetTopic, hotMeshClient);
    await WorkerService.activateWorkflow(await hotMeshClient);
    return hotMeshClient;
  };

  static hashOptions(connection: Connection): string {
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
   * @private
   */
  constructor() {}

  /**
   * @private
   */
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
        hotMesh.engine.logger.error('memflow-worker-deploy-activate-err', err);
        throw err;
      }
    } else if (app && !app.active) {
      try {
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('memflow-worker-activate-err', err);
        throw err;
      }
    }
  }

  /**
   * @private
   */
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

  /**
   * Connects a worker to the mesh.
   *
   * @example
   * ```typescript
   * import { MemFlow } from '@hotmeshio/hotmesh';
   * import { Client as Postgres } from 'pg';
   * import * as workflows from './workflows';
   *
   * async function run() {
   *   const worker = await MemFlow.Worker.create({
   *     connection: {
   *       class: Postgres,
   *       options: {
   *         connectionString: 'postgres://user:password@localhost:5432/db'
   *       },
   *     },
   *     taskQueue: 'default',
   *     workflow: workflows.example,
   *   });
   *
   *   await worker.run();
   * }
   * ```
   */
  static async create(config: WorkerConfig): Promise<WorkerService> {
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

  /**
   * @private
   */
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

  /**
   * Run the connected worker; no-op (unnecessary to call)
   */
  async run() {
    this.workflowRunner.engine.logger.info('memflow-worker-running');
  }

  /**
   * @private
   */
  async initActivityWorker(
    config: WorkerConfig,
    activityTopic: string,
  ): Promise<HotMesh> {
    const providerConfig = config.connection;
    const targetNamespace = config?.namespace ?? APP_ID;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${activityTopic}`;
    const hotMeshWorker = await HotMesh.init({
      guid: config.guid ? `${config.guid}XA` : undefined,
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: targetNamespace,
      engine: { connection: providerConfig },
      workers: [
        {
          topic: activityTopic,
          connection: providerConfig,
          callback: this.wrapActivityFunctions().bind(this),
        },
      ],
    });
    WorkerService.instances.set(targetTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  /**
   * @private
   */
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
        this.activityRunner.engine.logger.error(
          'memflow-worker-activity-err',
          {
            name: err.name,
            message: err.message,
            stack: err.stack,
          },
        );
        if (
          !(err instanceof MemFlowTimeoutError) &&
          !(err instanceof MemFlowMaxedError) &&
          !(err instanceof MemFlowFatalError)
        ) {
          //use code 599 as a proxy for all retryable errors
          // (basically anything not 596, 597, 598)
          return {
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_MEMFLOW_RETRYABLE,
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
          //always returrn success (the MemFlow module is just fine);
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

  /**
   * @private
   */
  async initWorkflowWorker(
    config: WorkerConfig,
    workflowTopic: string,
    workflowFunction: Function,
  ): Promise<HotMesh> {
    const providerConfig = config.connection;
    const targetNamespace = config?.namespace ?? APP_ID;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${workflowTopic}`;
    const hotMeshWorker = await HotMesh.init({
      guid: config.guid,
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: config.namespace ?? APP_ID,
      engine: { connection: providerConfig },
      workers: [
        {
          topic: workflowTopic,
          connection: providerConfig,
          callback: this.wrapWorkflowFunction(
            workflowFunction,
            workflowTopic,
            config,
          ).bind(this),
        },
      ],
    });
    WorkerService.instances.set(targetTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  /**
   * @private
   */
  static Context = {
    info: () => {
      return {
        workflowId: '',
        workflowTopic: '',
      };
    },
  };

  /**
   * @private
   */
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
        context.set('expire', workflowInput.expire);
        context.set('counter', counter);
        context.set('interruptionRegistry', interruptionRegistry);
        context.set('connection', config.connection);
        context.set('namespace', config.namespace ?? APP_ID);
        context.set('raw', data);
        context.set('workflowId', workflowInput.workflowId);
        if (workflowInput.originJobId) {
          //if present there is an origin job to which this job is subordinated;
          // garbage collect (expire) this job when originJobId is expired
          context.set('originJobId', workflowInput.originJobId);
        }
        //TODO: the query is provider-specific;
        //      refactor as an abstract interface the provider must implement
        let replayQuery = '';
        if (workflowInput.workflowDimension) {
          //every hook function runs in an isolated dimension controlled
          //by the index assigned when the signal was received; even if the
          //hook function re-runs, its scope will always remain constant
          context.set('workflowDimension', workflowInput.workflowDimension);
          replayQuery = `-*${workflowInput.workflowDimension}-*`;
        } else {
          //last letter of words like 'hook', 'sleep', 'wait', 'signal', 'search', 'start', 'proxy', 'child', 'collator', 'trace', 'enrich', 'publish'
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
          err instanceof MemFlowWaitForError ||
          interruptionRegistry.length > 1
        ) {
          isProcessing = true;

          //NOTE: this type is spawned when `Promise.all` is used OR if the interruption is a `waitFor`
          const workflowInput = data.data as unknown as WorkflowDataType;
          const execIndex = counter.counter - interruptionRegistry.length + 1;
          const {
            workflowId,
            workflowTopic,
            workflowDimension,
            originJobId,
            expire,
          } = workflowInput;
          const collatorFlowId = `${guid()}$C`;
          return {
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_MEMFLOW_ALL,
            metadata: { ...data.metadata },
            data: {
              code: HMSH_CODE_MEMFLOW_ALL,
              items: [...interruptionRegistry],
              size: interruptionRegistry.length,
              workflowDimension: workflowDimension || '',
              index: execIndex,
              originJobId: originJobId || workflowId,
              parentWorkflowId: workflowId,
              workflowId: collatorFlowId,
              workflowTopic: workflowTopic,
              expire,
            },
          } as StreamDataResponse;
        } else if (err instanceof MemFlowSleepError) {
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
        } else if (err instanceof MemFlowProxyError) {
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
              expire: err.expire,
              workflowId: err.workflowId,
              workflowTopic: err.workflowTopic,
              activityName: err.activityName,
              backoffCoefficient: err.backoffCoefficient,
              maximumAttempts: err.maximumAttempts,
              maximumInterval: err.maximumInterval,
            },
          } as StreamDataResponse;
        } else if (err instanceof MemFlowChildError) {
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
                err.backoffCoefficient || HMSH_MEMFLOW_EXP_BACKOFF,
              code: err.code,
              index: err.index,
              message: JSON.stringify(msg),
              maximumAttempts:
                err.maximumAttempts || HMSH_MEMFLOW_MAX_ATTEMPTS,
              maximumInterval:
                err.maximumInterval || s(HMSH_MEMFLOW_MAX_INTERVAL),
              originJobId: err.originJobId,
              entity: err.entity,
              parentWorkflowId: err.parentWorkflowId,
              expire: err.expire,
              persistent: err.persistent,
              signalIn: err.signalIn,
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
          code: err.code || new MemFlowRetryError(err.message).code,
          metadata: { ...data.metadata },
          data: {
            $error: {
              message: err.message,
              type: err.name,
              name: err.name,
              stack: err.stack,
              code: err.code || new MemFlowRetryError(err.message).code,
            },
          },
        } as StreamDataResponse;
      }
    };
  }

  /**
   * @private
   */
  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of WorkerService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
