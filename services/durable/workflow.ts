import ms from 'ms';

import {
  DurableChildError,
  DurableFatalError,
  DurableMaxedError,
  DurableProxyError,
  DurableSleepError,
  DurableTimeoutError,
  DurableWaitForError } from '../../modules/errors';
import { KeyService, KeyType } from '../../modules/key';
import { asyncLocalStorage } from '../../modules/storage';
import {
  deterministicRandom,
  formatISODate,
  sleepFor } from '../../modules/utils';
import { Search } from './search';
import { WorkerService } from './worker';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ActivityConfig,
  HookOptions,
  ProxyType,
  WorkflowContext,
  WorkflowOptions
} from "../../types/durable";
import { JobInterruptOptions } from '../../types/job';
import { StreamCode, StreamError, StreamStatus } from '../../types/stream';
import { StringStringType } from '../../types';
import {
  HMSH_CODE_DURABLE_CHILD,
  HMSH_CODE_DURABLE_FATAL,
  HMSH_CODE_DURABLE_MAXED,
  HMSH_CODE_DURABLE_PROXY,
  HMSH_CODE_DURABLE_SLEEP,
  HMSH_CODE_DURABLE_TIMEOUT,
  HMSH_CODE_DURABLE_WAIT, 
  HMSH_DURABLE_EXP_BACKOFF, 
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_DURABLE_MAX_INTERVAL} from '../../modules/enums';
import { SerializerService } from '../serializer';

export class WorkflowService {

  /**
   * Spawns a child workflow and awaits the return.
   * @template T - the result type
   * @param {WorkflowOptions} options - the workflow options
   * @returns {Promise<T>} - the result of the child workflow
   * @example
   * const result = await Durable.workflow.execChild<typeof resultType>({ ...options });
   */
  static async execChild<T>(options: WorkflowOptions): Promise<T> {
    //SYNC
    //check if the activity already ran
    const isStartChild = options.await === false;
    const [didRun, execIndex, result] = await WorkflowService.didRun(isStartChild ? 'start' : 'child');
    if (didRun) {
      //data is the job id if isStartChild is true, otherwise it is the result
      return result?.data as T;
    }
    //package the interruption inputs
    const store = asyncLocalStorage.getStore();
    const interruptionRegistry = store.get('interruptionRegistry');
    const workflowId = store.get('workflowId');
    const originJobId = store.get('originJobId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const entityOrEmptyString = options.entity ?? '';
    const childJobId = options.workflowId ?? `${entityOrEmptyString}-${workflowId}-$${options.entity ?? options.workflowName}${workflowDimension}-${execIndex}`;
    const parentWorkflowId = workflowId;
    const taskQueueName = options.entity ?? options.taskQueue;
    const workflowName = options.entity ?? options.workflowName;
    const workflowTopic = `${taskQueueName}-${workflowName}`;
    const interruptionMessage = {
      arguments: [...(options.args || [])],
      await: options?.await ?? true,
      backoffCoefficient: options?.config?.backoffCoefficient ?? HMSH_DURABLE_EXP_BACKOFF,
      index: execIndex,
      maximumAttempts: options?.config?.maximumAttempts ?? HMSH_DURABLE_MAX_ATTEMPTS,
      maximumInterval: ms(options?.config?.maximumInterval ?? HMSH_DURABLE_MAX_INTERVAL) / 1000,
      originJobId: originJobId ?? workflowId,
      parentWorkflowId,
      workflowDimension: workflowDimension,
      workflowId: childJobId,
      workflowTopic,
    };
    //push the packaged inputs to the registry
    interruptionRegistry.push({
      code: HMSH_CODE_DURABLE_CHILD,
      ...interruptionMessage,
    });
    //ASYNC
    //sleep (allow others to be packaged / registered) and throw the error
    await sleepFor(0);
    throw new DurableChildError(interruptionMessage);
  }

  /**
   * Spawns a child workflow and returns the child Job ID.
   * This method guarantees the spawned child has reserved the Job ID,
   * returning a 'DuplicateJobError' error if not. Otherwise,
   * this is a fire-and-forget method.
   * 
   * @param {WorkflowOptions} options - the workflow options
   * @returns {Promise<string>} - the childJobId
   * @example
   * const childJobId = await Durable.workflow.startChild({ ...options });
   */
  static async startChild(options: WorkflowOptions): Promise<string> {
    return this.execChild({ ...options, await: false });
  }

  /**
   * Wraps activities in a proxy that durably runs/re-runs them to completion.
   * 
   * @param {ActivityConfig} options - the activity configuration
   * that will be used to wrap the activities.
   * @returns {ProxyType<ACT>} - a proxy object with the same keys as the
   * activities object, but with the values replaced by a wrapped function
   * 
   * @example
   * // import the activities
   * import * as activities from './activities';
   * const proxy = WorkflowService.proxyActivities<typeof activities>({ activities });
   * 
   * //or destructure the proxy object, as the function names are the keys
   * const { activity1, activity2 } = WorkflowService.proxyActivities<typeof activities>({ activities });
   */
  static proxyActivities<ACT>(options?: ActivityConfig): ProxyType<ACT> {
    if (options.activities) {
      WorkerService.registerActivities(options.activities);
    }
    const proxy: any = {};
    const keys = Object.keys(WorkerService.activityRegistry);
    if (keys.length) {
      keys.forEach((key: string) => {
        const activityFunction = WorkerService.activityRegistry[key];
        proxy[key] = WorkflowService.wrapActivity<typeof activityFunction>(key, options);
      });
    }
    return proxy;
  }

  /**
   * Returns a search session for use when reading/writing to the workflow HASH.
   * The search session provides access to methods like `get`, `mget`, `set`, `del`, and `incr`.
   * @returns {Promise<Search>} - a search session
   */
  static async search(): Promise<Search> {
    const store = asyncLocalStorage.getStore();
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    //this ID is used as a item key with a hash (dash prefix ensures no collision)
    const searchSessionId = `-search${workflowDimension}-${execIndex}`;
    return new Search(workflowId, hotMeshClient, searchSessionId);
  }

  /**
   * Return a handle to the hotmesh client currently running the workflow
   * @returns {Promise<HotMesh>} - a hotmesh client
   */
  static async getHotMesh(): Promise<HotMesh> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    return await WorkerService.getHotMesh(workflowTopic, { namespace });
  }

  /**
   * Returns the current workflow context restored
   * from Redis
   */
  static getContext(): WorkflowContext {
    const store = asyncLocalStorage.getStore();
    const workflowId = store.get('workflowId');
    const replay = store.get('replay');
    const cursor = store.get('cursor');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    const COUNTER = store.get('counter');
    const raw = store.get('raw');
    return {
      COUNTER,
      counter: COUNTER.counter,
      cursor,
      namespace,
      raw,
      replay,
      workflowId,
      workflowDimension,
      workflowTopic,
      workflowTrace,
      workflowSpan,
    };
  }

  /**
   * Returns the synchronous output from the activity (replay)
   * if available locally
   * @param {string} prefix - one of: proxy, child, start, wait etc
   * @returns 
   */
  static async didRun(prefix: string): Promise<[boolean, number, any?]> {
    const {
      COUNTER,
      replay,
      workflowDimension,
    } = WorkflowService.getContext();
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
    if (sessionId in replay) {
      return [true, execIndex, SerializerService.fromString(replay[sessionId])];
    }
    return [false, execIndex];
  }

  /**
   * Those methods that may only be called once must be protected by flagging
   * their execution with a unique key (the key is stored in the HASH alongside
   * process state and job state)
   * @private
   */
  static async isSideEffectAllowed(hotMeshClient: HotMesh, prefix: string): Promise<boolean> {
    const store = asyncLocalStorage.getStore();
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
    const replay = store.get('replay') as StringStringType;
    if (sessionId in replay) {
      return false;
    }
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: workflowId
    }
    const workflowGuid = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
    const guidValue = Number(await hotMeshClient.engine.store.exec('HINCRBYFLOAT', workflowGuid, sessionId, '1') as string);
    return guidValue === 1;
  }

  /**
   * Returns a random number between 0 and 1. This number is deterministic
   * and will never vary for a given seed. This is useful for randomizing
   * pathways in a workflow that can be safely replayed.
   * @returns {number} - a random number between 0 and 1
   */
  static random(): number {
    const store = asyncLocalStorage.getStore();
    const COUNTER = store.get('counter');
    const seed = COUNTER.counter = COUNTER.counter + 1;
    return deterministicRandom(seed);
  }

  /**
   * Sends signal data into any other paused thread (which is currently
   * awaiting the signal)
   * @param {string} signalId - the signal id
   * @param {Record<any, any>} data - the signal data
   * @returns {Promise<string>} - the stream id
   */
  static async signal(signalId: string, data: Record<any, any>): Promise<string> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'signal')) {
      return await hotMeshClient.hook(`${namespace}.wfs.signal`, { id: signalId, data });
    }
  }

  /**
   * Spawns a hook from either the main thread or a hook thread with
   * the provided options; worflowId/TaskQueue/Name are optional and will
   * default to the current workflowId/WorkflowTopic if not provided
   * @param {HookOptions} options - the hook options
   */
  static async hook(options: HookOptions): Promise<string> {
    const {
      workflowId,
      namespace,
      workflowTopic,
    } = WorkflowService.getContext();
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'hook')) {
      const targetWorkflowId = options.workflowId ?? workflowId;
      let targetTopic: string;
      if (options.entity || (options.taskQueue && options.workflowName)) {
        targetTopic = `${options.entity ?? options.taskQueue}-${options.entity ?? options.workflowName}`;
      } else {
        targetTopic = workflowTopic;
      }
      const payload = {
        arguments: [...options.args],
        id: targetWorkflowId,
        workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
      }
      return await hotMeshClient.hook(`${namespace}.flow.signal`, payload, StreamStatus.PENDING, 202);
    }
  }

  /**
   * Executes a function once and caches the result. If the function is called
   * again, the cached result is returned. This is useful for wrapping
   * expensive activity calls that should only be run once, but which might
   * not require the cost and safety provided by proxyActivities.
   * @template T - the result type
   */
  static async once<T>(fn: (...args: any[]) => Promise<T>, ...args: any[]): Promise<T> {
    const {
      COUNTER,
      namespace,
      workflowId,
      workflowTopic,
      workflowDimension,
      replay,
    } = WorkflowService.getContext();
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const sessionId = `-once${workflowDimension}-${execIndex}-`;
    if (sessionId in replay) {
      return SerializerService.fromString(replay[sessionId]).data as T;
    }
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: workflowId
    }
    const workflowGuid = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
    const t1 = new Date();
    const response = await fn(...args);
    const t2 = new Date();
    const payload = {
      data: response,
      ac: formatISODate(t1),
      au: formatISODate(t2),
    };
    await hotMeshClient.engine.store.exec('HSET', workflowGuid, sessionId, SerializerService.toString(payload));
    return response;
  }

  /** 
   * Interrupts a running job
   */
  static async interrupt(jobId: string, options: JobInterruptOptions = {}): Promise<string | void> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'interrupt')) {
      return await hotMeshClient.interrupt(`${hotMeshClient.appId}.execute`, jobId, options);
    }
  }

  /**
   * Sleeps the workflow for a duration. As the function is reentrant, 
   * upon reentry, the function will traverse prior execution paths up
   * until the sleep command and then resume execution thereafter.
   * @param {string} duration - See the `ms` package for syntax examples: '1 minute', '2 hours', '3 days'
   * @returns {Promise<number>}
   */
  static async sleepFor(duration: string): Promise<number> {
    //SYNC
    //return early if this sleep command has already run
    const [didRun, execIndex, result] = await WorkflowService.didRun('sleep');
    if (didRun) {
      return (result as { completion: string, duration: number }).duration; //in seconds
    }
    //package the interruption inputs
    const store = asyncLocalStorage.getStore();
    const interruptionRegistry = store.get('interruptionRegistry');
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const interruptionMessage = {
      workflowId,
      duration: ms(duration) / 1000,
      index: execIndex,
      workflowDimension,
    }
    interruptionRegistry.push({
      code: HMSH_CODE_DURABLE_SLEEP,
      ...interruptionMessage,
    });
    //ASYNC
    //sleep to allow other interruptions to be packaged and registered
    await sleepFor(0);
    // NOTE: If you are reading this in the stack trace, await `sleepFor`
    throw new DurableSleepError(interruptionMessage);
  }

  /**
   * Pauses the workflow until `signalId` is received.
   * @template T - the result type
   * @param {string} signalId - a unique, shareable guid (e.g, 'abc123')
   * @returns {Promise<T>}
   * @example
   * const result = await Durable.workflow.waitFor<typeof resultType>('abc123');
   */
  static async waitFor<T>(signalId: string): Promise<T> {
    //SYNC
    //return early if this waitFor command has already run
    const [didRun, execIndex, result] = await WorkflowService.didRun('wait');
    if (didRun) {
      return (result as { id: string, data: { data: T }}).data.data as T;
    }
    //package the interruption inputs
    const store = asyncLocalStorage.getStore();
    const interruptionRegistry = store.get('interruptionRegistry');
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const interruptionMessage = {
      workflowId,
      signalId,
      index: execIndex,
      workflowDimension,
    }
    interruptionRegistry.push({
      code: HMSH_CODE_DURABLE_WAIT,
      ...interruptionMessage,
    });
    //ASYNC
    //sleep to allow other interruptions to be packaged and registered
    await sleepFor(0);
    // NOTE: If you are reading this in the stack trace, await `waitFor`
    throw new DurableWaitForError(interruptionMessage);
  }

  static wrapActivity<T>(activityName: string, options?: ActivityConfig): T {
    return async function () {
      //SYNC
      //check if the activity already ran
      type ProxyType = { data?: T, timestamp: string, $error?: StreamError};
      const [didRun, execIndex, result] = await WorkflowService.didRun('proxy');
      if (didRun) {
        if ((result as ProxyType)?.$error) {
          if (options?.retryPolicy?.throwOnError !== false) {
            //rethrow remote execution error (simulates throw)
            const code: StreamCode = result.$error.code;
            const message = result.$error.message;
            const stack = result.$error.stack;
            if (code === HMSH_CODE_DURABLE_FATAL) {
              throw new DurableFatalError(message, stack);
            } else if (code == HMSH_CODE_DURABLE_MAXED) {
              throw new DurableMaxedError(message, stack);
            } else if (code == HMSH_CODE_DURABLE_TIMEOUT) {
              throw new DurableTimeoutError(message, stack);
            }
          }
          return (result as ProxyType).$error as T;
        }
        return (result as ProxyType).data as T;
      }
      //package the interruption inputs
      const store = asyncLocalStorage.getStore();
      const interruptionRegistry = store.get('interruptionRegistry');
      const workflowDimension = store.get('workflowDimension') ?? '';
      const workflowId = store.get('workflowId');
      const originJobId = store.get('originJobId');
      const workflowTopic = store.get('workflowTopic');
      const activityTopic = `${workflowTopic}-activity`;
      const activityJobId = `-${workflowId}-$${activityName}${workflowDimension}-${execIndex}`;
      let maximumInterval: number;
      if (options.retryPolicy?.maximumInterval) {
        maximumInterval = ms(options.retryPolicy.maximumInterval) / 1000;
      }
      const interruptionMessage = {
        arguments: Array.from(arguments),
        workflowDimension: workflowDimension,
        index: execIndex,
        originJobId: originJobId || workflowId,
        parentWorkflowId: workflowId,
        workflowId: activityJobId,
        workflowTopic: activityTopic,
        activityName,
        backoffCoefficient: options?.retryPolicy?.backoffCoefficient ?? undefined,
        maximumAttempts: options?.retryPolicy?.maximumAttempts ?? undefined,
        maximumInterval: maximumInterval ?? undefined,
      };
      //push the packaged inputs to the registry
      interruptionRegistry.push({
        code: HMSH_CODE_DURABLE_PROXY,
        ...interruptionMessage,
      });
      //ASYNC
      //sleep (allow others to be packaged / registered) and throw the error
      await sleepFor(0);
      throw new DurableProxyError(interruptionMessage);
    } as T;
  }
}
