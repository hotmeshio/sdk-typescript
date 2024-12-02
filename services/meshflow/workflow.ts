import {
  MeshFlowChildError,
  MeshFlowFatalError,
  MeshFlowMaxedError,
  MeshFlowProxyError,
  MeshFlowRetryError,
  MeshFlowSleepError,
  MeshFlowTimeoutError,
  MeshFlowWaitForError,
} from '../../modules/errors';
import { KeyService, KeyType } from '../../modules/key';
import { asyncLocalStorage } from '../../modules/storage';
import {
  deterministicRandom,
  guid,
  s,
  sleepImmediate,
} from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import { SerializerService } from '../serializer';
import {
  ActivityConfig,
  ChildResponseType,
  HookOptions,
  ProxyResponseType,
  ProxyType,
  WorkflowContext,
  WorkflowOptions,
} from '../../types/meshflow';
import { JobInterruptOptions } from '../../types/job';
import { StreamCode, StreamStatus } from '../../types/stream';
import {
  StringAnyType,
  StringScalarType,
  StringStringType,
} from '../../types/serializer';
import {
  HMSH_CODE_MESHFLOW_CHILD,
  HMSH_CODE_MESHFLOW_FATAL,
  HMSH_CODE_MESHFLOW_MAXED,
  HMSH_CODE_MESHFLOW_PROXY,
  HMSH_CODE_MESHFLOW_SLEEP,
  HMSH_CODE_MESHFLOW_TIMEOUT,
  HMSH_CODE_MESHFLOW_WAIT,
  HMSH_MESHFLOW_EXP_BACKOFF,
  HMSH_MESHFLOW_MAX_ATTEMPTS,
  HMSH_MESHFLOW_MAX_INTERVAL,
} from '../../modules/enums';
import {
  MeshFlowChildErrorType,
  MeshFlowProxyErrorType,
} from '../../types/error';
import { TelemetryService } from '../telemetry';
import { QuorumMessage } from '../../types';
import { UserMessage } from '../../types/quorum';

import { Search } from './search';
import { WorkerService } from './worker';

/**
 * The workflow module provides a set of static extension methods
 * that can be called from within a workflow function. In this example,
 * the `waitFor` extension method is called to add collation to the
 * workflow, only continuing once both outside signals have been received.
 * 
 * @example
 * ```typescript
 * //waitForWorkflow.ts
 * import { MeshFlow } from '@hotmeshio/hotmesh';

 * export async function waitForExample(): Promise<[boolean, number]> {
 *   const [s1, s2] = await Promise.all([
 *     Meshflow.workflow.waitFor<boolean>('my-sig-nal-1'),
 *     Meshflow.workflow.waitFor<number>('my-sig-nal-2')
 *   ]);
 *   //do something with the signal payloads (s1, s2)
 *   return [s1, s2];
 * }
 * ```
 */
export class WorkflowService {
  /**
   * @private
   */
  constructor() {}

  /**
   * Returns the synchronous output from the activity (replay)
   * if available locally, revealing whether or not the activity already
   * ran during a prior execution cycle
   * @param {string} prefix - one of: proxy, child, start, wait etc
   * @private
   */
  static async didRun(prefix: string): Promise<[boolean, number, any]> {
    const { COUNTER, replay, workflowDimension } = WorkflowService.getContext();
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
    if (sessionId in replay) {
      const restored = SerializerService.fromString(replay[sessionId]);
      return [true, execIndex, restored];
    }
    return [false, execIndex, null];
  }

  /**
   * Those methods that may only be called once must be protected by flagging
   * their execution with a unique key (the key is stored in the HASH alongside
   * process state and job state)
   * @private
   */
  static async isSideEffectAllowed(
    hotMeshClient: HotMesh,
    prefix: string,
  ): Promise<boolean> {
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
      jobId: workflowId,
    };
    const workflowGuid = KeyService.mintKey(
      hotMeshClient.namespace,
      KeyType.JOB_STATE,
      keyParams,
    );
    const searchClient = hotMeshClient.engine.search;
    const guidValue = await searchClient.incrementFieldByFloat(
      workflowGuid,
      sessionId,
      1,
    );
    return guidValue === 1;
  }

  /**
   * Executes a trace, outputting the provided attributes to
   * the Open Telemetry sink. Executes exactly once during
   * workflow execution.
   *
   * It is safe to add this method into any actively running
   * workflow function as long as the trace call would be
   * the last replayable method (by execution order) in
   * the function.
   *
   * @example
   * ```typescript
   * import { workflow } from '@hotmeshio/hotmesh';
   *
   * export async function traceExample(): Promise<boolean> {
   *  const attributes = {
   *   key1: 'value1',
   *   key2: 'value2',
   *  };
   *  return await workflow.trace(attributes);
   * }
   * ```
   */
  static async trace(
    attributes: StringScalarType,
    config: { once: boolean } = { once: true },
  ): Promise<boolean> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
    //NOTE: COUNTER is an object reference, so it is up-to-date when needed
    const { raw, COUNTER } = this.getContext();
    const { trc: traceId, spn: spanId, aid: activityId } = raw.metadata;
    if (
      !config.once ||
      await WorkflowService.isSideEffectAllowed(hotMeshClient, 'trace')
    ) {
      return await TelemetryService.traceActivity(
        namespace,
        attributes,
        activityId,
        traceId,
        spanId,
        COUNTER.counter,
      );
    }
    return true;
  }

  /**
   * Convenience method for adding custom user data to the backend worflow record.
   * Runs exactly once during workflow execution.
   *
   * @example
   * ```typescript
   * import { workflow } from '@hotmeshio/hotmesh';
   *
   * export async function enrichExample(): Promise<boolean> {
   *  const fields = {
   *   key1: 'value1',
   *   key2: 'value2',
   *  };
   *  return await workflow.enrich(fields);
   * }
   * ```
   */
  static async enrich(fields: StringStringType): Promise<boolean> {
    const search = await WorkflowService.search();
    await search.set(fields);
    return true;
  }

  /**
   * Emits an event to the event bus provider (e.g., NATS, Redis, etc.)
   * The topic name will be structured as follows, prefixed
   * with the quorum (q) namespace: `hmsh:<namespace>:q:`.
   *
   * For example, if you provide `my.dog` as the topic, it will
   * be published at `hmsh:myapp:q:my.dog`.
   *
   * If using NATS as the pubsub provider, the delimiter
   * will be `.` instead of `:`.
   *
   * If using Postgres, there is a 63 character limit
   * on topic names, so be mindful of the length of the topic. If your
   * topic exceeds 63 characters, it will be hashed by HotMesh to ensure
   * it isn't truncated by Postgres.
   *
   * @example
   * ```typescript
   * import { workflow } from '@hotmeshio/hotmesh';
   *
   * export async function emitExample(): Promise<boolean> {
   *  const events = {
   *   'my.dog': { 'anything': 'goes' },
   *   'my.dog.cat': { 'anything': 'goes' },
   *  };
   *  return await workflow.emit(events);
   * }
   * ```
   */
  static async emit(
    events: StringAnyType,
    config: { once: boolean } = { once: true },
  ): Promise<boolean> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
    if (
      !config.once ||
      await WorkflowService.isSideEffectAllowed(hotMeshClient, 'emit')
    ) {
      for (const [topic, message] of Object.entries(events)) {
        await hotMeshClient.quorum.pub({ topic, message } as UserMessage);
      }
    }
    return true;
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
    const interruptionRegistry = store.get('interruptionRegistry');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const workflowTopic = store.get('workflowTopic');
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    const originJobId = store.get('originJobId');
    const workflowTrace = store.get('workflowTrace');
    const canRetry = store.get('canRetry');
    const workflowSpan = store.get('workflowSpan');
    const expire = store.get('expire');
    const COUNTER = store.get('counter');
    const raw = store.get('raw');
    return {
      canRetry,
      COUNTER,
      counter: COUNTER.counter,
      cursor,
      interruptionRegistry,
      connection,
      expire,
      namespace,
      originJobId,
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
   * Return a handle to the hotmesh client hosting the workflow execution
   */
  static async getHotMesh(): Promise<HotMesh> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    return await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
  }

  /**
   * Spawns a child workflow and awaits the return.
   * @template T - the result type
   * @param {WorkflowOptions} options - the workflow options
   * @returns {Promise<T>} - the result of the child workflow
   * @example
   * const result = await MeshFlow.workflow.execChild<typeof resultType>({ ...options });
   */
  static async execChild<T>(options: WorkflowOptions): Promise<T> {
    //SYNC
    //check if the activity already ran (check $error/done)
    const isStartChild = options.await === false;
    const prefix = isStartChild ? 'start' : 'child';
    const [didRun, execIndex, result]: [boolean, number, ChildResponseType<T>] =
      await WorkflowService.didRun(prefix);
    const context = WorkflowService.getContext();
    const { canRetry, interruptionRegistry } = context;

    if (didRun) {
      if (
        result?.$error &&
        (!result.$error.is_stream_error ||
          result.$error.is_stream_error && !canRetry)
      ) {
        if (options?.config?.throwOnError !== false) {
          //rethrow remote execution error (simulates local failure)
          const code: StreamCode = result.$error.code;
          const message = result.$error.message;
          const stack = result.$error.stack;
          if (code === HMSH_CODE_MESHFLOW_FATAL) {
            throw new MeshFlowFatalError(message, stack);
          } else if (code == HMSH_CODE_MESHFLOW_MAXED) {
            throw new MeshFlowMaxedError(message, stack);
          } else if (code == HMSH_CODE_MESHFLOW_TIMEOUT) {
            throw new MeshFlowTimeoutError(message, stack);
          } else {
            throw new MeshFlowRetryError(message, stack);
          }
        }
        return result.$error as T;
      } else if (!result?.$error) {
        return result.data as T;
      }
    }
    const interruptionMessage = WorkflowService.getChildInterruptPayload(
      context,
      options,
      execIndex,
    );
    //push the packaged inputs to the registry
    interruptionRegistry.push({
      code: HMSH_CODE_MESHFLOW_CHILD,
      ...interruptionMessage,
    });
    //ASYNC
    //sleep (allow others to be packaged / registered) and throw the error
    await sleepImmediate();
    throw new MeshFlowChildError(interruptionMessage);
  }

  static executeChild = WorkflowService.execChild;

  /**
   * constructs the payload necessary to spawn a child job
   * @private
   */
  static getChildInterruptPayload(
    context: WorkflowContext,
    options: WorkflowOptions,
    execIndex: number,
  ): MeshFlowChildErrorType {
    const { workflowId, originJobId, workflowDimension, expire } = context;
    let childJobId: string;
    if (options.workflowId) {
      childJobId = options.workflowId;
    } else if (options.entity) {
      childJobId = `${options.entity}-${guid()}-${workflowDimension}-${execIndex}`;
    } else {
      childJobId = `-${options.workflowName}-${guid()}-${workflowDimension}-${execIndex}`;
    }
    const parentWorkflowId = workflowId;
    const taskQueueName = options.taskQueue ?? options.entity;
    const workflowName = options.entity ?? options.workflowName;
    const workflowTopic = `${taskQueueName}-${workflowName}`;
    return {
      arguments: [...(options.args || [])],
      await: options?.await ?? true,
      backoffCoefficient:
        options?.config?.backoffCoefficient ?? HMSH_MESHFLOW_EXP_BACKOFF,
      index: execIndex,
      maximumAttempts:
        options?.config?.maximumAttempts ?? HMSH_MESHFLOW_MAX_ATTEMPTS,
      maximumInterval: s(
        options?.config?.maximumInterval ?? HMSH_MESHFLOW_MAX_INTERVAL,
      ),
      originJobId: originJobId ?? workflowId,
      expire: options.expire ?? expire,
      persistent: options.persistent,
      signalIn: options.signalIn,
      parentWorkflowId,
      workflowDimension: workflowDimension,
      workflowId: childJobId,
      workflowTopic,
    };
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
   * const childJobId = await MeshFlow.workflow.startChild({ ...options });
   */
  static async startChild(options: WorkflowOptions): Promise<string> {
    return WorkflowService.execChild({ ...options, await: false });
  }

  /**
   * Wraps activities in a proxy that durably runs/re-runs them to completion.
   * TODO: verify that activities do not collide if named same on same server but bound to different workflows
   *
   * @param {ActivityConfig} options - the activity configuration
   * that will be used to wrap the activities.
   * @returns {ProxyType<ACT>} - a proxy object with the same keys as the
   * activities object, but with the values replaced by a wrapped function
   * @private
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
        proxy[key] = WorkflowService.wrapActivity<typeof activityFunction>(
          key,
          options,
        );
      });
    }
    return proxy;
  }

  /**
   * @private
   */
  static wrapActivity<T>(activityName: string, options?: ActivityConfig): T {
    return async function () {
      //SYNC
      //check if the activity already ran
      const [didRun, execIndex, result]: [
        boolean,
        number,
        ProxyResponseType<T>,
      ] = await WorkflowService.didRun('proxy');
      if (didRun) {
        if (result?.$error) {
          if (options?.retryPolicy?.throwOnError !== false) {
            //rethrow remote execution error (simulates throw)
            const code: StreamCode = result.$error.code;
            const message = result.$error.message;
            const stack = result.$error.stack;
            if (code === HMSH_CODE_MESHFLOW_FATAL) {
              throw new MeshFlowFatalError(message, stack);
            } else if (code == HMSH_CODE_MESHFLOW_MAXED) {
              throw new MeshFlowMaxedError(message, stack);
            } else if (code == HMSH_CODE_MESHFLOW_TIMEOUT) {
              throw new MeshFlowTimeoutError(message, stack);
            }
          }
          return result.$error as T;
        }
        return result.data as T;
      }
      //package the interruption inputs
      const context = WorkflowService.getContext();
      const { interruptionRegistry } = context;
      const interruptionMessage = WorkflowService.getProxyInterruptPayload(
        context,
        activityName,
        execIndex,
        Array.from(arguments),
        options,
      );
      //push the packaged inputs to the registry
      interruptionRegistry.push({
        code: HMSH_CODE_MESHFLOW_PROXY,
        ...interruptionMessage,
      });
      //ASYNC
      //sleep (allow others to be packaged / registered) and throw the error
      await sleepImmediate();
      throw new MeshFlowProxyError(interruptionMessage);
    } as T;
  }

  /**
   * constructs the payload necessary to spawn a proxyActivity job
   * @private
   */
  static getProxyInterruptPayload(
    context: WorkflowContext,
    activityName: string,
    execIndex: number,
    args: any[],
    options?: ActivityConfig,
  ): MeshFlowProxyErrorType {
    const {
      workflowDimension,
      workflowId,
      originJobId,
      workflowTopic,
      expire,
    } = context;
    const activityTopic = `${workflowTopic}-activity`;
    const activityJobId = `-${workflowId}-$${activityName}${workflowDimension}-${execIndex}`;
    let maximumInterval: number;
    if (options.retryPolicy?.maximumInterval) {
      maximumInterval = s(options.retryPolicy.maximumInterval);
    }
    return {
      arguments: args,
      workflowDimension: workflowDimension,
      index: execIndex,
      originJobId: originJobId || workflowId,
      parentWorkflowId: workflowId,
      workflowId: activityJobId,
      workflowTopic: activityTopic,
      activityName,
      expire: options.expire ?? expire,
      backoffCoefficient: options?.retryPolicy?.backoffCoefficient ?? undefined,
      maximumAttempts: options?.retryPolicy?.maximumAttempts ?? undefined,
      maximumInterval: maximumInterval ?? undefined,
    };
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
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
    //this ID is used as a item key with a hash (dash prefix ensures no collision)
    const searchSessionId = `-search${workflowDimension}-${execIndex}`;
    return new Search(workflowId, hotMeshClient, searchSessionId);
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
  static async signal(
    signalId: string,
    data: Record<any, any>,
  ): Promise<string> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'signal')) {
      return await hotMeshClient.hook(`${namespace}.wfs.signal`, {
        id: signalId,
        data,
      });
    }
  }

  /**
   * Spawns a hook from either the main thread or a hook thread with
   * the provided options; worflowId/TaskQueue/Name are optional and will
   * default to the current workflowId/WorkflowTopic if not provided
   * @param {HookOptions} options - the hook options
   */
  static async hook(options: HookOptions): Promise<string> {
    const { workflowId, connection, namespace, workflowTopic } =
      WorkflowService.getContext();
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'hook')) {
      const targetWorkflowId = options.workflowId ?? workflowId;
      let targetTopic: string;
      if (options.entity || (options.taskQueue && options.workflowName)) {
        targetTopic = `${options.taskQueue ?? options.entity}-${options.entity ?? options.workflowName}`;
      } else {
        targetTopic = workflowTopic;
      }
      const payload = {
        arguments: [...options.args],
        id: targetWorkflowId,
        workflowTopic: targetTopic,
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_MESHFLOW_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_MESHFLOW_MAX_ATTEMPTS,
        maximumInterval: s(
          options?.config?.maximumInterval ?? HMSH_MESHFLOW_MAX_INTERVAL,
        ),
      };
      return await hotMeshClient.hook(
        `${namespace}.flow.signal`,
        payload,
        StreamStatus.PENDING,
        202,
      );
    }
  }

  /**
   * Interrupts a running job
   */
  static async interrupt(
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string | void> {
    const { workflowTopic, connection, namespace } =
      WorkflowService.getContext();
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'interrupt')) {
      return await hotMeshClient.interrupt(
        `${hotMeshClient.appId}.execute`,
        jobId,
        options,
      );
    }
  }

  /**
   * Promise.all (limited to 25 total concurrent workflow)
   * @private
   */
  static async all<T>(...promises: Promise<T>[]): Promise<T[]> {
    await new Promise((resolve) => setTimeout(resolve, 1));
    return await Promise.all(promises);
  }

  /**
   * Sleeps the workflow for a duration. As the function is reentrant,
   * upon reentry, the function will traverse prior execution paths up
   * until the sleep command and then resume execution thereafter.
   * @param {string} duration - See the `ms` package for syntax examples: '1 minute', '2 hours', '3 days'
   * @returns {Promise<number>} - resolved duration in seconds
   */
  static async sleepFor(duration: string): Promise<number> {
    //SYNC
    //return early if this sleep command has already run
    const [didRun, execIndex, result] = await WorkflowService.didRun('sleep');
    if (didRun) {
      return (result as { completion: string; duration: number }).duration; //in seconds
    }
    //package the interruption inputs
    const store = asyncLocalStorage.getStore();
    const interruptionRegistry = store.get('interruptionRegistry');
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const interruptionMessage = {
      workflowId,
      duration: s(duration),
      index: execIndex,
      workflowDimension,
    };
    interruptionRegistry.push({
      code: HMSH_CODE_MESHFLOW_SLEEP,
      ...interruptionMessage,
    });
    //ASYNC
    //sleep to allow other interruptions to be packaged and registered
    await sleepImmediate();
    // NOTE: If you are reading this in the stack trace, await `sleepFor`
    throw new MeshFlowSleepError(interruptionMessage);
  }

  /**
   * Pauses the workflow until `signalId` is received.
   * @template T - the result type
   * @param {string} signalId - a unique, shareable guid (e.g, 'abc123')
   * @returns {Promise<T>}
   * @example
   * const result = await MeshFlow.workflow.waitFor<typeof resultType>('abc123');
   */
  static async waitFor<T>(signalId: string): Promise<T> {
    //SYNC
    //return early if this waitFor command has already run
    const [didRun, execIndex, result] = await WorkflowService.didRun('wait');
    if (didRun) {
      return (result as { id: string; data: { data: T } }).data.data as T;
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
    };
    interruptionRegistry.push({
      code: HMSH_CODE_MESHFLOW_WAIT,
      ...interruptionMessage,
    });
    //ASYNC
    //sleep to allow other interruptions to be packaged and registered
    await sleepImmediate();
    // NOTE: If you are reading this in the stack trace, await `waitFor`
    throw new MeshFlowWaitForError(interruptionMessage);
  }
}
