import ms from 'ms';

import {
  DurableIncompleteSignalError,
  DurableSleepError,
  DurableWaitForSignalError } from '../../modules/errors';
import { KeyService, KeyType } from '../../modules/key';
import { asyncLocalStorage } from './asyncLocalStorage';
import { ClientService as Client } from './client';
import { ConnectionService as Connection } from './connection';
import { DEFAULT_COEFFICIENT } from './factory';
import { Search } from './search';
import { WorkerService } from './worker';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ActivityConfig,
  HookOptions,
  ProxyType,
  WorkflowContext,
  WorkflowOptions } from "../../types/durable";
import { JobOutput, JobState } from '../../types/job';
import { StreamStatus } from '../../types/stream';
import { deterministicRandom } from '../../modules/utils';

export class WorkflowService {

  /**
   * Spawns a child workflow. await and return the result.
   * @template T - the result type
   * @param {WorkflowOptions} options - the workflow options
   * @returns {Promise<T>} - the result of the child workflow
   */
  static async executeChild<T>(options: WorkflowOptions): Promise<T> {
    const store = asyncLocalStorage.getStore();
    const namespace = store.get('namespace');
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    //NOTE: this is the hash prefix; necessary for the search index to locate the entity
    //if the hash is a helper, a dash begins it, so it isn't indexed
    const entityOrEmptyString = options.entity ?? '';
    //If the workflowId is not provided, it is generated from the entity and the workflow name
    const childJobId = options.workflowId ?? `${entityOrEmptyString}-${workflowId}-$${options.entity ?? options.workflowName}${workflowDimension}-${execIndex}`;
    const parentWorkflowId = `${workflowId}-f`;

    const client = new Client({
      connection: await Connection.connect(WorkerService.connection),
    });

    let handle = await client.workflow.getHandle(
      options.entity ?? options.taskQueue,
      options.entity ?? options.workflowName,
      childJobId,
      namespace,
    );

    try {
      return await handle.result(true) as T;
    } catch (error) {
      handle = await client.workflow.start({
        ...options,
        namespace,
        workflowId: childJobId,
        parentWorkflowId,
        workflowTrace,
        workflowSpan,
      });
      //todo: options.startToCloseTimeout
      const result = await handle.result();
      return result as T;
    }
  }

  /**
   * Spawns a child workflow. return the childJobId.
   * This method is used when the result of the child workflow is not needed.
   * @param {WorkflowOptions} options - the workflow options
   * @returns {Promise<string>} - the childJobId
   */
  static async startChild<T>(options: WorkflowOptions): Promise<string> {
    const store = asyncLocalStorage.getStore();
    const namespace = store.get('namespace');
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    //NOTE: this is the hash prefix; necessary for the search index to locate the entity
    const entityOrEmptyString = options.entity ?? '';
    //If the workflowId is not provided, it is generated from the entity and the workflow name
    const childJobId = options.workflowId ?? `${entityOrEmptyString}-${workflowId}-$${options.entity ?? options.workflowName}${workflowDimension}-${execIndex}`;
    const parentWorkflowId = `${workflowId}-f`;
    const workflowTopic = `${options.entity ?? options.taskQueue}-${options.entity ?? options.workflowName}`;

    try {
      //get the status; if there is no error, return childJobId (what was spawned)
      const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
      await hotMeshClient.getStatus(childJobId);
      return childJobId;
    } catch (error) {
      const client = new Client({
        connection: await Connection.connect(WorkerService.connection),
      });
  
      await client.workflow.start({
        ...options,
        namespace,
        workflowId: childJobId,
        parentWorkflowId,
        workflowTrace,
        workflowSpan,
      });
      return childJobId;
    }
  }

  /**
   * Wraps activities in a proxy that will durably run them
   * @param {ActivityConfig} options - the activity configuration
   * that will be used to wrap the activities. You must pass an
   * `activities` object to this configuration. The activities object
   * should be a key-value pair of activity names and their respective
   * functions. This is typically done by importing the activities.
   * 
   * @returns {ProxyType<ACT>} - a proxy object with the same keys as the
   * activities object, but with the values replaced by a wrapped function
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
   * Returns the current workflow context
   * @returns {WorkflowContext} - the current workflow context
   */
  static getContext(): WorkflowContext {
    const store = asyncLocalStorage.getStore();
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    const COUNTER = store.get('counter');
    return {
      counter: COUNTER.counter,
      namespace,
      workflowId,
      workflowDimension,
      workflowTopic,
      workflowTrace,
      workflowSpan,
    };
  }

  /**
   * Those methods that may only be called once must be protected by flagging
   * their execution with a unique key (the key is stored in the HASH alongside
   * process state and job state)
   * @private
   */
  static async isSideEffectAllowed(hotMeshClient: HotMesh, prefix:string): Promise<boolean> {
    const store = asyncLocalStorage.getStore();
    const workflowId = store.get('workflowId');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
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
   * Sends signal data into any other paused thread (which is paused and
   * awaiting the signal) from within a hook-thread or the main-thread
   * @param {string} signalId - the signal id
   * @param {Record<any, any>} data - the signal data
   * @returns {Promise<string>} - the stream id
   */
  static async signal(signalId: string, data: Record<any, any>): Promise<string> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    //todo: this particular one is better patterned as a get/set,
    //since the receipt is a meaningful string (the stream id)
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
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
    if (await WorkflowService.isSideEffectAllowed(hotMeshClient, 'hook')) {
      const store = asyncLocalStorage.getStore();
      const workflowId = options.workflowId ?? store.get('workflowId');
      let workflowTopic = store.get('workflowTopic');
      if (options.entity || (options.taskQueue && options.workflowName)) {
        workflowTopic = `${options.entity ?? options.taskQueue}-${options.entity ?? options.workflowName}`;
      } //else this is essentially recursion as the function calls itself
      const payload = {
        arguments: [...options.args],
        id: workflowId,
        workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || DEFAULT_COEFFICIENT,
      }
      return await hotMeshClient.hook(`${namespace}.flow.signal`, payload, StreamStatus.PENDING, 202);
    }
  }

  /**
   * Sleeps for a duration.
   * @param {string} duration - for example: '1 minute', '2 hours', '3 days'
   * @returns {Promise<number>}
   */
  static async sleep(duration: string): Promise<number> {
    const seconds = ms(duration) / 1000;

    const store = asyncLocalStorage.getStore();
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const workflowId = store.get('workflowId');
    const workflowTopic = store.get('workflowTopic');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const namespace = store.get('namespace');
    const sleepJobId = `-${workflowId}-$sleep${workflowDimension}-${execIndex}`;

    try {
      const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });
      await hotMeshClient.getState(`${hotMeshClient.appId}.sleep.execute`, sleepJobId);
      //if no error is thrown, we've already slept, return the delay
      return seconds;
    } catch (e) {
      // spawn a new sleep job if error code 595 is thrown by the worker)
      // NOTE: If this message shows up in your stack trace, you forgot to await `Durable.workflow.sleep()` in your workflow code.
      throw new DurableSleepError(workflowId, seconds, execIndex, workflowDimension);
    }
  }

  /**
   * Waits for a signal to awaken
   * @param {string[]} signals - the signals to wait for
   * @param {Record<string, string>} options - the options
   * @returns {Promise<Record<any, any>[]>}
   */
  static async waitForSignal(signals: string[], options?: Record<string, string>): Promise<Record<any, any>[]> {
    const store = asyncLocalStorage.getStore();
    const COUNTER = store.get('counter');
    const workflowId = store.get('workflowId');
    const workflowTopic = store.get('workflowTopic');
    const workflowDimension = store.get('workflowDimension') ?? '';
    const namespace = store.get('namespace');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, { namespace });

    //iterate the list of signals and check for done
    let allAreComplete = true;
    let noneAreComplete = false;
    const signalResults: any[] = [];
    for (const signal of signals) {
      const execIndex = COUNTER.counter = COUNTER.counter + 1;
      const wfsJobId = `-${workflowId}-$wfs${workflowDimension}-${execIndex}`;
      try {
        if (allAreComplete) {
          const state = await hotMeshClient.getState(`${hotMeshClient.appId}.wfs.execute`, wfsJobId);
          if (state.data?.signalData) {
            //user data is nested to isolate from the signal id; unpackage it
            const signalData = state.data.signalData as { id: string, data: Record<any, any> };
            signalResults.push(signalData.data);
          } else {
            allAreComplete = false;
          }
        } else {
          signalResults.push({ signal, index: execIndex });
        }
      } catch (err) {
        //todo: options.startToCloseTimeout
        allAreComplete = false;
        noneAreComplete = true;
        signalResults.push({ signal, index: execIndex });
      }
    };

    if(allAreComplete) {
      return signalResults;
    } else if(noneAreComplete) {
      //this error is caught by the workflow runner
      //it is then returned as the workflow result (594)
      throw new DurableWaitForSignalError(workflowId, signalResults);
    } else {
      //this error happens when a signal is received but others are still open
      throw new DurableIncompleteSignalError(workflowId);
    }
  }

  static wrapActivity<T>(activityName: string, options?: ActivityConfig): T {
    return async function() {
      const store = asyncLocalStorage.getStore();
      const COUNTER = store.get('counter');
      //increment by state (not value) to avoid race conditions
      const execIndex = COUNTER.counter = COUNTER.counter + 1;
      const workflowId = store.get('workflowId');
      const workflowDimension = store.get('workflowDimension') ?? '';
      const workflowTopic = store.get('workflowTopic');
      const trc = store.get('workflowTrace');
      const spn = store.get('workflowSpan');
      const namespace = store.get('namespace');
      const activityTopic = `${workflowTopic}-activity`;
      const activityJobId = `-${workflowId}-$${activityName}${workflowDimension}-${execIndex}`;

      let activityState: JobOutput
      try {
        const hotMeshClient = await WorkerService.getHotMesh(activityTopic, { namespace });
        activityState = await hotMeshClient.getState(`${hotMeshClient.appId}.activity.execute`, activityJobId);
        if (activityState.metadata.err) {
          await hotMeshClient.scrub(activityJobId);
          throw new Error(activityState.metadata.err);
        } else if (activityState.metadata.js === 0 || activityState.data?.done) {
          return activityState.data?.response as T;
        }
        //one time subscription
        return await new Promise((resolve, reject) => {
          hotMeshClient.sub(`${hotMeshClient.appId}.activity.executed.${activityJobId}`, async (topic, message) => {
            const response = message.data?.response;
            hotMeshClient.unsub(`${hotMeshClient.appId}.activity.executed.${activityJobId}`);
            // Resolve the Promise when the callback is triggered with a message
            resolve(response);
          });
        });
      } catch (e) {
        //expected; thrown by `getState` when the job cannot be found
        const duration = ms(options?.startToCloseTimeout || '1 minute');
        const payload = {
          arguments: Array.from(arguments),
          //the parent id is provided to categorize this activity for later cleanup
          parentWorkflowId: `${workflowId}-a`,
          workflowId: activityJobId,
          workflowTopic: activityTopic,
          activityName,
        };
        //start the job
        const hotMeshClient = await WorkerService.getHotMesh(activityTopic, { namespace });
        const context = { metadata: { trc, spn }, data: {}};
        const jobOutput = await hotMeshClient.pubsub(`${hotMeshClient.appId}.activity.execute`, payload, context as JobState, duration);
        return jobOutput.data.response as T;
      }
    } as T;
  }
}
