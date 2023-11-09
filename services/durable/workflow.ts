import ms from 'ms';

import { asyncLocalStorage } from './asyncLocalStorage';
import { WorkerService } from './worker';
import { ClientService as Client } from './client';
import { ConnectionService as Connection } from './connection';
import { ActivityConfig, ProxyType, WorkflowOptions } from "../../types/durable";
import { JobOutput, JobState } from '../../types';
import { ACTIVITY_PUBLISHES_TOPIC, ACTIVITY_SUBSCRIBES_TOPIC, SLEEP_SUBSCRIBES_TOPIC, WFS_SUBSCRIBES_TOPIC } from './factory';
import { DurableIncompleteSignalError, DurableSleepError, DurableWaitForSignalError } from '../../modules/errors';

/*
`proxyActivities` returns a wrapped instance of the 
target activity, so that when the workflow calls a
proxied activity, it is actually calling the proxy
function, which in turn calls the activity function.

Here is an example of how the methods in this file are used:

./workflows.ts

import { Durable } from '@hotmeshio/hotmesh';
import * as activities from './activities';

const { greet } = Durable.workflow.proxyActivities<typeof activities>({
  activities: activities,
  startToCloseTimeout: '1 minute',
  retryPolicy: {
    initialInterval: '5 seconds',  // Initial delay between retries
    maximumAttempts: 3,            // Max number of retry attempts
    backoffCoefficient: 2.0,       // Backoff factor for delay between retries: delay = initialInterval * (backoffCoefficient ^ retry_attempt)
    maximumInterval: '30 seconds', // Max delay between retries
  },
});

export async function example(name: string): Promise<string> {
  return await greet(name);
}
*/

export class WorkflowService {

  /**
   * Spawn a child workflow. await the result.
   */
  static async executeChild<T>(options: WorkflowOptions): Promise<T> {
    const store = asyncLocalStorage.getStore();
    if (!store) {
      throw new Error('durable-store-not-found');
    }
    const workflowId = store.get('workflowId');
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const childJobId = `${workflowId}-$${options.workflowName}-${execIndex}`;
    const parentWorkflowId = `${workflowId}-f`;

    const client = new Client({
      connection: await Connection.connect(WorkerService.connection),
    });

    let handle = await client.workflow.getHandle(
      options.taskQueue,
      options.workflowName,
      childJobId
    );

    try {
      return await handle.result() as T;
    } catch (error) {
      handle = await client.workflow.start({
        ...options,
        workflowId: childJobId,
        parentWorkflowId,
        workflowTrace,
        workflowSpan,
      });
      const result = await handle.result();
      return result as T;
    }
  }

  static proxyActivities<ACT>(options?: ActivityConfig): ProxyType<ACT> {
    if (options.activities) {
      WorkerService.registerActivities(options.activities)
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

  static async sleep(duration: string): Promise<number> {
    const seconds = ms(duration) / 1000;

    const store = asyncLocalStorage.getStore();
    if (!store) {
      throw new Error('durable-store-not-found');
    }
    const COUNTER = store.get('counter');
    const execIndex = COUNTER.counter = COUNTER.counter + 1;
    const workflowId = store.get('workflowId');
    const workflowTopic = store.get('workflowTopic');
    const sleepJobId = `${workflowId}-$sleep-${execIndex}`;

    try {
      const hotMeshClient = await WorkerService.getHotMesh(workflowTopic);
      await hotMeshClient.getState(SLEEP_SUBSCRIBES_TOPIC, sleepJobId);
      //if no error is thrown, we've already slept, return the delay
      return seconds;
    } catch (e) {
      //if an error, the sleep job was not found...rethrow error; sleep job
      // will be automatically created according to the DAG rules (they
      // spawn a new sleep job if error code 595 is thrown by the worker)
      throw new DurableSleepError(workflowId, seconds, execIndex);
    }
  }

  static async waitForSignal(signals: string[], options?: Record<string, string>): Promise<Record<any, any>[]> {
    const store = asyncLocalStorage.getStore();
    if (!store) {
      throw new Error('durable-store-not-found');
    }
    const COUNTER = store.get('counter');
    const workflowId = store.get('workflowId');
    const workflowTopic = store.get('workflowTopic');
    const hotMeshClient = await WorkerService.getHotMesh(workflowTopic);

    //iterate the list of signals and check for done
    let allAreComplete = true;
    let noneAreComplete = false;
    const signalResults: any[] = [];
    for (const signal of signals) {
      const execIndex = COUNTER.counter = COUNTER.counter + 1;
      const wfsJobId = `${workflowId}-$wfs-${execIndex}`;
      try {
        if (allAreComplete) {
          const state = await hotMeshClient.getState(WFS_SUBSCRIBES_TOPIC, wfsJobId);
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
      if (!store) {
        throw new Error('durable-store-not-found');
      }
      const COUNTER = store.get('counter');
      //increment by state (not value) to avoid race conditions
      const execIndex = COUNTER.counter = COUNTER.counter + 1;
      const workflowId = store.get('workflowId');
      const workflowTopic = store.get('workflowTopic');
      const trc = store.get('workflowTrace');
      const spn = store.get('workflowSpan');
      const activityTopic = `${workflowTopic}-activity`;
      const activityJobId = `${workflowId}-$${activityName}-${execIndex}`;

      let activityState: JobOutput
      try {
        const hotMeshClient = await WorkerService.getHotMesh(activityTopic);
        activityState = await hotMeshClient.getState(ACTIVITY_SUBSCRIBES_TOPIC, activityJobId);
        if (activityState.metadata.err) {
          await hotMeshClient.scrub(activityJobId);
          throw new Error(activityState.metadata.err);
        } else if (activityState.metadata.js === 0 || activityState.data?.done) {
          return activityState.data?.response as T;
        }
        //one time subscription
        return await new Promise((resolve, reject) => {
          hotMeshClient.sub(`${ACTIVITY_PUBLISHES_TOPIC}.${activityJobId}`, async (topic, message) => {
            const response = message.data?.response;
            hotMeshClient.unsub(`${ACTIVITY_PUBLISHES_TOPIC}.${activityJobId}`);
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
        const hotMeshClient = await WorkerService.getHotMesh(activityTopic);
        const context = { metadata: { trc, spn }, data: {}};
        const jobOutput = await hotMeshClient.pubsub(ACTIVITY_SUBSCRIBES_TOPIC, payload, context as JobState, duration);
        return jobOutput.data.response as T;
      }
    } as T;
  }
}
