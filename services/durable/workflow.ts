import ms from 'ms';

import { asyncLocalStorage } from './asyncLocalStorage';
import { WorkerService } from './worker';
import { ClientService as Client } from './client';
import { ConnectionService as Connection } from './connection';
import { ActivityConfig, ProxyType, WorkflowOptions } from "../../types/durable";
import { JobOutput, JobState } from '../../types';

/*
`proxyActivities` returns a wrapped instance of the 
target activity, so that when the workflow calls a
proxied activity, it is actually calling the proxy
function, which in turn calls the activity function.

`proxyActivities` must be called AFTER the activities
have been registered in order to work properly.
If the activities are not already registered,
`proxyActivities` will throw an error. This is OK.

The `client` (client.ts) is equivalent to the 
HotMesh `engine`. The jobs it creates will be
put in the taskQueue. When the `worker` (worker.ts)
is eventually initialized (if it happens to be inited later),
it will see the items in the queue and process them. If it happens
to already be inited, the jobs will immediately be dequeued and
processed. In either case, the jobs will be processed.

Here is an example of how the methods in this file are used:

./workflows.ts

import { Durable } from '@hotmeshio/hotmesh';
import type * as activities from './activities';
const { greet } = Durable.workflow.proxyActivities<typeof activities>({
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
  static async executeChild<T>(options: WorkflowOptions): Promise<T> {
    const store = asyncLocalStorage.getStore();
    if (!store) {
      throw new Error('durable-store-not-found');
    }
    const workflowId = store.get('workflowId');
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');

    const client = new Client({
      connection: await Connection.connect(WorkerService.connection),
    });
    //todo: should I allow-cross/app callback (pj:'@DURABLE@hello-world@<pjid>'/pa: <paid>/pd: <pdad>)
    const handle = await client.workflow.start({
      ...options,
      workflowId: `${workflowId}${options.workflowId}`, //concat
      workflowTrace,
      workflowSpan,
    });
    const result = await handle.result();
    return result as T;
  }

  static proxyActivities<ACT>(options?: ActivityConfig): ProxyType<ACT> {
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
      const activityJobId = `${workflowId}-${activityName}-${execIndex}`;

      let activityState: JobOutput
      try {
        const hmshInstance = await WorkerService.getHotMesh(activityTopic);
        activityState = await hmshInstance.getState(activityTopic, activityJobId);
        if (activityState.metadata.err) {
          await hmshInstance.scrub(activityJobId);
          throw new Error(activityState.metadata.err);
        } else if (activityState.metadata.js === 0) {
          //return immediately
          return activityState.data?.response as T;
        }
        //one time subscription
        return await new Promise((resolve, reject) => {
          hmshInstance.sub(activityTopic, async (topic, message) => {
            const response = message.data?.response;
            hmshInstance.unsub(activityTopic);
            // Resolve the Promise when the callback is triggered with a message
            resolve(response);
          });
        });
      } catch (e) {
        //expected; thrown by `getState` when the job cannot be found
        const duration = ms(options?.startToCloseTimeout || '1 minute');
        const payload = {
          arguments: Array.from(arguments),
          workflowId: activityJobId,
          workflowTopic,
          activityName,
        };
        //start the job
        const hmshInstance = await WorkerService.getHotMesh(activityTopic);
        const context = { metadata: { trc, spn }, data: {}};
        const jobOutput = await hmshInstance.pubsub(activityTopic, payload, context as JobState, duration);
        return jobOutput.data.response as T;
      }
    } as T;
  }
}
