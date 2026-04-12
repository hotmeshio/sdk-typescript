/**
 * Signal delivery to paused hook activities.
 *
 * Signals resume workflows waiting on external events:
 *   - `signal()`    → single webhook delivery to a specific hook
 *   - `signalAll()` → fan-out to all workflows matching a query
 *   - `hookTime()`  → time-based hook awaken (sleep/cron)
 *
 * Also provides the quorum-level task processors for web/time hooks
 * and the router throttle control.
 */

import { KeyType, VALSEP } from '../../modules/key';
import { getSubscriptionTopic, guid } from '../../modules/utils';
import { ReporterService } from '../reporter';
import { Router } from '../router';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import { TaskService } from '../task';
import { ILogger } from '../logger';
import { AppVID } from '../../types/app';
import { JobData } from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { JobStatsInput } from '../../types/stats';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamStatus,
} from '../../types/stream';
import { WorkListTaskType } from '../../types/task';
import { HookInterface } from '../../types/hook';
import { JobMessageCallback } from '../../types/quorum';
import { JobOutput } from '../../types/job';

interface SignalContext {
  guid: string;
  appId: string;
  store: StoreService<ProviderClient, ProviderTransaction>;
  stream: StreamService<ProviderClient, ProviderTransaction>;
  subscribe: SubService<ProviderClient>;
  router: Router<typeof this.stream> | null;
  taskService: TaskService;
  logger: ILogger;
  jobCallbacks: Record<string, JobMessageCallback>;
  getVID(vid?: AppVID): Promise<AppVID>;
  getSchema(topic: string): Promise<[string, any]>;
  resolveQuery(topic: string, query: JobStatsInput): Promise<any>;
  interrupt(topic: string, jobId: string, options?: any): Promise<string>;
}

/**
 * Delivers a signal (data payload) to a paused hook activity,
 * resuming its Leg 2 execution.
 */
export async function signal(
  instance: SignalContext,
  topic: string,
  data: JobData,
  status: StreamStatus = StreamStatus.SUCCESS,
  code: StreamCode = 200,
  transaction?: ProviderTransaction,
): Promise<string> {
  const hookRule = await instance.taskService.getHookRule(topic);
  const [aid] = await instance.getSchema(`.${hookRule.to}`);
  const streamData: StreamData = {
    type: StreamDataType.WEBHOOK,
    status,
    code,
    metadata: {
      guid: guid(),
      aid,
      topic,
    },
    data,
  };
  return (await instance.router?.publishMessage(
    null,
    streamData,
    transaction,
  )) as string;
}

export async function hookTime(
  instance: SignalContext,
  jobId: string,
  gId: string,
  topicOrActivity: string,
  type?: WorkListTaskType,
): Promise<string | void> {
  if (type === 'interrupt' || type === 'expire') {
    return await instance.interrupt(topicOrActivity, jobId, {
      suppress: true,
      expire: 1,
    });
  }
  const [aid, ...dimensions] = topicOrActivity.split(',');
  const dad = `,${dimensions.join(',')}`;
  const streamData: StreamData = {
    type: StreamDataType.TIMEHOOK,
    metadata: {
      guid: guid(),
      jid: jobId,
      gid: gId,
      dad,
      aid,
    },
    data: { timestamp: Date.now() },
  };
  await instance.router?.publishMessage(null, streamData);
}

/**
 * Fan-out variant that delivers data to all paused workflows
 * matching a search query.
 */
export async function signalAll(
  instance: SignalContext,
  hookTopic: string,
  data: JobData,
  keyResolver: JobStatsInput,
  queryFacets: string[] = [],
): Promise<string[]> {
  const config = await instance.getVID();
  const hookRule = await instance.taskService.getHookRule(hookTopic);
  if (hookRule) {
    const subscriptionTopic = await getSubscriptionTopic(
      hookRule.to,
      instance.store,
      config,
    );
    const resolvedQuery = await instance.resolveQuery(
      subscriptionTopic,
      keyResolver,
    );
    const reporter = new ReporterService(
      config,
      instance.store,
      instance.logger,
    );
    const workItems = await reporter.getWorkItems(resolvedQuery, queryFacets);
    if (workItems.length) {
      const taskService = new TaskService(instance.store, instance.logger);
      await taskService.enqueueWorkItems(
        workItems.map((workItem) =>
          [
            hookTopic,
            workItem,
            keyResolver.scrub || false,
            JSON.stringify(data),
          ].join(VALSEP),
        ),
      );
      instance.subscribe.publish(
        KeyType.QUORUM,
        { type: 'work', originator: instance.guid },
        instance.appId,
      );
    }
    return workItems;
  } else {
    throw new Error(`unable to find hook rule for topic ${hookTopic}`);
  }
}

export async function processWebHooks(
  instance: SignalContext,
  signalFn: HookInterface,
) {
  instance.taskService.processWebHooks(signalFn);
}

export async function processTimeHooks(
  instance: SignalContext,
  hookTimeFn: (
    jobId: string,
    gId: string,
    activityId: string,
    type: WorkListTaskType,
  ) => Promise<void>,
) {
  instance.taskService.processTimeHooks(hookTimeFn);
}

export async function throttle(instance: SignalContext, delayInMillis: number) {
  try {
    instance.router?.setThrottle(delayInMillis);
  } catch (e) {
    instance.logger.error('engine-throttle-error', { error: e });
  }
}

export function routeToSubscribers(
  instance: SignalContext,
  topic: string,
  message: JobOutput,
) {
  const jobCallback = instance.jobCallbacks[message.metadata.jid];
  if (jobCallback) {
    delete instance.jobCallbacks[message.metadata.jid];
    jobCallback(topic, message);
  }
}
