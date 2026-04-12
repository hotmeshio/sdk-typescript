/**
 * Pub/sub messaging, subscriptions, and one-time callbacks.
 *
 * This module handles the full publish/subscribe lifecycle:
 *   - `pub()`    → fire-and-forget job trigger
 *   - `pubsub()` → trigger + await result (with timeout)
 *   - `sub()/unsub()`   → permanent topic subscriptions
 *   - `psub()/punsub()` → pattern-based subscriptions
 *   - `add()`   → raw stream message publish
 */

import {
  HMSH_OTT_WAIT_TIME,
  HMSH_CODE_TIMEOUT,
} from '../../modules/enums';
import { KeyType } from '../../modules/key';
import { Router } from '../router';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import { ILogger } from '../logger';
import { AppVID } from '../../types/app';
import {
  ExtensionType,
  JobData,
  JobOutput,
  JobState,
} from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import {
  JobMessageCallback,
  SubscriptionCallback,
} from '../../types/quorum';
import {
  StreamData,
  StreamDataResponse,
  StreamError,
} from '../../types/stream';

interface PubSubContext {
  guid: string;
  appId: string;
  store: StoreService<ProviderClient, ProviderTransaction>;
  stream: StreamService<ProviderClient, ProviderTransaction>;
  subscribe: SubService<ProviderClient>;
  router: Router<typeof this.stream> | null;
  logger: ILogger;
  jobCallbacks: Record<string, JobMessageCallback>;
  getVID(vid?: AppVID): Promise<AppVID>;
  initActivity(topic: string, data?: JobData, context?: JobState): Promise<any>;
  getState(topic: string, jobId: string): Promise<JobOutput>;
  getPublishesTopic(context: JobState): Promise<string>;
}

export async function pub(
  instance: PubSubContext,
  topic: string,
  data: JobData,
  context?: JobState,
  extended?: ExtensionType,
): Promise<string> {
  const activityHandler = await instance.initActivity(topic, data, context);
  if (activityHandler) {
    return await activityHandler.process(extended);
  } else {
    throw new Error(`unable to process activity for topic ${topic}`);
  }
}

export async function sub(
  instance: PubSubContext,
  topic: string,
  callback: JobMessageCallback,
): Promise<void> {
  const subscriptionCallback = createSubscriptionCallback(instance, callback);
  return await instance.subscribe.subscribe(
    KeyType.QUORUM,
    subscriptionCallback,
    instance.appId,
    topic,
  );
}

export async function unsub(
  instance: PubSubContext,
  topic: string,
): Promise<void> {
  return await instance.subscribe.unsubscribe(
    KeyType.QUORUM,
    instance.appId,
    topic,
  );
}

export async function psub(
  instance: PubSubContext,
  wild: string,
  callback: JobMessageCallback,
): Promise<void> {
  const subscriptionCallback = createSubscriptionCallback(instance, callback);
  return await instance.subscribe.psubscribe(
    KeyType.QUORUM,
    subscriptionCallback,
    instance.appId,
    wild,
  );
}

export async function punsub(
  instance: PubSubContext,
  wild: string,
): Promise<void> {
  return await instance.subscribe.punsubscribe(
    KeyType.QUORUM,
    instance.appId,
    wild,
  );
}

export async function pubsub(
  instance: PubSubContext,
  topic: string,
  data: JobData,
  context?: JobState | null,
  timeout = HMSH_OTT_WAIT_TIME,
): Promise<JobOutput> {
  context = {
    metadata: {
      ngn: instance.guid,
      trc: context?.metadata?.trc,
      spn: context?.metadata?.spn,
    },
  } as JobState;
  const jobId = await pub(instance, topic, data, context);
  return new Promise((resolve, reject) => {
    registerJobCallback(instance, jobId, (topic: string, output: JobOutput) => {
      if (output.metadata.err) {
        const error = JSON.parse(output.metadata.err) as StreamError;
        reject({
          error,
          job_id: output.metadata.jid,
        });
      } else {
        resolve(output);
      }
    });
    setTimeout(() => {
      removeJobCallback(instance, jobId);
      reject({
        code: HMSH_CODE_TIMEOUT,
        message: 'timeout',
        job_id: jobId,
      } as StreamError);
    }, timeout);
  });
}

export async function add(
  instance: PubSubContext,
  streamData: StreamData | StreamDataResponse,
): Promise<string> {
  return (await instance.router?.publishMessage(null, streamData)) as string;
}

export function registerJobCallback(
  instance: PubSubContext,
  jobId: string,
  jobCallback: JobMessageCallback,
) {
  instance.jobCallbacks[jobId] = jobCallback;
}

export function removeJobCallback(
  instance: PubSubContext,
  jobId: string,
) {
  delete instance.jobCallbacks[jobId];
}

export function hasOneTimeSubscription(context: JobState): boolean {
  return Boolean(context.metadata.ngn);
}

/**
 * Resolves the `publishes` topic for the activity that produced
 * this job's output — used to notify permanent subscribers.
 */
export async function getPublishesTopic(
  instance: PubSubContext,
  context: JobState,
): Promise<string> {
  const config = await instance.getVID();
  const activityId =
    context.metadata.aid || context['$self']?.output?.metadata?.aid;
  const schema = await instance.store.getSchema(activityId, config);
  return schema.publishes;
}

// ── helpers ────────────────────────────────────────────────────────

/**
 * Wraps a user-provided callback to handle reference-based payloads.
 * When `_ref` is true the published message was too large, so
 * we fetch the full job state before invoking the callback.
 */
function createSubscriptionCallback(
  instance: PubSubContext,
  callback: JobMessageCallback,
): SubscriptionCallback {
  return async (
    topic: string,
    message: { topic: string; job: JobOutput; _ref?: boolean },
  ) => {
    let jobOutput = message.job;
    if (message._ref && message.job?.metadata) {
      jobOutput = await instance.getState(
        message.job.metadata.tpc,
        message.job.metadata.jid,
      );
    }
    callback(message.topic, jobOutput);
  };
}
