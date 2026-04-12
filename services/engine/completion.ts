/**
 * Job lifecycle completion — parent notification, cleanup, and expiry.
 *
 * When a job finishes (or is interrupted), this module:
 * 1. Notifies the parent job (if this is a child via execChild)
 * 2. Publishes to one-time and permanent subscribers
 * 3. Registers the job for TTL-based cleanup
 */

import {
  HMSH_CODE_SUCCESS,
  HMSH_CODE_PENDING,
  HMSH_EXPIRE_JOB_SECONDS,
} from '../../modules/enums';
import { guid, restoreHierarchy } from '../../modules/utils';
import { KeyType } from '../../modules/key';
import { Router } from '../router';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import { TaskService } from '../task';
import { ILogger } from '../logger';
import { AppVID } from '../../types/app';
import {
  JobState,
  JobOutput,
  JobMetadata,
  JobCompletionOptions,
  JobInterruptOptions,
} from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { JobMessage } from '../../types/quorum';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamError,
  StreamStatus,
} from '../../types/stream';

interface CompletionContext {
  appId: string;
  store: StoreService<ProviderClient, ProviderTransaction>;
  stream: StreamService<ProviderClient, ProviderTransaction>;
  subscribe: SubService<ProviderClient>;
  router: Router<typeof this.stream> | null;
  taskService: TaskService;
  logger: ILogger;
  getVID(vid?: AppVID): Promise<AppVID>;
  getState(topic: string, jobId: string): Promise<JobOutput>;
  getPublishesTopic(context: JobState): Promise<string>;
}

/**
 * Sends the child job result back to the waiting parent activity.
 * Only applies to non-severed children (execChild, not startChild).
 */
export async function execAdjacentParent(
  instance: CompletionContext,
  context: JobState,
  jobOutput: JobOutput,
  emit = false,
  transaction?: ProviderTransaction,
): Promise<string> {
  if (hasParentJob(context)) {
    const error = resolveError(jobOutput.metadata);
    const spn =
      context['$self']?.output?.metadata?.l2s ||
      context['$self']?.output?.metadata?.l1s;
    const streamData: StreamData = {
      metadata: {
        guid: guid(),
        jid: context.metadata.pj,
        gid: context.metadata.pg,
        dad: context.metadata.pd,
        aid: context.metadata.pa,
        trc: context.metadata.trc,
        spn,
      },
      type: StreamDataType.RESULT,
      data: jobOutput.data,
    };
    if (error && error.code) {
      streamData.status = StreamStatus.ERROR;
      streamData.data = error;
      streamData.code = error.code;
      streamData.stack = error.stack;
    } else if (emit) {
      streamData.status = StreamStatus.PENDING;
      streamData.code = HMSH_CODE_PENDING;
    } else {
      streamData.status = StreamStatus.SUCCESS;
      streamData.code = HMSH_CODE_SUCCESS;
    }
    return (await instance.router?.publishMessage(
      null,
      streamData,
      transaction,
    )) as string;
  }
}

export function hasParentJob(
  context: JobState,
  checkSevered = false,
): boolean {
  if (checkSevered) {
    return Boolean(
      context.metadata.pj && context.metadata.pa && !context.metadata.px,
    );
  }
  return Boolean(context.metadata.pj && context.metadata.pa);
}

export function resolveError(
  metadata: JobMetadata,
): StreamError | undefined {
  if (metadata && metadata.err) {
    return JSON.parse(metadata.err) as StreamError;
  }
}

export async function interrupt(
  instance: CompletionContext,
  topic: string,
  jobId: string,
  options: JobInterruptOptions = {},
): Promise<string> {
  await instance.store.interrupt(topic, jobId, options);
  const context = (await instance.getState(topic, jobId)) as JobState;
  const completionOpts: JobCompletionOptions = {
    interrupt: options.descend,
    expire: options.expire,
  };
  return (await runJobCompletionTasks(
    instance,
    context,
    completionOpts,
  )) as string;
}

export async function scrub(
  instance: CompletionContext,
  jobId: string,
) {
  await instance.store.scrub(jobId);
}

/**
 * Orchestrates all post-completion work for a finished job:
 * notify parent, publish to subscribers, schedule cleanup.
 */
export async function runJobCompletionTasks(
  instance: CompletionContext,
  context: JobState,
  options: JobCompletionOptions = {},
  transaction?: ProviderTransaction,
): Promise<string | void> {
  const isAwait = hasParentJob(context, true);
  const isOneTimeSub = hasOneTimeSubscription(context);
  const topic = await instance.getPublishesTopic(context);
  let msgId: string;
  let jobOutput: JobOutput;

  if (isAwait || isOneTimeSub || topic) {
    jobOutput = await instance.getState(
      context.metadata.tpc,
      context.metadata.jid,
    );
    if (isAwait) {
      msgId = await execAdjacentParent(
        instance,
        context,
        jobOutput,
        options.emit,
        transaction,
      );
    }
    if (transaction) {
      await publishOneTimeSubscribers(instance, context, jobOutput, options.emit, transaction);
      await publishPermanentSubscribers(instance, context, jobOutput, options.emit, transaction);
    } else {
      publishOneTimeSubscribers(instance, context, jobOutput, options.emit);
      publishPermanentSubscribers(instance, context, jobOutput, options.emit);
    }
  }

  if (!options.emit) {
    if (transaction) {
      await instance.taskService.registerJobForCleanup(
        context.metadata.jid,
        resolveExpires(context, options),
        options,
        transaction,
      );
    } else {
      instance.taskService.registerJobForCleanup(
        context.metadata.jid,
        resolveExpires(context, options),
        options,
      );
    }
  }
  return msgId;
}

export function resolveExpires(
  context: JobState,
  options: JobCompletionOptions,
): number {
  return options.expire ?? context.metadata.expire ?? HMSH_EXPIRE_JOB_SECONDS;
}

function hasOneTimeSubscription(context: JobState): boolean {
  return Boolean(context.metadata.ngn);
}

async function publishOneTimeSubscribers(
  instance: CompletionContext,
  context: JobState,
  jobOutput: JobOutput,
  emit = false,
  transaction?: ProviderTransaction,
) {
  if (hasOneTimeSubscription(context)) {
    const message: JobMessage = {
      type: 'job',
      topic: context.metadata.jid,
      job: restoreHierarchy(jobOutput) as JobOutput,
    };
    await instance.subscribe.publish(
      KeyType.QUORUM,
      message,
      instance.appId,
      context.metadata.ngn,
      transaction,
    );
  }
}

async function publishPermanentSubscribers(
  instance: CompletionContext,
  context: JobState,
  jobOutput: JobOutput,
  emit = false,
  transaction?: ProviderTransaction,
) {
  const topic = await instance.getPublishesTopic(context);
  if (topic) {
    const message: JobMessage = {
      type: 'job',
      topic,
      job: restoreHierarchy(jobOutput) as JobOutput,
    };
    await instance.subscribe.publish(
      KeyType.QUORUM,
      message,
      instance.appId,
      `${topic}.${context.metadata.jid}`,
      transaction,
    );
  }
}
