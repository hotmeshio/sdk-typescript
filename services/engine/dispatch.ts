/**
 * Stream message routing — the engine's main re-entry point.
 *
 * Every message consumed from the engine stream arrives here.
 * The dispatcher inspects the message type and delegates to the
 * appropriate activity handler (Hook, Trigger, Await, Worker).
 *
 * Message types:
 *   TIMEHOOK   → Hook.processTimeHookEvent   (sleep/cron awaken)
 *   WEBHOOK    → Hook.processWebHookEvent     (signal-in awaken)
 *   TRANSITION → Activity.process             (adjacent activity)
 *   AWAIT      → Trigger.process              (child job spawn)
 *   RESULT     → Await.processEvent           (child job result)
 *   (default)  → Worker.processEvent          (worker response)
 */

import { Hook } from '../activities/hook';
import { Trigger } from '../activities/trigger';
import { Await } from '../activities/await';
import { Worker } from '../activities/worker';
import { ILogger } from '../logger';
import { JobState, JobData, PartialJobState } from '../../types/job';
import {
  StreamDataResponse,
  StreamDataType,
  StreamStatus,
} from '../../types/stream';

interface DispatchContext {
  logger: ILogger;
  initActivity(
    topic: string,
    data?: JobData,
    context?: JobState,
  ): Promise<any>;
}

export async function processStreamMessage(
  instance: DispatchContext,
  streamData: StreamDataResponse,
): Promise<void> {
  instance.logger.debug('engine-process', {
    jid: streamData.metadata.jid,
    gid: streamData.metadata.gid,
    dad: streamData.metadata.dad,
    aid: streamData.metadata.aid,
    status: streamData.status || StreamStatus.SUCCESS,
    code: streamData.code || 200,
    type: streamData.type,
  });

  const context: PartialJobState = {
    metadata: {
      guid: streamData.metadata.guid,
      jid: streamData.metadata.jid,
      gid: streamData.metadata.gid,
      dad: streamData.metadata.dad,
      aid: streamData.metadata.aid,
    },
    data: streamData.data,
  };

  if (streamData.type === StreamDataType.TIMEHOOK) {
    await dispatchTimeHook(instance, streamData, context);
  } else if (streamData.type === StreamDataType.WEBHOOK) {
    await dispatchWebHook(instance, streamData, context);
  } else if (streamData.type === StreamDataType.TRANSITION) {
    await dispatchTransition(instance, streamData, context);
  } else if (streamData.type === StreamDataType.AWAIT) {
    await dispatchAwait(instance, streamData, context);
  } else if (streamData.type === StreamDataType.RESULT) {
    await dispatchResult(instance, streamData, context);
  } else {
    await dispatchWorkerResult(instance, streamData, context);
  }

  instance.logger.debug('engine-process-end', {
    jid: streamData.metadata.jid,
    gid: streamData.metadata.gid,
    aid: streamData.metadata.aid,
  });
}

async function dispatchTimeHook(
  instance: DispatchContext,
  streamData: StreamDataResponse,
  context: PartialJobState,
) {
  const handler = (await instance.initActivity(
    `.${streamData.metadata.aid}`,
    context.data,
    context as JobState,
  )) as Hook;
  await handler.processTimeHookEvent(streamData.metadata.jid);
}

async function dispatchWebHook(
  instance: DispatchContext,
  streamData: StreamDataResponse,
  context: PartialJobState,
) {
  const handler = (await instance.initActivity(
    `.${streamData.metadata.aid}`,
    context.data,
    context as JobState,
  )) as Hook;
  await handler.processWebHookEvent(streamData.status, streamData.code);
}

async function dispatchTransition(
  instance: DispatchContext,
  streamData: StreamDataResponse,
  context: PartialJobState,
) {
  const handler = await instance.initActivity(
    `.${streamData.metadata.aid}`,
    context.data,
    context as JobState,
  );
  await handler.process();
}

async function dispatchAwait(
  instance: DispatchContext,
  streamData: StreamDataResponse,
  context: PartialJobState,
) {
  context.metadata = {
    ...context.metadata,
    pj: streamData.metadata.jid,
    pg: streamData.metadata.gid,
    pd: streamData.metadata.dad,
    pa: streamData.metadata.aid,
    px: streamData.metadata.await === false,
    trc: streamData.metadata.trc,
    spn: streamData.metadata.spn,
  };
  const handler = (await instance.initActivity(
    streamData.metadata.topic,
    streamData.data,
    context as JobState,
  )) as Trigger;
  await handler.process();
}

async function dispatchResult(
  instance: DispatchContext,
  streamData: StreamDataResponse,
  context: PartialJobState,
) {
  const handler = (await instance.initActivity(
    `.${context.metadata.aid}`,
    streamData.data,
    context as JobState,
  )) as Await;
  await handler.processEvent(streamData.status, streamData.code);
}

async function dispatchWorkerResult(
  instance: DispatchContext,
  streamData: StreamDataResponse,
  context: PartialJobState,
) {
  const handler = (await instance.initActivity(
    `.${streamData.metadata.aid}`,
    streamData.data,
    context as JobState,
  )) as Worker;
  await handler.processEvent(streamData.status, streamData.code, 'output');
}
