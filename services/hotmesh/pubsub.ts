import { EngineService } from '../engine';
import {
  JobState,
  JobData,
  JobOutput,
  ExtensionType,
} from '../../types/job';
import { JobMessageCallback } from '../../types/quorum';
import { StreamData, StreamDataResponse } from '../../types/stream';

interface PubSubContext {
  engine: EngineService | null;
}

/**
 * Publishes a message to a workflow topic, starting a new job.
 * Returns the job ID immediately (fire-and-forget).
 */
export async function pub(
  instance: PubSubContext,
  topic: string,
  data: JobData = {},
  context?: JobState,
  extended?: ExtensionType,
): Promise<string> {
  return await instance.engine?.pub(topic, data, context, extended);
}

/**
 * Subscribes to all output and interim emissions from a specific workflow topic.
 */
export async function sub(
  instance: PubSubContext,
  topic: string,
  callback: JobMessageCallback,
): Promise<void> {
  return await instance.engine?.sub(topic, callback);
}

/**
 * Unsubscribes from a single workflow topic previously registered with `sub()`.
 */
export async function unsub(
  instance: PubSubContext,
  topic: string,
): Promise<void> {
  return await instance.engine?.unsub(topic);
}

/**
 * Subscribes to workflow emissions matching a wildcard pattern.
 */
export async function psub(
  instance: PubSubContext,
  wild: string,
  callback: JobMessageCallback,
): Promise<void> {
  return await instance.engine?.psub(wild, callback);
}

/**
 * Unsubscribes from a wildcard pattern previously registered with `psub()`.
 */
export async function punsub(
  instance: PubSubContext,
  wild: string,
): Promise<void> {
  return await instance.engine?.punsub(wild);
}

/**
 * Publishes a message to a workflow topic and blocks until the workflow completes.
 */
export async function pubsub(
  instance: PubSubContext,
  topic: string,
  data: JobData = {},
  context?: JobState | null,
  timeout?: number,
): Promise<JobOutput> {
  return await instance.engine?.pubsub(topic, data, context, timeout);
}

/**
 * Adds a transition message to the workstream, resuming Leg 2 of a paused
 * reentrant activity.
 */
export async function add(
  instance: PubSubContext,
  streamData: StreamData | StreamDataResponse,
): Promise<string> {
  return (await instance.engine.add(streamData)) as string;
}
