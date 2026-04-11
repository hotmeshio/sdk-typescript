import { EngineService } from '../engine';
import { QuorumService } from '../quorum';
import { WorkerService } from '../worker';
import {
  QuorumMessage,
  QuorumMessageCallback,
  QuorumProfile,
  ThrottleMessage,
  ThrottleOptions,
} from '../../types/quorum';
import { MAX_DELAY } from '../../modules/enums';

interface QuorumContext {
  engine: EngineService | null;
  quorum: QuorumService | null;
  workers: WorkerService[];
}

/**
 * Broadcasts a roll call across the mesh and collects responses.
 */
export async function rollCall(
  instance: QuorumContext,
  delay?: number,
): Promise<QuorumProfile[]> {
  return await instance.quorum?.rollCall(delay);
}

/**
 * Broadcasts a throttle command to the mesh via the quorum channel.
 */
export async function throttle(
  instance: QuorumContext,
  options: ThrottleOptions,
): Promise<boolean> {
  let throttleValue: number;
  if (options.throttle === -1) {
    throttleValue = MAX_DELAY;
  } else {
    throttleValue = options.throttle;
  }
  if (
    !Number.isInteger(throttleValue) ||
    throttleValue < 0 ||
    throttleValue > MAX_DELAY
  ) {
    throw new Error(
      `Throttle must be a non-negative integer and not exceed ${MAX_DELAY} ms; send -1 to throttle indefinitely`,
    );
  }
  const throttleMessage: ThrottleMessage = {
    type: 'throttle',
    throttle: throttleValue,
  };
  if (options.guid) {
    throttleMessage.guid = options.guid;
  }
  if (options.topic !== undefined) {
    throttleMessage.topic = options.topic;
  }
  await instance.engine.store.setThrottleRate(throttleMessage);
  return await instance.quorum?.pub(throttleMessage);
}

/**
 * Publishes a message to every mesh member via the quorum channel.
 */
export async function pubQuorum(
  instance: QuorumContext,
  quorumMessage: QuorumMessage,
) {
  return await instance.quorum?.pub(quorumMessage);
}

/**
 * Subscribes to the quorum channel.
 */
export async function subQuorum(
  instance: QuorumContext,
  callback: QuorumMessageCallback,
): Promise<void> {
  return await instance.quorum?.sub(callback);
}

/**
 * Unsubscribes a callback previously registered with `subQuorum()`.
 */
export async function unsubQuorum(
  instance: QuorumContext,
  callback: QuorumMessageCallback,
): Promise<void> {
  return await instance.quorum?.unsub(callback);
}
