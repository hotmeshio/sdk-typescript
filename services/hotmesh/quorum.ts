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
  const throttleValue = options.throttle;
  if (
    !Number.isInteger(throttleValue) ||
    (throttleValue < -1) ||
    (throttleValue !== -1 && throttleValue < 0)
  ) {
    throw new Error(
      'Throttle must be -1 (pause) or a non-negative integer (ms delay); 0 = resume',
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
