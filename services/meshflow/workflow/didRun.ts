import { SerializerService } from './common';
import { getContext } from './context';

/**
 * Determines if a replayed execution result already exists for the given prefix.
 * @private
 * @param {string} prefix - Identifier prefix (e.g., 'proxy', 'child', 'start', 'wait', etc.)
 * @returns {Promise<[boolean, number, any]>} A tuple: [alreadyRan, executionIndex, restoredData]
 */
export async function didRun(prefix: string): Promise<[boolean, number, any]> {
  const { COUNTER, replay, workflowDimension } = getContext();
  const execIndex = COUNTER.counter = COUNTER.counter + 1;
  const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
  if (sessionId in replay) {
    const restored = SerializerService.fromString(replay[sessionId]);
    return [true, execIndex, restored];
  }
  return [false, execIndex, null];
}
