import {
  sleepImmediate,
  MemFlowSleepError,
  HMSH_CODE_MEMFLOW_SLEEP,
  s,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Sleeps the workflow for a specified duration, deterministically.
 * On replay, it will not actually sleep again, but resume after sleep.
 * 
 * @example
 * // Basic usage - sleep for a specific duration
 * await MemFlow.workflow.sleepFor('2 seconds');
 * 
 * @example
 * // Using with Promise.all for parallel operations
 * const [greeting, timeInSeconds] = await Promise.all([
 *   someActivity(name),
 *   MemFlow.workflow.sleepFor('1 second')
 * ]);
 * 
 * @example
 * // Multiple sequential sleeps
 * await MemFlow.workflow.sleepFor('1 seconds');  // First pause
 * await MemFlow.workflow.sleepFor('2 seconds');  // Second pause
 * 
 * @param {string} duration - A human-readable duration string (e.g., '1m', '2 hours', '30 seconds').
 * @returns {Promise<number>} The resolved duration in seconds.
 */
export async function sleepFor(duration: string): Promise<number> {
  const [didRunAlready, execIndex, result] = await didRun('sleep');
  if (didRunAlready) {
    return (result as { completion: string; duration: number }).duration;
  }
  const store = asyncLocalStorage.getStore();
  const interruptionRegistry = store.get('interruptionRegistry');
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const interruptionMessage = {
    workflowId,
    duration: s(duration),
    index: execIndex,
    workflowDimension,
  };
  interruptionRegistry.push({
    code: HMSH_CODE_MEMFLOW_SLEEP,
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new MemFlowSleepError(interruptionMessage);
}
