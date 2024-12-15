import {
  sleepImmediate,
  MeshFlowSleepError,
  HMSH_CODE_MESHFLOW_SLEEP,
  s,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Sleeps the workflow for a specified duration, deterministically.
 * On replay, it will not actually sleep again, but resume after sleep.
 *
 * @param {string} duration - A human-readable duration string (e.g., '1m', '2 hours').
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
    code: HMSH_CODE_MESHFLOW_SLEEP,
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new MeshFlowSleepError(interruptionMessage);
}
