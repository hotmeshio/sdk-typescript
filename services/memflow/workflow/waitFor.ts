import {
  sleepImmediate,
  MemFlowWaitForError,
  HMSH_CODE_MEMFLOW_WAIT,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Pauses the workflow until a signal with the given `signalId` is received.
 * This method is commonly used to coordinate between the main workflow and hook functions,
 * or to wait for external events.
 * 
 * @example
 * // Basic usage - wait for a single signal
 * const payload = await MemFlow.workflow.waitFor<PayloadType>('abcdefg');
 * 
 * @example
 * // Wait for multiple signals in parallel
 * const [signal1, signal2] = await Promise.all([
 *   MemFlow.workflow.waitFor<Record<string, any>>('signal1'),
 *   MemFlow.workflow.waitFor<Record<string, any>>('signal2')
 * ]);
 * 
 * @example
 * // Typical pattern with hook functions
 * // In main workflow:
 * await MemFlow.workflow.waitFor<ResponseType>('hook-complete');
 * 
 * // In hook function:
 * await MemFlow.workflow.signal('hook-complete', { data: result });
 * 
 * @template T - The type of data expected in the signal payload
 * @param {string} signalId - A unique signal identifier shared by the sender and receiver.
 * @returns {Promise<T>} The data payload associated with the received signal.
 */
export async function waitFor<T>(signalId: string): Promise<T> {
  const [didRunAlready, execIndex, result] = await didRun('wait');
  if (didRunAlready) {
    return (result as { id: string; data: { data: T } }).data.data as T;
  }

  const store = asyncLocalStorage.getStore();
  const interruptionRegistry = store.get('interruptionRegistry');
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const interruptionMessage = {
    workflowId,
    signalId,
    index: execIndex,
    workflowDimension,
    type: 'MemFlowWaitForError',
    code: HMSH_CODE_MEMFLOW_WAIT,
  };
  interruptionRegistry.push(interruptionMessage);

  await sleepImmediate();
  throw new MemFlowWaitForError(interruptionMessage);
}
