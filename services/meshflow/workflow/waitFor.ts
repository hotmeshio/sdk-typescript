import {
  sleepImmediate,
  MeshFlowWaitForError,
  HMSH_CODE_MESHFLOW_WAIT,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Pauses the workflow until a signal with the given `signalId` is received.
 *
 * @template T
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
  };
  interruptionRegistry.push({
    code: HMSH_CODE_MESHFLOW_WAIT,
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new MeshFlowWaitForError(interruptionMessage);
}
