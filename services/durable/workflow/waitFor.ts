import {
  sleepImmediate,
  DurableWaitForError,
  HMSH_CODE_DURABLE_WAIT,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Pauses the workflow until a signal with the given `signalId` is received.
 * The workflow suspends durably — it survives process restarts and will
 * resume exactly once when the matching `signal()` call delivers data.
 *
 * `waitFor` is the **receive** side of the signal coordination pair.
 * The **send** side is `signal()`, which can be called from another
 * workflow, a hook function, or externally via `Durable.Client.workflow.signal()`.
 *
 * On replay, `waitFor` returns the previously stored signal payload
 * immediately (no actual suspension occurs).
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Human-in-the-loop approval pattern
 * export async function approvalWorkflow(orderId: string): Promise<boolean> {
 *   const { submitForReview } = Durable.workflow.proxyActivities<typeof activities>();
 *
 *   await submitForReview(orderId);
 *
 *   // Pause indefinitely until a human approves or rejects
 *   const decision = await Durable.workflow.waitFor<{ approved: boolean }>('approval');
 *
 *   return decision.approved;
 * }
 *
 * // Later, from outside the workflow (e.g., an API handler):
 * await client.workflow.signal('approval', { approved: true });
 * ```
 *
 * ```typescript
 * // Fan-in: wait for multiple signals in parallel
 * export async function gatherWorkflow(): Promise<[string, number]> {
 *   const [name, score] = await Promise.all([
 *     Durable.workflow.waitFor<string>('name-signal'),
 *     Durable.workflow.waitFor<number>('score-signal'),
 *   ]);
 *   return [name, score];
 * }
 * ```
 *
 * ```typescript
 * // Paired with hook: spawn work and wait for the result
 * export async function orchestrator(input: string): Promise<string> {
 *   const signalId = `result-${Durable.workflow.random()}`;
 *
 *   // Spawn a hook that will signal back when done
 *   await Durable.workflow.hook({
 *     taskQueue: 'processors',
 *     workflowName: 'processItem',
 *     args: [input, signalId],
 *   });
 *
 *   // Wait for the hook to signal completion
 *   return await Durable.workflow.waitFor<string>(signalId);
 * }
 * ```
 *
 * @template T - The type of data expected in the signal payload.
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
    type: 'DurableWaitForError',
    code: HMSH_CODE_DURABLE_WAIT,
  };
  interruptionRegistry.push(interruptionMessage);

  await sleepImmediate();
  //if you are seeing this error in the logs, you might have forgotten to `await waitFor(...)`
  throw new DurableWaitForError(interruptionMessage);
}
