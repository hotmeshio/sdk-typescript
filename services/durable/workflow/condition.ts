import {
  sleepImmediate,
  DurableWaitForError,
  DurableTelemetryService,
  HMSH_CODE_DURABLE_WAIT,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Pauses the workflow until a signal with the given `signalId` is received.
 * The workflow suspends durably — it survives process restarts and will
 * resume exactly once when the matching `signal()` call delivers data.
 *
 * `condition` is the **receive** side of the signal coordination pair.
 * The **send** side is `signal()`, which can be called from another
 * workflow, a hook function, or externally via `Durable.Client.workflow.signal()`.
 *
 * On replay, `condition` returns the previously stored signal payload
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
 *   const decision = await Durable.workflow.condition<{ approved: boolean }>('approval');
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
 *     Durable.workflow.condition<string>('name-signal'),
 *     Durable.workflow.condition<number>('score-signal'),
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
 *   return await Durable.workflow.condition<string>(signalId);
 * }
 * ```
 *
 * @template T - The type of data expected in the signal payload.
 * @param {string} signalId - A unique signal identifier shared by the sender and receiver.
 * @returns {Promise<T>} The data payload associated with the received signal.
 */
export async function condition<T>(signalId: string): Promise<T> {
  const [didRunAlready, execIndex, result] = await didRun('wait');
  if (didRunAlready) {
    // Emit RETURN span in debug mode
    if (DurableTelemetryService.isVerbose() && result) {
      const store = asyncLocalStorage.getStore();
      const workflowTrace = store.get('workflowTrace');
      const workflowSpan = store.get('workflowSpan');
      if (workflowTrace && workflowSpan && result.ac && result.au) {
        DurableTelemetryService.emitDurationSpan(
          workflowTrace,
          workflowSpan,
          `RETURN/wait/${signalId}/${execIndex}`,
          DurableTelemetryService.parseTimestamp(result.ac),
          DurableTelemetryService.parseTimestamp(result.au),
          {
            'durable.operation.type': 'wait',
            'durable.signal.id': signalId,
            'durable.exec.index': execIndex,
          },
        );
      }
    }
    return (result as { id: string; data: { data: T } }).data.data as T;
  }

  const store = asyncLocalStorage.getStore();

  // Emit DISPATCH span in debug mode
  if (DurableTelemetryService.isVerbose()) {
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    if (workflowTrace && workflowSpan) {
      DurableTelemetryService.emitPointSpan(
        workflowTrace,
        workflowSpan,
        `DISPATCH/wait/${signalId}/${execIndex}`,
        {
          'durable.operation.type': 'wait',
          'durable.signal.id': signalId,
          'durable.exec.index': execIndex,
        },
      );
    }
  }

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
  //if you are seeing this error in the logs, you might have forgotten to `await condition(...)`
  throw new DurableWaitForError(interruptionMessage);
}
