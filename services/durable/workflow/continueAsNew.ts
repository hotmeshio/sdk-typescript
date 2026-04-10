import {
  DurableContinueAsNewError,
  sleepImmediate,
  asyncLocalStorage,
  HMSH_CODE_DURABLE_CONTINUE,
} from './common';

/**
 * Completes the current workflow execution and immediately starts a new
 * execution of the same workflow with the provided arguments. The execution
 * history is reset, preventing unbounded state growth in long-running
 * workflows.
 *
 * `continueAsNew` never returns — it always throws an internal interruption
 * error that the engine catches to orchestrate the restart. Any code after
 * the call is unreachable.
 *
 * ## When to Use
 *
 * Use `continueAsNew` for workflows that would otherwise run indefinitely
 * and accumulate unbounded replay history:
 * - Polling loops
 * - Subscription renewals
 * - Iterative processing with cursors
 * - Long-running agents with periodic state resets
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Cursor-based batch processor
 * export async function batchProcessor(cursor: string, totalProcessed = 0): Promise<void> {
 *   const { fetchBatch, processBatch } = Durable.workflow.proxyActivities<typeof activities>({
 *     activities,
 *     retryPolicy: { maximumAttempts: 3 },
 *   });
 *
 *   const batch = await fetchBatch(cursor);
 *   await processBatch(batch.items);
 *
 *   if (batch.nextCursor) {
 *     // Restart with fresh history, carrying forward the cursor
 *     await Durable.workflow.continueAsNew(batch.nextCursor, totalProcessed + batch.items.length);
 *   }
 *   // No more items — workflow completes naturally
 * }
 * ```
 *
 * ```typescript
 * // Periodic polling workflow
 * export async function poller(resourceId: string, attempt = 0): Promise<string> {
 *   const { checkStatus } = Durable.workflow.proxyActivities<typeof activities>({
 *     activities,
 *     retryPolicy: { maximumAttempts: 3 },
 *   });
 *
 *   const status = await checkStatus(resourceId);
 *   if (status === 'ready') return status;
 *
 *   await Durable.workflow.sleep('30 seconds');
 *
 *   // Reset history on each poll iteration
 *   await Durable.workflow.continueAsNew(resourceId, attempt + 1);
 * }
 * ```
 *
 * @param {...any[]} args - The arguments to pass to the new workflow execution.
 *   These become the workflow function's parameters on restart.
 * @returns {Promise<never>} Never returns — always throws an internal interruption.
 */
export async function continueAsNew(...args: any[]): Promise<never> {
  const store = asyncLocalStorage.getStore();
  const interruptionRegistry = store.get('interruptionRegistry');
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const counter = store.get('counter');
  const execIndex = (counter.counter = counter.counter + 1);

  const payload = {
    arguments: args,
    index: execIndex,
    workflowDimension,
    workflowId,
  };

  interruptionRegistry.push({
    code: HMSH_CODE_DURABLE_CONTINUE,
    type: 'DurableContinueAsNewError',
    ...payload,
  });

  await sleepImmediate();
  throw new DurableContinueAsNewError(payload);
}
