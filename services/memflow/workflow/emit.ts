import {
  asyncLocalStorage,
  WorkerService,
  StringAnyType,
  UserMessage,
} from './common';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Emits pub/sub events to the event bus, allowing workflows to broadcast
 * messages to external subscribers. Each entry in the `events` map is
 * published as a separate message on the corresponding topic.
 *
 * By default (`config.once = true`), events are emitted exactly once per
 * workflow execution — the `isSideEffectAllowed` guard prevents re-emission
 * on replay. Set `config.once = false` to emit on every re-execution
 * (rarely needed).
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * // Emit a domain event when an order is processed
 * export async function orderWorkflow(orderId: string): Promise<void> {
 *   const { processOrder } = MemFlow.workflow.proxyActivities<typeof activities>();
 *   const result = await processOrder(orderId);
 *
 *   await MemFlow.workflow.emit({
 *     'order.completed': { orderId, total: result.total },
 *     'analytics.event': { type: 'order', orderId },
 *   });
 * }
 * ```
 *
 * ```typescript
 * // Emit progress events during a long-running workflow
 * export async function batchWorkflow(items: string[]): Promise<void> {
 *   const { processItem } = MemFlow.workflow.proxyActivities<typeof activities>();
 *
 *   for (let i = 0; i < items.length; i++) {
 *     await processItem(items[i]);
 *     await MemFlow.workflow.emit(
 *       { 'batch.progress': { completed: i + 1, total: items.length } },
 *       { once: false },  // emit on every execution (progress updates)
 *     );
 *   }
 * }
 * ```
 *
 * @param {StringAnyType} events - A mapping of `topic` to message payload.
 * @param {{ once: boolean }} [config={ once: true }] - If `true`, events emit only once (idempotent).
 * @returns {Promise<boolean>} `true` after emission completes.
 */
export async function emit(
  events: StringAnyType,
  config: { once: boolean } = { once: true },
): Promise<boolean> {
  const store = asyncLocalStorage.getStore();
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });

  if (!config.once || await isSideEffectAllowed(hotMeshClient, 'emit')) {
    for (const [topic, message] of Object.entries(events)) {
      await hotMeshClient.quorum.pub({ topic, message } as UserMessage);
    }
  }
  return true;
}
