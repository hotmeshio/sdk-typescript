import { asyncLocalStorage, WorkerService } from './common';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Sends a signal payload to a paused workflow thread that is awaiting this
 * `signalId` via `waitFor()`. Signals are the primary mechanism for
 * inter-workflow communication and for delivering results from hook
 * functions back to the orchestrating workflow.
 *
 * `signal` is the **send** side of the coordination pair. The **receive**
 * side is `waitFor()`. A signal can be sent from:
 * - Another workflow function
 * - A hook function (most common pattern with `execHook`)
 * - An external client via `MemFlow.Client.workflow.signal()`
 *
 * Signals fire exactly once per workflow execution — the `isSideEffectAllowed`
 * guard ensures they are not re-sent on replay.
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * // Hook function that signals completion back to the parent workflow
 * export async function processOrder(
 *   orderId: string,
 *   signalInfo?: { signal: string; $memflow: boolean },
 * ): Promise<{ total: number }> {
 *   const { calculateTotal } = MemFlow.workflow.proxyActivities<typeof activities>();
 *   const total = await calculateTotal(orderId);
 *
 *   // Signal the waiting workflow with the result
 *   if (signalInfo?.signal) {
 *     await MemFlow.workflow.signal(signalInfo.signal, { total });
 *   }
 *   return { total };
 * }
 * ```
 *
 * ```typescript
 * // Cross-workflow coordination: workflow A signals workflow B
 * export async function coordinatorWorkflow(): Promise<void> {
 *   const { prepareData } = MemFlow.workflow.proxyActivities<typeof activities>();
 *   const data = await prepareData();
 *
 *   // Signal another workflow that is paused on waitFor('data-ready')
 *   await MemFlow.workflow.signal('data-ready', { payload: data });
 * }
 * ```
 *
 * ```typescript
 * // External signal from an API handler (outside a workflow)
 * const client = new MemFlow.Client({ connection });
 * await client.workflow.signal('approval-signal', { approved: true });
 * ```
 *
 * @param {string} signalId - Unique signal identifier that matches a `waitFor()` call.
 * @param {Record<any, any>} data - The payload to deliver to the waiting workflow.
 * @returns {Promise<string>} The resulting hook/stream ID.
 */
export async function signal(
  signalId: string,
  data: Record<any, any>,
): Promise<string> {
  const store = asyncLocalStorage.getStore();
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  if (await isSideEffectAllowed(hotMeshClient, 'signal')) {
    return await hotMeshClient.signal(`${namespace}.wfs.signal`, {
      id: signalId,
      data,
    });
  }
}
