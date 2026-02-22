import { asyncLocalStorage, WorkerService, Search } from './common';

/**
 * Returns a `Search` session handle for reading and writing key-value data
 * on the workflow's backend HASH record. Search fields are flat string
 * key-value pairs stored alongside the job state, making them queryable
 * via `Durable.Client.workflow.search()` (FT.SEARCH).
 *
 * Each call produces a unique session ID tied to the deterministic
 * execution counter, ensuring correct replay behavior.
 *
 * Use `search()` for flat, indexable key-value data. For structured
 * JSON documents, use `entity()` instead.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * export async function orderWorkflow(orderId: string): Promise<void> {
 *   const search = await Durable.workflow.search();
 *
 *   // Write searchable fields
 *   await search.set({
 *     orderId,
 *     status: 'processing',
 *     createdAt: new Date().toISOString(),
 *   });
 *
 *   const { processOrder } = Durable.workflow.proxyActivities<typeof activities>();
 *   await processOrder(orderId);
 *
 *   // Update status
 *   await search.set({ status: 'completed' });
 *
 *   // Read a field back
 *   const status = await search.get('status');
 * }
 * ```
 *
 * ```typescript
 * // Increment a numeric counter
 * export async function counterWorkflow(): Promise<number> {
 *   const search = await Durable.workflow.search();
 *   await search.set({ count: '0' });
 *   await search.incr('count', 1);
 *   await search.incr('count', 1);
 *   return Number(await search.get('count')); // 2
 * }
 * ```
 *
 * @returns {Promise<Search>} A search session scoped to the current workflow job.
 */
export async function search(): Promise<Search> {
  const store = asyncLocalStorage.getStore();
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const COUNTER = store.get('counter');
  const execIndex = COUNTER.counter = COUNTER.counter + 1;
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  const searchSessionId = `-search${workflowDimension}-${execIndex}`;
  return new Search(workflowId, hotMeshClient, searchSessionId);
}
