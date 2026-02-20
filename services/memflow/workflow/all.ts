/**
 * A workflow-safe version of `Promise.all` that applies a micro-delay
 * before parallel execution to ensure correct sequencing of the
 * deterministic execution counter. Use this when you need to run
 * multiple durable operations concurrently within a workflow function.
 *
 * In most cases, standard `Promise.all` works correctly for MemFlow
 * operations (e.g., parallel `waitFor` calls). Use `MemFlow.workflow.all`
 * when you observe counter-sequencing issues with complex parallel
 * patterns.
 *
 * ## Example
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * export async function parallelWorkflow(): Promise<[string, number]> {
 *   const { fetchName, fetchScore } = MemFlow.workflow.proxyActivities<typeof activities>();
 *
 *   const [name, score] = await MemFlow.workflow.all(
 *     fetchName('user-1'),
 *     fetchScore('user-1'),
 *   );
 *
 *   return [name, score];
 * }
 * ```
 *
 * @param {...Promise<T>[]} promises - An array of promises to execute concurrently.
 * @returns {Promise<T[]>} A promise resolving to an array of results.
 */
export async function all<T>(...promises: Promise<T>[]): Promise<T[]> {
  // a micro-delay to ensure correct sequencing
  await new Promise((resolve) => setTimeout(resolve, 1));
  return await Promise.all(promises);
}
