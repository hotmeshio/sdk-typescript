import { asyncLocalStorage, deterministicRandom } from './common';

/**
 * Returns a deterministic pseudo-random number between 0 and 1. The value
 * is derived from the current execution counter, so it produces the
 * **same result** on every replay of the workflow at this execution point.
 *
 * Use this instead of `Math.random()` inside workflow functions.
 * `Math.random()` is non-deterministic and would break replay correctness.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Generate a deterministic unique suffix
 * export async function uniqueWorkflow(): Promise<string> {
 *   const suffix = Math.floor(Durable.workflow.random() * 10000);
 *   return `item-${suffix}`;
 * }
 * ```
 *
 * ```typescript
 * // A/B test routing (deterministic per workflow execution)
 * export async function experimentWorkflow(userId: string): Promise<string> {
 *   const { variantA, variantB } = Durable.workflow.proxyActivities<typeof activities>();
 *
 *   if (Durable.workflow.random() < 0.5) {
 *     return await variantA(userId);
 *   } else {
 *     return await variantB(userId);
 *   }
 * }
 * ```
 *
 * @returns {number} A deterministic pseudo-random number in [0, 1).
 */
export function random(): number {
  const store = asyncLocalStorage.getStore();
  const COUNTER = store.get('counter');
  const seed = COUNTER.counter = COUNTER.counter + 1;
  return deterministicRandom(seed);
}
