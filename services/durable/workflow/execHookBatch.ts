import { HookOptions } from './common';
import { hook } from './hook';
import { waitFor } from './waitFor';
import { ExecHookOptions } from './execHook';

/**
 * Configuration for a single hook in a batch execution.
 * @see {@link execHookBatch}
 */
export interface BatchHookConfig<T = any> {
  /** Unique key to identify this hook's result in the returned object */
  key: string;
  /** Hook execution options */
  options: ExecHookOptions;
}

/**
 * Executes multiple hooks in parallel and awaits all their signal responses,
 * returning a keyed object of results. This is the recommended way to run
 * concurrent hooks — it solves a race condition where calling
 * `Promise.all([execHook(), execHook()])` would throw before all `waitFor`
 * registrations complete.
 *
 * ## Three-Phase Execution
 *
 * 1. **Fire all hooks** via `Promise.all` (registers streams immediately).
 * 2. **Await all signals** via `Promise.all` (all `waitFor` registrations
 *    happen together before any `DurableWaitForError` is thrown).
 * 3. **Combine results** into a `{ [key]: result }` map.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Fan-out to multiple AI agents, gather all perspectives
 * export async function researchWorkflow(query: string): Promise<Summary> {
 *   const perspectives = await Durable.workflow.execHookBatch<{
 *     optimistic: PerspectiveResult;
 *     skeptical: PerspectiveResult;
 *     neutral: PerspectiveResult;
 *   }>([
 *     {
 *       key: 'optimistic',
 *       options: {
 *         taskQueue: 'agents',
 *         workflowName: 'analyzeOptimistic',
 *         args: [query],
 *       },
 *     },
 *     {
 *       key: 'skeptical',
 *       options: {
 *         taskQueue: 'agents',
 *         workflowName: 'analyzeSkeptical',
 *         args: [query],
 *       },
 *     },
 *     {
 *       key: 'neutral',
 *       options: {
 *         taskQueue: 'agents',
 *         workflowName: 'analyzeNeutral',
 *         args: [query],
 *       },
 *     },
 *   ]);
 *
 *   // All three results are available as typed properties
 *   const { synthesize } = Durable.workflow.proxyActivities<typeof activities>();
 *   return await synthesize(
 *     perspectives.optimistic,
 *     perspectives.skeptical,
 *     perspectives.neutral,
 *   );
 * }
 * ```
 *
 * ```typescript
 * // Parallel validation with different services
 * const checks = await Durable.workflow.execHookBatch<{
 *   fraud: { safe: boolean };
 *   compliance: { approved: boolean };
 * }>([
 *   {
 *     key: 'fraud',
 *     options: {
 *       taskQueue: 'fraud-detection',
 *       workflowName: 'checkFraud',
 *       args: [transactionId],
 *     },
 *   },
 *   {
 *     key: 'compliance',
 *     options: {
 *       taskQueue: 'compliance',
 *       workflowName: 'checkCompliance',
 *       args: [transactionId],
 *     },
 *   },
 * ]);
 *
 * if (checks.fraud.safe && checks.compliance.approved) {
 *   // proceed with transaction
 * }
 * ```
 *
 * @template T - Object type with keys matching the batch hook keys.
 * @param {BatchHookConfig[]} hookConfigs - Array of hook configurations with unique keys.
 * @returns {Promise<T>} Object mapping each config's `key` to its signal response.
 */
export async function execHookBatch<T extends Record<string, any>>(
  hookConfigs: BatchHookConfig[]
): Promise<T> {
  // Generate signal IDs for hooks that don't have them
  const processedConfigs = hookConfigs.map(config => ({
    ...config,
    options: {
      ...config.options,
      signalId: config.options.signalId || `durable-hook-${crypto.randomUUID()}`
    }
  }));

  // STEP 1: Fire off all hooks (but don't await them)
  // This registers the hooks/streams with the system immediately
  await Promise.all(
    processedConfigs.map(config => {
      const hookOptions: HookOptions = {
        ...config.options,
        args: [...config.options.args, { 
          signal: config.options.signalId, 
          $durable: true 
        }]
      };
      return hook(hookOptions);
    })
  );
  
  // STEP 2: Await all waitFor operations 
  // This ensures all waitFor registrations happen in the same call stack
  // before any DurableWaitForError is thrown (via setImmediate mechanism)
  const results = await Promise.all(
    processedConfigs.map(config => 
      waitFor<T[typeof config.key]>(config.options.signalId!)
    )
  );
  
  // STEP 3: Return results as a keyed object
  return Object.fromEntries(
    processedConfigs.map((config, i) => [config.key, results[i]])
  ) as T;
}
