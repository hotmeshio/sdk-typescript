import { HookOptions } from './common';
import { hook } from './hook';
import { waitFor } from './waitFor';
import { ExecHookOptions } from './execHook';

/**
 * Configuration for a single hook in a batch execution
 */
export interface BatchHookConfig<T = any> {
  /** Unique key to identify this hook's result in the returned object */
  key: string;
  /** Hook execution options */
  options: ExecHookOptions;
}

/**
 * Executes multiple hooks in parallel and awaits all their signal responses.
 * This solves the race condition where Promise.all() with execHook() would prevent
 * all waitFor() registrations from completing.
 * 
 * The method ensures all waitFor() registrations happen before any hooks execute,
 * preventing signals from being sent before the framework is ready to receive them.
 *
 * @template T - Object type with keys matching the batch hook keys and values as expected response types
 * @param {BatchHookConfig[]} hookConfigs - Array of hook configurations with unique keys
 * @returns {Promise<T>} Object with keys from hookConfigs and values as the signal responses
 *
 * @example
 * ```typescript
 * // Execute multiple research perspectives in parallel
 * const results = await MemFlow.workflow.execHookBatch<{
 *   optimistic: OptimisticResult;
 *   skeptical: SkepticalResult;
 * }>([
 *   {
 *     key: 'optimistic',
 *     options: {
 *       taskQueue: 'agents',
 *       workflowName: 'optimisticPerspective',
 *       args: [query],
 *       signalId: 'optimistic-complete'
 *     }
 *   },
 *   {
 *     key: 'skeptical', 
 *     options: {
 *       taskQueue: 'agents',
 *       workflowName: 'skepticalPerspective',
 *       args: [query],
 *       signalId: 'skeptical-complete'
 *     }
 *   }
 * ]);
 * 
 * // results.optimistic contains the OptimisticResult
 * // results.skeptical contains the SkepticalResult
 * ```
 */
export async function execHookBatch<T extends Record<string, any>>(
  hookConfigs: BatchHookConfig[]
): Promise<T> {
  // Generate signal IDs for hooks that don't have them
  const processedConfigs = hookConfigs.map(config => ({
    ...config,
    options: {
      ...config.options,
      signalId: config.options.signalId || `memflow-hook-${crypto.randomUUID()}`
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
          $memflow: true 
        }]
      };
      return hook(hookOptions);
    })
  );
  
  // STEP 2: Await all waitFor operations 
  // This ensures all waitFor registrations happen in the same call stack
  // before any MemFlowWaitForError is thrown (via setImmediate mechanism)
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
