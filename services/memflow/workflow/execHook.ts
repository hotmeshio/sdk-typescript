import { HookOptions } from './common';
import { hook } from './hook';
import { waitFor } from './waitFor';
import { didInterrupt } from './interruption';

/**
 * Extended hook options that include signal configuration.
 * Used by `execHook()` and `execHookBatch()`.
 */
export interface ExecHookOptions extends HookOptions {
  /** Signal ID to send after hook execution; if not provided, a random one will be generated */
  signalId?: string;
}

/**
 * Combines `hook()` + `waitFor()` into a single call: spawns a hook
 * function on a target workflow and suspends the current workflow until
 * the hook signals completion. This is the recommended pattern for
 * request/response communication between workflow threads.
 *
 * ## Signal Injection
 *
 * A `signalId` is automatically generated (or use the one you provide)
 * and injected as the **last argument** to the hooked function as
 * `{ signal: string, $memflow: true }`. The hook function must call
 * `MemFlow.workflow.signal(signalInfo.signal, result)` to deliver
 * its response back to the waiting workflow.
 *
 * ## Difference from `execChild`
 *
 * - `execChild` spawns a **new** workflow job with its own lifecycle.
 * - `execHook` runs within an **existing** workflow's job, in an
 *   isolated dimensional thread. This is lighter-weight and shares
 *   the parent job's data namespace.
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * // Orchestrator: spawn a hook and await its result
 * export async function reviewWorkflow(docId: string): Promise<string> {
 *   const verdict = await MemFlow.workflow.execHook<{ approved: boolean }>({
 *     taskQueue: 'reviewers',
 *     workflowName: 'reviewDocument',
 *     args: [docId],
 *   });
 *
 *   return verdict.approved ? 'accepted' : 'rejected';
 * }
 * ```
 *
 * ```typescript
 * // The hooked function (runs on the 'reviewers' worker)
 * export async function reviewDocument(
 *   docId: string,
 *   signalInfo?: { signal: string; $memflow: boolean },
 * ): Promise<{ approved: boolean }> {
 *   const { analyzeDocument } = MemFlow.workflow.proxyActivities<typeof activities>();
 *   const score = await analyzeDocument(docId);
 *   const result = { approved: score > 0.8 };
 *
 *   // Signal the waiting workflow with the result
 *   if (signalInfo?.signal) {
 *     await MemFlow.workflow.signal(signalInfo.signal, result);
 *   }
 *   return result;
 * }
 * ```
 *
 * ```typescript
 * // With explicit signalId for traceability
 * const result = await MemFlow.workflow.execHook<AnalysisResult>({
 *   taskQueue: 'analyzers',
 *   workflowName: 'runAnalysis',
 *   args: [datasetId],
 *   signalId: `analysis-${datasetId}`,
 * });
 * ```
 *
 * @template T - The type of data returned by the hook function's signal.
 * @param {ExecHookOptions} options - Hook configuration including target workflow and arguments.
 * @returns {Promise<T>} The signal result from the hooked function.
 */
export async function execHook<T>(options: ExecHookOptions): Promise<T> {
  try {
    if (!options.signalId) {
      options.signalId = 'memflow-hook-' + crypto.randomUUID();
    }
    const hookOptions: HookOptions = {
      ...options,
      args: [...options.args, { signal: options.signalId, $memflow: true }],
    };

    // Execute the hook with the signal information
    await hook(hookOptions);

    // Wait for the signal response and return it
    return await waitFor<T>(options.signalId);
  } catch (error) {
    if (didInterrupt(error)) {
      throw error;
    }
  }
}
