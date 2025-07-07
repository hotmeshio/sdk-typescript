import { HookOptions } from './common';
import { hook } from './hook';
import { waitFor } from './waitFor';

/**
 * Extended hook options that include signal configuration
 */
export interface ExecHookOptions extends HookOptions {
  /** Signal ID to send after hook execution */
  signalId: string;
}

/**
 * Executes a hook function and awaits the signal response.
 * This is a convenience method that combines `hook()` and `waitFor()` operations.
 * 
 * **Signal Injection**: The `signalId` is automatically injected as the LAST argument
 * to the hooked function. The hooked function should check for this signal parameter
 * and emit the signal when processing is complete.
 * 
 * This behaves like `execChild` but targets the existing workflow instead of
 * spawning a new workflow.
 * 
 * @template T
 * @param {ExecHookOptions} options - Hook configuration with signal ID.
 * @returns {Promise<T>} The signal result from the hooked function.
 * 
 * @example
 * ```typescript
 * // Execute a hook and await its signal response
 * const result = await MemFlow.workflow.execHook({
 *   taskQueue: 'processing',
 *   workflowName: 'processData',
 *   args: ['user123', 'batch-process'],
 *   signalId: 'processing-complete'
 * });
 * 
 * // The hooked function receives the signal as the last argument:
 * export async function processData(userId: string, processType: string, signalInfo?: { signal: string }) {
 *   // ... do processing work ...
 *   const result = { userId, processType, status: 'completed' };
 *   
 *   // Check if called via execHook (signalInfo will be present)
 *   if (signalInfo?.signal) {
 *     await MemFlow.workflow.signal(signalInfo.signal, result);
 *   }
 *   
 *   return result;
 * }
 * ```
 * 
 * @example
 * ```typescript
 * // Alternative pattern - check if last arg is signal object
 * export async function myHookFunction(arg1: string, arg2: number, ...rest: any[]) {
 *   // ... process arg1 and arg2 ...
 *   const result = { processed: true, data: [arg1, arg2] };
 *   
 *   // Check if last argument is a signal object
 *   const lastArg = rest[rest.length - 1];
 *   if (lastArg && typeof lastArg === 'object' && lastArg.signal) {
 *     await MemFlow.workflow.signal(lastArg.signal, result);
 *   }
 *   
 *   return result;
 * }
 * ```
 */
export async function execHook<T>(options: ExecHookOptions): Promise<T> {
  // Create hook options with signal field added to args
  const hookOptions: HookOptions = {
    ...options,
    args: [...options.args, { signal: options.signalId }]
  };

  // Execute the hook with the signal information
  await hook(hookOptions);

  // Wait for the signal response and return it
  return await waitFor<T>(options.signalId);
} 