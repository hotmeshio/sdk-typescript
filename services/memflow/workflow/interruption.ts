import {
  MemFlowChildError,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowProxyError,
  MemFlowRetryError,
  MemFlowSleepError,
  MemFlowTimeoutError,
  MemFlowWaitForError,
  MemFlowWaitForAllError,
} from '../../../modules/errors';

/**
 * Type guard that returns `true` if an error is a MemFlow engine
 * control-flow signal rather than a genuine application error.
 *
 * MemFlow uses thrown errors internally to suspend workflow execution
 * for durable operations like `sleepFor`, `waitFor`, `proxyActivities`,
 * and `execChild`. These errors must be re-thrown (not swallowed) so
 * the engine can persist state and schedule the next step.
 *
 * **Always use `didInterrupt` in `catch` blocks inside workflow
 * functions** to avoid accidentally swallowing engine signals.
 *
 * ## Recognized Error Types
 *
 * `MemFlowChildError`, `MemFlowFatalError`, `MemFlowMaxedError`,
 * `MemFlowProxyError`, `MemFlowRetryError`, `MemFlowSleepError`,
 * `MemFlowTimeoutError`, `MemFlowWaitForError`, `MemFlowWaitForAllError`
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * export async function safeWorkflow(): Promise<string> {
 *   const { riskyOperation } = MemFlow.workflow.proxyActivities<typeof activities>();
 *
 *   try {
 *     return await riskyOperation();
 *   } catch (error) {
 *     // CRITICAL: re-throw engine signals
 *     if (MemFlow.workflow.didInterrupt(error)) {
 *       throw error;
 *     }
 *     // Handle real application errors
 *     return 'fallback-value';
 *   }
 * }
 * ```
 *
 * ```typescript
 * // Common pattern in interceptors
 * const interceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     try {
 *       return await next();
 *     } catch (error) {
 *       if (MemFlow.workflow.didInterrupt(error)) {
 *         throw error;  // always re-throw engine signals
 *       }
 *       // Log and re-throw application errors
 *       console.error('Workflow failed:', error);
 *       throw error;
 *     }
 *   },
 * };
 * ```
 *
 * @param {Error} error - The error to check.
 * @returns {boolean} `true` if the error is a MemFlow engine interruption signal.
 */
export function didInterrupt(error: Error): boolean {
  return (
    error instanceof MemFlowChildError ||
    error instanceof MemFlowFatalError ||
    error instanceof MemFlowMaxedError ||
    error instanceof MemFlowProxyError ||
    error instanceof MemFlowRetryError ||
    error instanceof MemFlowSleepError ||
    error instanceof MemFlowTimeoutError ||
    error instanceof MemFlowWaitForError ||
    error instanceof MemFlowWaitForAllError
  );
}
