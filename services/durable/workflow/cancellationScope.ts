import { asyncLocalStorage } from './common';

/**
 * Error thrown when a workflow detects a pending cancellation request.
 * Workflows can catch this error to perform cleanup before terminating.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * export async function myWorkflow(): Promise<string> {
 *   const acts = Durable.workflow.proxyActivities<typeof activities>({ ... });
 *
 *   try {
 *     await acts.longRunningTask();
 *   } catch (err) {
 *     if (Durable.workflow.isCancellation(err)) {
 *       await CancellationScope.nonCancellable(async () => {
 *         await acts.cleanup();
 *       });
 *       return 'cancelled-with-cleanup';
 *     }
 *     throw err;
 *   }
 *   return 'done';
 * }
 * ```
 */
export class CancelledFailure extends Error {
  type = 'CancelledFailure';
  constructor(message = 'Workflow cancelled') {
    super(message);
    this.name = 'CancelledFailure';
  }
}

/**
 * Type guard that returns `true` if the error is a {@link CancelledFailure},
 * indicating the workflow was cancelled via `handle.cancel()`.
 *
 * Use this inside `catch` blocks to distinguish cancellation from
 * application errors. Always check with `didInterrupt` first for
 * engine control-flow signals, then check `isCancellation` for
 * cooperative cancellation.
 *
 * @param {any} err - The error to check.
 * @returns {boolean} `true` if the error is a `CancelledFailure`.
 */
export function isCancellation(err: any): err is CancelledFailure {
  return err instanceof CancelledFailure;
}

/**
 * Controls how cancellation propagates through workflow code.
 *
 * When a workflow is cancelled via `handle.cancel()`, the next durable
 * operation (`sleep`, `proxyActivities`, `executeChild`, `condition`,
 * `continueAsNew`) throws a {@link CancelledFailure}. Use
 * `CancellationScope.nonCancellable(fn)` to shield cleanup code from
 * this behavior — durable operations inside the scope will execute
 * normally even when cancellation is pending.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * const { CancellationScope, isCancellation } = Durable.workflow;
 *
 * export async function orderWorkflow(orderId: string): Promise<string> {
 *   const acts = Durable.workflow.proxyActivities<typeof activities>({ ... });
 *
 *   try {
 *     await acts.chargePayment(orderId);
 *     await Durable.workflow.sleep('7 days');
 *     await acts.shipOrder(orderId);
 *   } catch (err) {
 *     if (isCancellation(err)) {
 *       // Shield cleanup from further cancellation
 *       await CancellationScope.nonCancellable(async () => {
 *         await acts.refundPayment(orderId);
 *         await acts.notifyCustomer(orderId, 'Order cancelled');
 *       });
 *       return 'cancelled';
 *     }
 *     throw err;
 *   }
 *   return 'shipped';
 * }
 * ```
 */
export class CancellationScope {
  /**
   * Execute `fn` with cancellation suppressed. Durable operations
   * inside the callback will not throw `CancelledFailure` even if
   * the workflow has a pending cancellation request. Use this for
   * cleanup logic (refunds, notifications, audit logs) that must
   * complete regardless of cancellation state.
   *
   * @param fn - The async function to execute without cancellation.
   * @returns The return value of `fn`.
   */
  static async nonCancellable<T>(fn: () => Promise<T>): Promise<T> {
    const store = asyncLocalStorage.getStore();
    const prev = store.get('nonCancellable');
    store.set('nonCancellable', true);
    try {
      return await fn();
    } finally {
      store.set('nonCancellable', prev ?? false);
    }
  }
}

/**
 * Checks for a pending cancellation request and throws
 * `CancelledFailure` if one exists and we are not inside
 * a `CancellationScope.nonCancellable()` block.
 *
 * Called at the entry of every durable operation.
 * @private
 */
export function checkCancellation(): void {
  const store = asyncLocalStorage.getStore();
  if (!store) return;
  const replay = store.get('replay');
  if (!replay) return;
  const workflowDimension = store.get('workflowDimension') ?? '';
  const cancelKey = `-cancelled${workflowDimension}-`;
  if (cancelKey in replay && !store.get('nonCancellable')) {
    throw new CancelledFailure();
  }
}
