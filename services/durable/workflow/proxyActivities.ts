import {
  WorkerService,
  ActivityConfig,
  ProxyType,
  sleepImmediate,
  DurableFatalError,
  DurableMaxedError,
  DurableTimeoutError,
  DurableProxyError,
  HMSH_CODE_DURABLE_FATAL,
  HMSH_CODE_DURABLE_MAXED,
  HMSH_CODE_DURABLE_TIMEOUT,
  HMSH_CODE_DURABLE_PROXY,
  DurableProxyErrorType,
  s,
  asyncLocalStorage,
} from './common';
import { getContext } from './context';
import { didRun } from './didRun';

/**
 * Constructs payload for spawning a proxyActivity job.
 * @private
 */
function getProxyInterruptPayload(
  context: ReturnType<typeof getContext>,
  activityName: string,
  execIndex: number,
  args: any[],
  options?: ActivityConfig,
): DurableProxyErrorType {
  const { workflowDimension, workflowId, originJobId, workflowTopic, expire } =
    context;
  
  // Use explicitly provided taskQueue, otherwise derive from workflow (original behavior)
  // This keeps backward compatibility while allowing explicit global/custom queues
  const activityTopic = options?.taskQueue 
    ? `${options.taskQueue}-activity`
    : `${workflowTopic}-activity`;
    
  const activityJobId = `-${workflowId}-$${activityName}${workflowDimension}-${execIndex}`;
  let maximumInterval: number;
  if (options?.retryPolicy?.maximumInterval) {
    maximumInterval = s(options.retryPolicy.maximumInterval);
  }
  return {
    arguments: args,
    workflowDimension,
    index: execIndex,
    originJobId: originJobId || workflowId,
    parentWorkflowId: workflowId,
    workflowId: activityJobId,
    workflowTopic: activityTopic,
    activityName,
    expire: options?.expire ?? expire,
    backoffCoefficient: options?.retryPolicy?.backoffCoefficient ?? undefined,
    maximumAttempts: options?.retryPolicy?.maximumAttempts ?? undefined,
    maximumInterval: maximumInterval ?? undefined,
  };
}

/**
 * Wraps a single activity in a proxy, orchestrating its execution and replay.
 * @private
 */
function wrapActivity<T>(activityName: string, options?: ActivityConfig): T {
  return async function (...args: any[]) {
    const [didRunAlready, execIndex, result] = await didRun('proxy');
    if (didRunAlready) {
      if (result?.$error) {
        if (options?.retryPolicy?.throwOnError !== false) {
          const code = result.$error.code;
          const message = result.$error.message;
          const stack = result.$error.stack;
          if (code === HMSH_CODE_DURABLE_FATAL) {
            throw new DurableFatalError(message, stack);
          } else if (code === HMSH_CODE_DURABLE_MAXED) {
            throw new DurableMaxedError(message, stack);
          } else if (code === HMSH_CODE_DURABLE_TIMEOUT) {
            throw new DurableTimeoutError(message, stack);
          } else {
            // For any other error code, throw a DurableFatalError to stop the workflow
            throw new DurableFatalError(message, stack);
          }
        }
        return result.$error as T;
      }
      return result.data as T;
    }

    const context = getContext();
    const { interruptionRegistry } = context;

    // Core activity registration logic
    const executeActivity = async () => {
      const interruptionMessage = getProxyInterruptPayload(
        context,
        activityName,
        execIndex,
        args,
        options,
      );
      interruptionRegistry.push({
        code: HMSH_CODE_DURABLE_PROXY,
        type: 'DurableProxyError',
        ...interruptionMessage,
      });
      await sleepImmediate();
      throw new DurableProxyError(interruptionMessage);
    };

    // Check for activity interceptors
    const store = asyncLocalStorage.getStore();
    const interceptorService = store?.get('activityInterceptorService');

    if (interceptorService?.activityInterceptors?.length > 0) {
      return await interceptorService.executeActivityChain(
        { activityName, args, options },
        store,
        executeActivity,
      );
    }

    return await executeActivity();
  } as unknown as T;
}

/**
 * Creates a typed proxy for calling activity functions with durable execution,
 * automatic retry, and deterministic replay. This is the primary way to invoke
 * side-effectful code (HTTP calls, database writes, file I/O) from within a
 * workflow function.
 *
 * Activities execute on a **separate worker process** via message queue,
 * isolating side effects from the deterministic workflow function. Each
 * proxied call is assigned a unique execution index, and on replay the
 * stored result is returned without re-executing the activity.
 *
 * ## Routing
 *
 * - **Default**: Activities route to `{workflowTaskQueue}-activity`.
 * - **Explicit `taskQueue`**: Activities route to `{taskQueue}-activity`,
 *   enabling shared/global activity worker pools across workflows.
 *
 * ## Retry Policy
 *
 * | Option               | Default | Description |
 * |----------------------|---------|-------------|
 * | `maximumAttempts`    | 50      | Max retries before the activity is marked as failed |
 * | `backoffCoefficient` | 2       | Exponential backoff multiplier |
 * | `maximumInterval`    | `'5m'`  | Cap on delay between retries |
 * | `throwOnError`       | `true`  | Throw on activity failure (set `false` to return the error) |
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import * as activities from './activities';
 *
 * // Standard pattern: register and proxy activities inline
 * export async function orderWorkflow(orderId: string): Promise<string> {
 *   const { validateOrder, chargePayment, sendConfirmation } =
 *     Durable.workflow.proxyActivities<typeof activities>({
 *       activities,
 *       retryPolicy: {
 *         maximumAttempts: 3,
 *         backoffCoefficient: 2,
 *         maximumInterval: '30s',
 *       },
 *     });
 *
 *   await validateOrder(orderId);
 *   const receipt = await chargePayment(orderId);
 *   await sendConfirmation(orderId, receipt);
 *   return receipt;
 * }
 * ```
 *
 * ```typescript
 * // Remote activities: reference a pre-registered worker pool by taskQueue
 * interface PaymentActivities {
 *   processPayment: (amount: number) => Promise<string>;
 *   refundPayment: (txId: string) => Promise<void>;
 * }
 *
 * export async function refundWorkflow(txId: string): Promise<void> {
 *   const { refundPayment } =
 *     Durable.workflow.proxyActivities<PaymentActivities>({
 *       taskQueue: 'payments',
 *       retryPolicy: { maximumAttempts: 5 },
 *     });
 *
 *   await refundPayment(txId);
 * }
 * ```
 *
 * ```typescript
 * // Interceptor with shared activity pool
 * const auditInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     const { auditLog } = Durable.workflow.proxyActivities<{
 *       auditLog: (id: string, action: string) => Promise<void>;
 *     }>({
 *       taskQueue: 'shared-audit',
 *       retryPolicy: { maximumAttempts: 3 },
 *     });
 *
 *     await auditLog(ctx.get('workflowId'), 'started');
 *     const result = await next();
 *     await auditLog(ctx.get('workflowId'), 'completed');
 *     return result;
 *   },
 * };
 * ```
 *
 * ```typescript
 * // Graceful error handling (no throw)
 * const { riskyOperation } = Durable.workflow.proxyActivities<typeof activities>({
 *   activities,
 *   retryPolicy: { maximumAttempts: 1, throwOnError: false },
 * });
 *
 * const result = await riskyOperation();
 * if (result instanceof Error) {
 *   // handle gracefully
 * }
 * ```
 *
 * @template ACT - The activity type map (use `typeof activities` for inline registration).
 * @param {ActivityConfig} [options] - Activity configuration including retry policy and routing.
 * @returns {ProxyType<ACT>} A typed proxy object mapping activity names to their durable wrappers.
 */
export function proxyActivities<ACT>(options?: ActivityConfig): ProxyType<ACT> {
  // Register activities if provided (optional - may already be registered remotely)
  if (options?.activities) {
    WorkerService.registerActivities(options.activities);
  }
  
  // Create proxy for all registered activities
  const proxy: any = {};
  const keys = Object.keys(WorkerService.activityRegistry);
  if (keys.length) {
    keys.forEach((key: string) => {
      const activityFunction = WorkerService.activityRegistry[key];
      proxy[key] = wrapActivity<typeof activityFunction>(key, options);
    });
  }
  return proxy as ProxyType<ACT>;
}

export { wrapActivity, getProxyInterruptPayload };
