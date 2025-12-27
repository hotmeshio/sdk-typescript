import {
  WorkerService,
  ActivityConfig,
  ProxyType,
  sleepImmediate,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowTimeoutError,
  MemFlowProxyError,
  HMSH_CODE_MEMFLOW_FATAL,
  HMSH_CODE_MEMFLOW_MAXED,
  HMSH_CODE_MEMFLOW_TIMEOUT,
  HMSH_CODE_MEMFLOW_PROXY,
  MemFlowProxyErrorType,
  s,
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
): MemFlowProxyErrorType {
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
          if (code === HMSH_CODE_MEMFLOW_FATAL) {
            throw new MemFlowFatalError(message, stack);
          } else if (code === HMSH_CODE_MEMFLOW_MAXED) {
            throw new MemFlowMaxedError(message, stack);
          } else if (code === HMSH_CODE_MEMFLOW_TIMEOUT) {
            throw new MemFlowTimeoutError(message, stack);
          } else {
            // For any other error code, throw a MemFlowFatalError to stop the workflow
            throw new MemFlowFatalError(message, stack);
          }
        }
        return result.$error as T;
      }
      return result.data as T;
    }

    const context = getContext();
    const { interruptionRegistry } = context;
    const interruptionMessage = getProxyInterruptPayload(
      context,
      activityName,
      execIndex,
      args,
      options,
    );
    interruptionRegistry.push({
      code: HMSH_CODE_MEMFLOW_PROXY,
      type: 'MemFlowProxyError',
      ...interruptionMessage,
    });
    await sleepImmediate();
    throw new MemFlowProxyError(interruptionMessage);
  } as unknown as T;
}

/**
 * Create proxies for activity functions with automatic retry and deterministic replay.
 * Activities execute via message queue, so they can run on different servers.
 * 
 * Without `taskQueue`, activities use the workflow's task queue (e.g., `my-workflow-activity`).
 * With `taskQueue`, activities use the specified queue (e.g., `payment-activity`).
 * 
 * The `activities` parameter is optional. If activities are already registered via
 * `registerActivityWorker()`, you can reference them by providing just the `taskQueue`
 * and a TypeScript interface.
 * 
 * @template ACT
 * @param {ActivityConfig} [options] - Activity configuration
 * @param {any} [options.activities] - (Optional) Activity functions to register inline
 * @param {string} [options.taskQueue] - (Optional) Task queue name (without `-activity` suffix)
 * @param {object} [options.retryPolicy] - Retry configuration
 * @returns {ProxyType<ACT>} Proxy for calling activities with durability and retry
 * 
 * @example
 * ```typescript
 * // Inline registration (activities in same codebase)
 * const activities = MemFlow.workflow.proxyActivities<typeof activities>({
 *   activities: { processData, validateData },
 *   retryPolicy: { maximumAttempts: 3 }
 * });
 * 
 * await activities.processData('input');
 * ```
 * 
 * @example
 * ```typescript
 * // Reference pre-registered activities (can be on different server)
 * interface PaymentActivities {
 *   processPayment: (amount: number) => Promise<string>;
 *   sendEmail: (to: string, subject: string) => Promise<void>;
 * }
 * 
 * const { processPayment, sendEmail } = 
 *   MemFlow.workflow.proxyActivities<PaymentActivities>({
 *     taskQueue: 'payment',
 *     retryPolicy: { maximumAttempts: 3 }
 *   });
 * 
 * const result = await processPayment(100.00);
 * await sendEmail('user@example.com', 'Payment processed');
 * ```
 * 
 * @example
 * ```typescript
 * // Shared activities in interceptor
 * const interceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     const { auditLog } = MemFlow.workflow.proxyActivities<{
 *       auditLog: (id: string, action: string) => Promise<void>;
 *     }>({
 *       taskQueue: 'shared',
 *       retryPolicy: { maximumAttempts: 3 }
 *     });
 *     
 *     await auditLog(ctx.get('workflowId'), 'started');
 *     const result = await next();
 *     await auditLog(ctx.get('workflowId'), 'completed');
 *     return result;
 *   }
 * };
 * ```
 * 
 * @example
 * ```typescript
 * // Custom task queue for specific activities
 * const highPriority = MemFlow.workflow.proxyActivities<typeof activities>({
 *   activities: { criticalProcess },
 *   taskQueue: 'high-priority',
 *   retryPolicy: { maximumAttempts: 5 }
 * });
 * ```
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
