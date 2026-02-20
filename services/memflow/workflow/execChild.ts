import {
  WorkflowOptions,
  MemFlowChildError,
  MemFlowChildErrorType,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowTimeoutError,
  MemFlowRetryError,
  HMSH_CODE_MEMFLOW_CHILD,
  HMSH_CODE_MEMFLOW_FATAL,
  HMSH_CODE_MEMFLOW_MAXED,
  HMSH_CODE_MEMFLOW_TIMEOUT,
  sleepImmediate,
  guid,
  s,
  HMSH_MEMFLOW_EXP_BACKOFF,
  HMSH_MEMFLOW_MAX_ATTEMPTS,
  HMSH_MEMFLOW_MAX_INTERVAL,
} from './common';
import { getContext } from './context';
import { didRun } from './didRun';

/**
 * Constructs the payload necessary to spawn a child workflow.
 * @private
 */
function getChildInterruptPayload(
  context: ReturnType<typeof getContext>,
  options: WorkflowOptions,
  execIndex: number,
): MemFlowChildErrorType {
  const { workflowId, originJobId, workflowDimension, expire } = context;
  let childJobId: string;
  if (options.workflowId) {
    childJobId = options.workflowId;
  } else if (options.entity) {
    childJobId = `${options.entity}-${guid()}-${workflowDimension}-${execIndex}`;
  } else {
    childJobId = `-${options.workflowName}-${guid()}-${workflowDimension}-${execIndex}`;
  }

  const parentWorkflowId = workflowId;
  const taskQueueName = options.taskQueue ?? options.entity;
  const workflowName = options.taskQueue
    ? options.workflowName
    : options.entity ?? options.workflowName;
  const workflowTopic = `${taskQueueName}-${workflowName}`;
  return {
    arguments: [...(options.args || [])],
    await: options?.await ?? true,
    backoffCoefficient:
      options?.config?.backoffCoefficient ?? HMSH_MEMFLOW_EXP_BACKOFF,
    index: execIndex,
    maximumAttempts:
      options?.config?.maximumAttempts ?? HMSH_MEMFLOW_MAX_ATTEMPTS,
    maximumInterval: s(
      options?.config?.maximumInterval ?? HMSH_MEMFLOW_MAX_INTERVAL,
    ),
    originJobId: originJobId ?? workflowId,
    entity: options.entity,
    expire: options.expire ?? expire,
    persistent: options.persistent,
    signalIn: options.signalIn,
    parentWorkflowId,
    workflowDimension: workflowDimension,
    workflowId: childJobId,
    workflowTopic,
  };
}

/**
 * Spawns a child workflow and awaits its result. The child runs as an
 * independent job with its own lifecycle, retry policy, and dimensional
 * isolation. If the child fails, the error is propagated to the parent
 * as a typed error (`MemFlowFatalError`, `MemFlowMaxedError`,
 * `MemFlowTimeoutError`, or `MemFlowRetryError`).
 *
 * On replay, the stored child result is returned immediately without
 * re-spawning the child workflow.
 *
 * ## Child Job ID
 *
 * If `options.workflowId` is provided, it is used directly. Otherwise,
 * the child ID is generated from the entity/workflow name, a GUID, the
 * parent's dimensional coordinates, and the execution index — ensuring
 * uniqueness across parallel and re-entrant executions.
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * // Spawn a child workflow and await its result
 * export async function parentWorkflow(orderId: string): Promise<string> {
 *   const result = await MemFlow.workflow.execChild<{ status: string }>({
 *     taskQueue: 'payments',
 *     workflowName: 'processPayment',
 *     args: [orderId, 99.99],
 *     config: {
 *       maximumAttempts: 3,
 *       backoffCoefficient: 2,
 *     },
 *   });
 *   return result.status;
 * }
 * ```
 *
 * ```typescript
 * // Fan-out: spawn multiple children in parallel
 * export async function batchWorkflow(items: string[]): Promise<string[]> {
 *   const results = await Promise.all(
 *     items.map((item) =>
 *       MemFlow.workflow.execChild<string>({
 *         taskQueue: 'processors',
 *         workflowName: 'processItem',
 *         args: [item],
 *       }),
 *     ),
 *   );
 *   return results;
 * }
 * ```
 *
 * ```typescript
 * // Entity-based child (uses entity name as task queue)
 * const user = await MemFlow.workflow.execChild<UserRecord>({
 *   entity: 'user',
 *   args: [{ name: 'Alice', email: 'alice@example.com' }],
 *   workflowId: 'user-alice',          // deterministic ID
 *   expire: 3600,                       // 1 hour TTL
 * });
 * ```
 *
 * @template T - The return type of the child workflow.
 * @param {WorkflowOptions} options - Child workflow configuration.
 * @returns {Promise<T>} The child workflow's return value.
 */
export async function execChild<T>(options: WorkflowOptions): Promise<T> {
  const isStartChild = options.await === false;
  const prefix = isStartChild ? 'start' : 'child';
  const [didRunAlready, execIndex, result] = await didRun(prefix);
  const context = getContext();
  const { canRetry, interruptionRegistry } = context;

  if (didRunAlready) {
    if (result?.$error && (!result.$error.is_stream_error || !canRetry)) {
      if (options?.config?.throwOnError !== false) {
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
          throw new MemFlowRetryError(message, stack);
        }
      }
      return result.$error as T;
    } else if (!result?.$error) {
      return result.data as T;
    }
  }

  const interruptionMessage = getChildInterruptPayload(
    context,
    options,
    execIndex,
  );
  interruptionRegistry.push({
    code: HMSH_CODE_MEMFLOW_CHILD,
    type: 'MemFlowChildError',
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new MemFlowChildError(interruptionMessage);
}

/**
 * Alias for {@link execChild}.
 */
export const executeChild = execChild;

/**
 * Spawns a child workflow in fire-and-forget mode. The parent workflow
 * continues immediately without waiting for the child to complete.
 * Returns the child's job ID for later reference (e.g., to interrupt
 * or query the child).
 *
 * This is a convenience wrapper around `execChild` with `await: false`.
 *
 * ## Example
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * export async function dispatchWorkflow(taskId: string): Promise<string> {
 *   // Fire-and-forget: start the child and continue immediately
 *   const childJobId = await MemFlow.workflow.startChild({
 *     taskQueue: 'background',
 *     workflowName: 'longRunningTask',
 *     args: [taskId],
 *   });
 *
 *   // Optionally store the child ID for monitoring
 *   const search = await MemFlow.workflow.search();
 *   await search.set({ childJobId });
 *
 *   return childJobId;
 * }
 * ```
 *
 * @param {WorkflowOptions} options - Child workflow configuration.
 * @returns {Promise<string>} The child workflow's job ID.
 */
export async function startChild(options: WorkflowOptions): Promise<string> {
  return execChild({ ...options, await: false });
}
