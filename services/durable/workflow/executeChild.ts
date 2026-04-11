import {
  WorkflowOptions,
  DurableChildError,
  DurableChildErrorType,
  DurableFatalError,
  DurableMaxedError,
  DurableTimeoutError,
  DurableRetryError,
  DurableTelemetryService,
  HMSH_CODE_DURABLE_CHILD,
  HMSH_CODE_DURABLE_FATAL,
  HMSH_CODE_DURABLE_MAXED,
  HMSH_CODE_DURABLE_TIMEOUT,
  sleepImmediate,
  guid,
  s,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_INITIAL_INTERVAL,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_DURABLE_MAX_INTERVAL,
} from './common';
import { checkCancellation } from './cancellationScope';
import { workflowInfo } from './workflowInfo';
import { didRun } from './didRun';

/**
 * Constructs the payload necessary to spawn a child workflow.
 * @private
 */
function getChildInterruptPayload(
  context: ReturnType<typeof workflowInfo>,
  options: WorkflowOptions,
  execIndex: number,
): DurableChildErrorType {
  const { workflowId, originJobId, workflowDimension, expire, taskQueue: parentTaskQueue } = context;
  let childJobId: string;
  if (options.workflowId) {
    childJobId = options.workflowId;
  } else if (options.entity) {
    childJobId = `${options.entity}-${guid()}-${workflowDimension}-${execIndex}`;
  } else {
    childJobId = `-${options.workflowName}-${guid()}-${workflowDimension}-${execIndex}`;
  }

  const parentWorkflowId = workflowId;
  // Use explicit taskQueue, or parent's taskQueue, or entity as fallback
  const taskQueueName = options.taskQueue ?? parentTaskQueue ?? options.entity;
  const workflowName = options.taskQueue
    ? options.workflowName
    : options.entity ?? options.workflowName;
  const workflowTopic = `${taskQueueName}-${workflowName}`;
  return {
    arguments: [...(options.args || [])],
    await: options?.await ?? true,
    backoffCoefficient:
      options?.config?.backoffCoefficient ?? HMSH_DURABLE_EXP_BACKOFF,
    index: execIndex,
    initialInterval: s(
      options?.config?.initialInterval ?? `${HMSH_DURABLE_INITIAL_INTERVAL}s`,
    ),
    maximumAttempts:
      options?.config?.maximumAttempts ?? HMSH_DURABLE_MAX_ATTEMPTS,
    maximumInterval: s(
      options?.config?.maximumInterval ?? HMSH_DURABLE_MAX_INTERVAL,
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
    taskQueue: taskQueueName,
    workflowName: workflowName,
  };
}

/**
 * Spawns a child workflow and awaits its result. The child runs as an
 * independent job with its own lifecycle, retry policy, and dimensional
 * isolation. If the child fails, the error is propagated to the parent
 * as a typed error (`DurableFatalError`, `DurableMaxedError`,
 * `DurableTimeoutError`, or `DurableRetryError`).
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
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Spawn a child workflow and await its result
 * export async function parentWorkflow(orderId: string): Promise<string> {
 *   const result = await Durable.workflow.executeChild<{ status: string }>({
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
 *       Durable.workflow.executeChild<string>({
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
 * const user = await Durable.workflow.executeChild<UserRecord>({
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
export async function executeChild<T>(options: WorkflowOptions): Promise<T> {
  const isStartChild = options.await === false;
  const prefix = isStartChild ? 'start' : 'child';
  const [didRunAlready, execIndex, result] = await didRun(prefix);
  checkCancellation();
  const context = workflowInfo();
  const { canRetry, interruptionRegistry } = context;

  if (didRunAlready) {
    // Emit RETURN span in debug mode
    if (DurableTelemetryService.isVerbose() && result) {
      const { workflowTrace, workflowSpan } = context;
      if (workflowTrace && workflowSpan && result.ac && result.au) {
        const workflowName = options.workflowName || options.entity || 'unknown';
        DurableTelemetryService.emitDurationSpan(
          workflowTrace,
          workflowSpan,
          `RETURN/${prefix}/${workflowName}/${execIndex}`,
          DurableTelemetryService.parseTimestamp(result.ac),
          DurableTelemetryService.parseTimestamp(result.au),
          {
            'durable.operation.type': prefix,
            'durable.workflow.name': workflowName,
            'durable.exec.index': execIndex,
          },
        );
      }
    }

    if (result?.$error && (!result.$error.is_stream_error || !canRetry)) {
      if (options?.config?.throwOnError !== false) {
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
          throw new DurableRetryError(message, stack);
        }
      }
      return result.$error as T;
    } else if (!result?.$error) {
      return result.data as T;
    }
  }

  // Emit DISPATCH span in debug mode
  if (DurableTelemetryService.isVerbose()) {
    const { workflowTrace, workflowSpan } = context;
    if (workflowTrace && workflowSpan) {
      const workflowName = options.workflowName || options.entity || 'unknown';
      DurableTelemetryService.emitPointSpan(
        workflowTrace,
        workflowSpan,
        `DISPATCH/${prefix}/${workflowName}/${execIndex}`,
        {
          'durable.operation.type': prefix,
          'durable.workflow.name': workflowName,
          'durable.exec.index': execIndex,
        },
      );
    }
  }

  const interruptionMessage = getChildInterruptPayload(
    context,
    options,
    execIndex,
  );
  interruptionRegistry.push({
    code: HMSH_CODE_DURABLE_CHILD,
    type: 'DurableChildError',
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new DurableChildError(interruptionMessage);
}

export async function startChild(options: WorkflowOptions): Promise<string> {
  return executeChild({ ...options, await: false });
}

