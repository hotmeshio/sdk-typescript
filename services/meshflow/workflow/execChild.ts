import {
  WorkflowOptions,
  MeshFlowChildError,
  MeshFlowChildErrorType,
  MeshFlowFatalError,
  MeshFlowMaxedError,
  MeshFlowTimeoutError,
  MeshFlowRetryError,
  HMSH_CODE_MESHFLOW_CHILD,
  HMSH_CODE_MESHFLOW_FATAL,
  HMSH_CODE_MESHFLOW_MAXED,
  HMSH_CODE_MESHFLOW_TIMEOUT,
  sleepImmediate,
  guid,
  s,
  HMSH_MESHFLOW_EXP_BACKOFF,
  HMSH_MESHFLOW_MAX_ATTEMPTS,
  HMSH_MESHFLOW_MAX_INTERVAL,
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
): MeshFlowChildErrorType {
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
  const workflowName = options.entity ?? options.workflowName;
  const workflowTopic = `${taskQueueName}-${workflowName}`;
  return {
    arguments: [...(options.args || [])],
    await: options?.await ?? true,
    backoffCoefficient:
      options?.config?.backoffCoefficient ?? HMSH_MESHFLOW_EXP_BACKOFF,
    index: execIndex,
    maximumAttempts:
      options?.config?.maximumAttempts ?? HMSH_MESHFLOW_MAX_ATTEMPTS,
    maximumInterval: s(
      options?.config?.maximumInterval ?? HMSH_MESHFLOW_MAX_INTERVAL,
    ),
    originJobId: originJobId ?? workflowId,
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
 * Spawns a child workflow and awaits the result, or if `await` is false, returns immediately.
 * @template T
 * @param {WorkflowOptions} options - Workflow options.
 * @returns {Promise<T>} Result of the child workflow.
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
        if (code === HMSH_CODE_MESHFLOW_FATAL) {
          throw new MeshFlowFatalError(message, stack);
        } else if (code === HMSH_CODE_MESHFLOW_MAXED) {
          throw new MeshFlowMaxedError(message, stack);
        } else if (code === HMSH_CODE_MESHFLOW_TIMEOUT) {
          throw new MeshFlowTimeoutError(message, stack);
        } else {
          throw new MeshFlowRetryError(message, stack);
        }
      }
      return result.$error as T;
    } else if (!result?.$error) {
      return result.data as T;
    }
  }

  const interruptionMessage = getChildInterruptPayload(context, options, execIndex);
  interruptionRegistry.push({
    code: HMSH_CODE_MESHFLOW_CHILD,
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new MeshFlowChildError(interruptionMessage);
}

/**
 * Alias for execChild.
 */
export const executeChild = execChild;

/**
 * Spawns a child workflow and returns the child Job ID without awaiting its completion.
 * @param {WorkflowOptions} options - Workflow options.
 * @returns {Promise<string>} The child job ID.
 */
export async function startChild(options: WorkflowOptions): Promise<string> {
  return execChild({ ...options, await: false });
}
