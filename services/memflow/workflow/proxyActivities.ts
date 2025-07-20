import {
  WorkerService,
  ActivityConfig,
  ProxyType,
  sleepImmediate,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowTimeoutError,
  MemFlowProxyError,
  MemFlowRetryError,
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
  const activityTopic = `${workflowTopic}-activity`;
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
 * Provides a proxy for defined activities, ensuring deterministic replay and retry.
 * @template ACT
 * @param {ActivityConfig} [options] - Optional activity config (includes retryPolicy).
 * @returns {ProxyType<ACT>} A proxy to call activities as if local, but durably managed by the workflow.
 */
export function proxyActivities<ACT>(options?: ActivityConfig): ProxyType<ACT> {
  if (options?.activities) {
    WorkerService.registerActivities(options.activities);
  }
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
