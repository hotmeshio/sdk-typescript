import {
  HookOptions,
  WorkerService,
  s,
  HMSH_MESHFLOW_EXP_BACKOFF,
  HMSH_MESHFLOW_MAX_ATTEMPTS,
  HMSH_MESHFLOW_MAX_INTERVAL,
  StreamStatus,
} from './common';
import { getContext } from './context';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Spawns a hook from the main thread or a hook thread.
 * If entity/workflowName are not provided, defaults to the current workflow.
 *
 * @param {HookOptions} options - Hook configuration options.
 * @returns {Promise<string>} The resulting hook/stream ID.
 */
export async function hook(options: HookOptions): Promise<string> {
  const { workflowId, connection, namespace, workflowTopic } = getContext();
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  if (await isSideEffectAllowed(hotMeshClient, 'hook')) {
    const targetWorkflowId = options.workflowId ?? workflowId;
    let targetTopic: string;
    if (options.entity || (options.taskQueue && options.workflowName)) {
      targetTopic = `${options.taskQueue ?? options.entity}-${options.entity ?? options.workflowName}`;
    } else {
      targetTopic = workflowTopic;
    }
    const payload = {
      arguments: [...options.args],
      id: targetWorkflowId,
      workflowTopic: targetTopic,
      backoffCoefficient:
        options.config?.backoffCoefficient || HMSH_MESHFLOW_EXP_BACKOFF,
      maximumAttempts:
        options.config?.maximumAttempts || HMSH_MESHFLOW_MAX_ATTEMPTS,
      maximumInterval: s(
        options?.config?.maximumInterval ?? HMSH_MESHFLOW_MAX_INTERVAL,
      ),
    };
    return await hotMeshClient.hook(
      `${namespace}.flow.signal`,
      payload,
      StreamStatus.PENDING,
      202,
    );
  }
}
