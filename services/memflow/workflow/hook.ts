import {
  HookOptions,
  WorkerService,
  s,
  HMSH_MEMFLOW_EXP_BACKOFF,
  HMSH_MEMFLOW_MAX_ATTEMPTS,
  HMSH_MEMFLOW_MAX_INTERVAL,
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

    // DEFENSIVE CHECK: Prevent infinite loops
    if (
      targetTopic === workflowTopic &&
      !options.entity &&
      !options.taskQueue
    ) {
      throw new Error(
        `MemFlow Hook Error: Potential infinite loop detected!\n\n` +
          `The hook would target the same workflow topic ('${workflowTopic}') as the current workflow, ` +
          `creating an infinite loop.\n\n` +
          `To fix this, provide either:\n` +
          `1. 'taskQueue' parameter: MemFlow.workflow.hook({ taskQueue: 'your-queue', workflowName: '${options.workflowName}', args: [...] })\n` +
          `2. 'entity' parameter: MemFlow.workflow.hook({ entity: 'your-entity', args: [...] })\n\n` +
          `Current workflow topic: ${workflowTopic}\n` +
          `Target topic would be: ${targetTopic}\n` +
          `Provided options: ${JSON.stringify(
            {
              workflowName: options.workflowName,
              taskQueue: options.taskQueue,
              entity: options.entity,
            },
            null,
            2,
          )}`,
      );
    }

    const payload = {
      arguments: [...options.args],
      id: targetWorkflowId,
      workflowTopic: targetTopic,
      backoffCoefficient:
        options.config?.backoffCoefficient || HMSH_MEMFLOW_EXP_BACKOFF,
      maximumAttempts:
        options.config?.maximumAttempts || HMSH_MEMFLOW_MAX_ATTEMPTS,
      maximumInterval: s(
        options?.config?.maximumInterval ?? HMSH_MEMFLOW_MAX_INTERVAL,
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
