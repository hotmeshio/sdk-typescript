import {
  HookOptions,
  WorkerService,
  s,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_DURABLE_MAX_INTERVAL,
  StreamStatus,
} from './common';
import { getContext } from './context';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Spawns a hook execution against an existing workflow job. The hook runs
 * in an isolated dimensional thread within the target job's namespace,
 * allowing it to read/write the same job state without interfering with
 * the main workflow thread.
 *
 * This is the low-level primitive behind `execHook()`. Use `hook()`
 * directly when you need fire-and-forget hook execution or when you
 * manage signal coordination yourself.
 *
 * ## Target Resolution
 *
 * - If `taskQueue` and `workflowName` (or `entity`) are provided, the
 *   hook targets that specific workflow type.
 * - If neither is provided, the hook targets the **current** workflow.
 *   However, targeting the same topic as the current workflow is
 *   rejected to prevent infinite loops.
 *
 * ## Idempotency
 *
 * The `isSideEffectAllowed` guard ensures hooks fire exactly once —
 * on replay, the hook is not re-spawned.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Fire-and-forget: spawn a hook without waiting for its result
 * export async function notifyWorkflow(userId: string): Promise<void> {
 *   await Durable.workflow.hook({
 *     taskQueue: 'notifications',
 *     workflowName: 'sendNotification',
 *     args: [userId, 'Your order has shipped'],
 *   });
 *   // Continues immediately, does not wait for the hook
 * }
 * ```
 *
 * ```typescript
 * // Manual signal coordination (equivalent to execHook)
 * export async function manualHookPattern(itemId: string): Promise<string> {
 *   const signalId = `process-${itemId}`;
 *
 *   await Durable.workflow.hook({
 *     taskQueue: 'processors',
 *     workflowName: 'processItem',
 *     args: [itemId, signalId],
 *   });
 *
 *   // Manually wait for the hook to signal back
 *   return await Durable.workflow.waitFor<string>(signalId);
 * }
 * ```
 *
 * ```typescript
 * // Hook with retry configuration
 * await Durable.workflow.hook({
 *   taskQueue: 'enrichment',
 *   workflowName: 'enrichProfile',
 *   args: [profileId],
 *   config: {
 *     maximumAttempts: 5,
 *     backoffCoefficient: 2,
 *     maximumInterval: '1m',
 *   },
 * });
 * ```
 *
 * @param {HookOptions} options - Hook configuration including target workflow and arguments.
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
        `Durable Hook Error: Potential infinite loop detected!\n\n` +
          `The hook would target the same workflow topic ('${workflowTopic}') as the current workflow, ` +
          `creating an infinite loop.\n\n` +
          `To fix this, provide either:\n` +
          `1. 'taskQueue' parameter: Durable.workflow.hook({ taskQueue: 'your-queue', workflowName: '${options.workflowName}', args: [...] })\n` +
          `2. 'entity' parameter: Durable.workflow.hook({ entity: 'your-entity', args: [...] })\n\n` +
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
        options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
      maximumAttempts:
        options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
      maximumInterval: s(
        options?.config?.maximumInterval ?? HMSH_DURABLE_MAX_INTERVAL,
      ),
    };
    return await hotMeshClient.signal(
      `${namespace}.flow.signal`,
      payload,
      StreamStatus.PENDING,
      202,
    );
  }
}
