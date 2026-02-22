import { WorkerService, JobInterruptOptions } from './common';
import { getContext } from './context';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Terminates a running workflow job by its ID. The target job's status
 * is set to an error code indicating abnormal termination, and any
 * pending activities or timers are cancelled.
 *
 * This is the workflow-internal interrupt — it can only be called from
 * within a workflow function. For external interruption, use
 * `hotMesh.interrupt()` directly.
 *
 * The interrupt fires exactly once per workflow execution — the
 * `isSideEffectAllowed` guard prevents re-interrupting on replay.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Cancel a child workflow from the parent
 * export async function supervisorWorkflow(): Promise<void> {
 *   const childId = await Durable.workflow.startChild({
 *     taskQueue: 'workers',
 *     workflowName: 'longTask',
 *     args: [],
 *   });
 *
 *   // Wait for a timeout, then cancel the child
 *   await Durable.workflow.sleepFor('5 minutes');
 *   await Durable.workflow.interrupt(childId, {
 *     reason: 'Timed out waiting for child',
 *     descend: true,     // also interrupt any grandchild workflows
 *   });
 * }
 * ```
 *
 * ```typescript
 * // Self-interrupt on validation failure
 * export async function validatedWorkflow(input: string): Promise<void> {
 *   const { workflowId } = Durable.workflow.getContext();
 *   const { validate } = Durable.workflow.proxyActivities<typeof activities>();
 *
 *   const isValid = await validate(input);
 *   if (!isValid) {
 *     await Durable.workflow.interrupt(workflowId, {
 *       reason: 'Invalid input',
 *     });
 *   }
 * }
 * ```
 *
 * @param {string} jobId - The ID of the workflow job to interrupt.
 * @param {JobInterruptOptions} [options={}] - Interruption options (`reason`, `descend`, etc.).
 * @returns {Promise<string | void>} The result of the interruption, if any.
 */
export async function interrupt(
  jobId: string,
  options: JobInterruptOptions = {},
): Promise<string | void> {
  const { workflowTopic, connection, namespace } = getContext();
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  if (await isSideEffectAllowed(hotMeshClient, 'interrupt')) {
    return await hotMeshClient.interrupt(
      `${hotMeshClient.appId}.execute`,
      jobId,
      options,
    );
  }
}
