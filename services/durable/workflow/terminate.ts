import { WorkerService, JobInterruptOptions } from './common';
import { workflowInfo } from './workflowInfo';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Terminates a running workflow job by its ID. The target job's status
 * is set to an error code indicating abnormal termination, and any
 * pending activities or timers are cancelled.
 *
 * This is the workflow-internal terminate — it can only be called from
 * within a workflow function. For external termination, use
 * `handle.terminate()` directly.
 *
 * The terminate fires exactly once per workflow execution — the
 * `isSideEffectAllowed` guard prevents re-terminating on replay.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Terminate a child workflow from the parent
 * export async function supervisorWorkflow(): Promise<void> {
 *   const childId = await Durable.workflow.startChild({
 *     taskQueue: 'workers',
 *     workflowName: 'longTask',
 *     args: [],
 *   });
 *
 *   // Wait for a timeout, then terminate the child
 *   await Durable.workflow.sleep('5 minutes');
 *   await Durable.workflow.terminate(childId, {
 *     reason: 'Timed out waiting for child',
 *     descend: true,     // also terminate any grandchild workflows
 *   });
 * }
 * ```
 *
 * ```typescript
 * // Self-terminate on validation failure
 * export async function validatedWorkflow(input: string): Promise<void> {
 *   const { workflowId } = Durable.workflow.workflowInfo();
 *   const { validate } = Durable.workflow.proxyActivities<typeof activities>();
 *
 *   const isValid = await validate(input);
 *   if (!isValid) {
 *     await Durable.workflow.terminate(workflowId, {
 *       reason: 'Invalid input',
 *     });
 *   }
 * }
 * ```
 *
 * @param {string} jobId - The ID of the workflow job to terminate.
 * @param {JobInterruptOptions} [options={}] - Termination options (`reason`, `descend`, etc.).
 * @returns {Promise<string | void>} The result of the termination, if any.
 */
export async function terminate(
  jobId: string,
  options: JobInterruptOptions = {},
): Promise<string | void> {
  const { workflowTopic, connection, namespace } = workflowInfo();
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
