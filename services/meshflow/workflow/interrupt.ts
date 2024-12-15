import {
  WorkerService,
  JobInterruptOptions,
} from './common';
import { getContext } from './context';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Interrupts a running job by sending an interruption request.
 *
 * @param {string} jobId - The ID of the job to interrupt.
 * @param {JobInterruptOptions} options - Additional interruption options.
 * @returns {Promise<string|void>} Result of the interruption, if any.
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
