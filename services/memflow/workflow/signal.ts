import { asyncLocalStorage, WorkerService } from './common';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Sends a signal payload to any paused workflow thread awaiting this signal.
 * @param {string} signalId - Unique signal identifier.
 * @param {Record<any, any>} data - The payload to send with the signal.
 * @returns {Promise<string>} The resulting hook/stream ID.
 */
export async function signal(
  signalId: string,
  data: Record<any, any>,
): Promise<string> {
  const store = asyncLocalStorage.getStore();
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  if (await isSideEffectAllowed(hotMeshClient, 'signal')) {
    return await hotMeshClient.hook(`${namespace}.wfs.signal`, {
      id: signalId,
      data,
    });
  }
}
