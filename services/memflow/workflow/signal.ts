import { asyncLocalStorage, WorkerService } from './common';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Sends a signal payload to any paused workflow thread awaiting this signal.
 * This method is commonly used to coordinate between workflows, hook functions,
 * and external events.
 *
 * @example
 * // Basic usage - send a simple signal with data
 * await MemFlow.workflow.signal('signal-id', { name: 'WarmMash' });
 *
 * @example
 * // Hook function signaling completion
 * export async function exampleHook(name: string): Promise<void> {
 *   const result = await processData(name);
 *   await MemFlow.workflow.signal('hook-complete', { data: result });
 * }
 *
 * @example
 * // Signal with complex data structure
 * await MemFlow.workflow.signal('process-complete', {
 *   status: 'success',
 *   data: { id: 123, name: 'test' },
 *   timestamp: new Date().toISOString()
 * });
 *
 * @param {string} signalId - Unique signal identifier that matches a waitFor() call.
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
