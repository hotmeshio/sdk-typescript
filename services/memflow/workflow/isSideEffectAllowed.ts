import {
  asyncLocalStorage,
  KeyService,
  KeyType,
  WorkerService,
  HotMesh,
} from './common';

/**
 * Checks if a side-effect is allowed to run. This ensures certain actions
 * are executed exactly once.
 * @private
 * @param {HotMesh} hotMeshClient - The HotMesh client.
 * @param {string} prefix - A unique prefix representing the action (e.g., 'trace', 'emit', etc.)
 * @returns {Promise<boolean>} True if the side effect can run, false otherwise.
 */
export async function isSideEffectAllowed(
  hotMeshClient: HotMesh,
  prefix: string,
): Promise<boolean> {
  const store = asyncLocalStorage.getStore();
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const COUNTER = store.get('counter');
  const execIndex = COUNTER.counter = COUNTER.counter + 1;
  const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
  const replay = store.get('replay');

  if (sessionId in replay) {
    return false;
  }

  const keyParams = {
    appId: hotMeshClient.appId,
    jobId: workflowId,
  };
  const workflowGuid = KeyService.mintKey(
    hotMeshClient.namespace,
    KeyType.JOB_STATE,
    keyParams,
  );
  const searchClient = hotMeshClient.engine.search;
  const guidValue = await searchClient.incrementFieldByFloat(
    workflowGuid,
    sessionId,
    1,
  );
  return guidValue === 1;
}
