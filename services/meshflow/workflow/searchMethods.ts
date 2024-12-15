import {
  asyncLocalStorage,
  WorkerService,
  Search,
} from './common';

/**
 * Returns a search session handle for interacting with the workflow's HASH storage.
 * @returns {Promise<Search>} A search session for workflow data.
 */
export async function search(): Promise<Search> {
  const store = asyncLocalStorage.getStore();
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const COUNTER = store.get('counter');
  const execIndex = COUNTER.counter = COUNTER.counter + 1;
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  const searchSessionId = `-search${workflowDimension}-${execIndex}`;
  return new Search(workflowId, hotMeshClient, searchSessionId);
}
