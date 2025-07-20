import { asyncLocalStorage, WorkerService, Entity } from './common';

/**
 * Returns an entity session handle for interacting with the workflow's JSONB entity storage.
 * @returns {Promise<Entity>} An entity session for workflow data.
 *
 * @example
 * ```typescript
 * const entity = await workflow.entity();
 * await entity.set({ user: { id: 123 } });
 * await entity.merge({ user: { name: "John" } });
 * const user = await entity.get("user");
 * ```
 */
export async function entity(): Promise<Entity> {
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
  const entitySessionId = `-entity${workflowDimension}-${execIndex}`;
  return new Entity(workflowId, hotMeshClient, entitySessionId);
}
