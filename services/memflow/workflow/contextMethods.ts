import { asyncLocalStorage, WorkerService, Context } from './common';

/**
 * Returns a context session handle for interacting with the workflow's JSONB context storage.
 * @returns {Promise<Context>} A context session for workflow data.
 * 
 * @example
 * ```typescript
 * const context = await workflow.context();
 * await context.set({ user: { id: 123 } });
 * await context.merge({ user: { name: "John" } });
 * const user = await context.get("user");
 * ```
 */
export async function context(): Promise<Context> {
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
  const contextSessionId = `-context${workflowDimension}-${execIndex}`;
  return new Context(workflowId, hotMeshClient, contextSessionId);
} 