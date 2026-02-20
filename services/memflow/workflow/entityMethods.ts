import { asyncLocalStorage, WorkerService, Entity } from './common';

/**
 * Returns an `Entity` session handle for interacting with the workflow's
 * structured JSON document storage. Unlike `search()` (flat HASH
 * key-value pairs), `entity()` provides a JSONB document store with
 * deep merge, append, and path-based get operations.
 *
 * Each call produces a unique session ID tied to the deterministic
 * execution counter, ensuring correct replay behavior.
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * export async function userProfileWorkflow(userId: string): Promise<UserProfile> {
 *   const entity = await MemFlow.workflow.entity();
 *
 *   // Initialize a structured document
 *   await entity.set({
 *     user: { id: userId, status: 'active' },
 *     preferences: { theme: 'dark', locale: 'en-US' },
 *   });
 *
 *   // Deep merge: adds name without overwriting existing fields
 *   await entity.merge({ user: { name: 'Alice', email: 'alice@example.com' } });
 *   // user is now: { id: userId, status: 'active', name: 'Alice', email: '...' }
 *
 *   // Append to an array
 *   await entity.set({ user: { tags: ['premium'] } });
 *   await entity.append({ user: { tags: ['verified'] } });
 *   // user.tags is now: ['premium', 'verified']
 *
 *   // Read a nested path
 *   const user = await entity.get('user');
 *   return user as UserProfile;
 * }
 * ```
 *
 * ```typescript
 * // Accumulate state across activities
 * export async function pipelineWorkflow(input: string): Promise<PipelineResult> {
 *   const entity = await MemFlow.workflow.entity();
 *   const { step1, step2, step3 } = MemFlow.workflow.proxyActivities<typeof activities>();
 *
 *   const r1 = await step1(input);
 *   await entity.merge({ pipeline: { step1: r1 } });
 *
 *   const r2 = await step2(r1);
 *   await entity.merge({ pipeline: { step2: r2 } });
 *
 *   const r3 = await step3(r2);
 *   await entity.merge({ pipeline: { step3: r3 } });
 *
 *   return await entity.get('pipeline') as PipelineResult;
 * }
 * ```
 *
 * @returns {Promise<Entity>} An entity session scoped to the current workflow job.
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
