import { asyncLocalStorage, WorkflowContext } from './common';

/**
 * Returns the current workflow's execution context, providing access to
 * the workflow ID, replay state, dimensional coordinates, connection
 * info, and other runtime metadata.
 *
 * The context is populated by the worker's `wrapWorkflowFunction` and
 * stored in `AsyncLocalStorage`, making it available to any code running
 * within the workflow function's call stack.
 *
 * ## Examples
 *
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * // Access the workflow ID and namespace
 * export async function contextAwareWorkflow(): Promise<string> {
 *   const ctx = MemFlow.workflow.getContext();
 *   console.log(`Running workflow ${ctx.workflowId} in ${ctx.namespace}`);
 *   return ctx.workflowId;
 * }
 * ```
 *
 * ```typescript
 * // Check if the current execution is a replay
 * export async function replayAwareWorkflow(): Promise<void> {
 *   const { counter, workflowDimension } = MemFlow.workflow.getContext();
 *
 *   // Use context for logging/debugging
 *   console.log(`Execution counter: ${counter}, dimension: ${workflowDimension}`);
 * }
 * ```
 *
 * ```typescript
 * // Pass context info to child workflows
 * export async function parentWorkflow(): Promise<void> {
 *   const { workflowId } = MemFlow.workflow.getContext();
 *
 *   await MemFlow.workflow.execChild({
 *     taskQueue: 'children',
 *     workflowName: 'childWorkflow',
 *     args: [workflowId],  // pass parent ID to child
 *   });
 * }
 * ```
 *
 * @returns {WorkflowContext} The current workflow context.
 */
export function getContext(): WorkflowContext {
  const store = asyncLocalStorage.getStore();
  const workflowId = store.get('workflowId');
  const replay = store.get('replay');
  const cursor = store.get('cursor');
  const interruptionRegistry = store.get('interruptionRegistry');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const originJobId = store.get('originJobId');
  const workflowTrace = store.get('workflowTrace');
  const canRetry = store.get('canRetry');
  const workflowSpan = store.get('workflowSpan');
  const expire = store.get('expire');
  const COUNTER = store.get('counter');
  const raw = store.get('raw');
  return {
    canRetry,
    COUNTER,
    counter: COUNTER.counter,
    cursor,
    interruptionRegistry,
    connection,
    expire,
    namespace,
    originJobId,
    raw,
    replay,
    workflowId,
    workflowDimension,
    workflowTopic,
    workflowTrace,
    workflowSpan,
  };
}
