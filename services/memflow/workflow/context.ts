import { asyncLocalStorage, WorkflowContext } from './common';

/**
 * Returns the current workflow context, restored from storage.
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
