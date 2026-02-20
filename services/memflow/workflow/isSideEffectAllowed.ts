import {
  asyncLocalStorage,
  KeyService,
  KeyType,
  WorkerService,
  HotMesh,
} from './common';

/**
 * Guards side-effectful operations (`emit`, `signal`, `hook`, `trace`,
 * `interrupt`) against duplicate execution during replay. Unlike
 * `didRun()` (which replays stored results), this guard is for
 * operations that don't produce a return value to cache — they simply
 * must not run twice.
 *
 * ## Mechanism
 *
 * 1. Increments the execution counter to produce a `sessionId`.
 * 2. If that `sessionId` already exists in the `replay` hash, the
 *    operation already ran in a previous execution → return `false`.
 * 3. Otherwise, atomically increments a field on the job's backend
 *    HASH via `incrementFieldByFloat`. If the result is exactly `1`,
 *    this is the first worker to reach this point → return `true`.
 *    If `> 1`, a concurrent worker already executed it → return `false`.
 *
 * This provides **distributed idempotency** for side effects across
 * replays and concurrent worker instances.
 *
 * @private
 * @param {HotMesh} hotMeshClient - The HotMesh client.
 * @param {string} prefix - Operation type (`'trace'`, `'emit'`, `'signal'`, `'hook'`, `'interrupt'`).
 * @returns {Promise<boolean>} `true` if the side effect should execute, `false` if it already ran.
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
