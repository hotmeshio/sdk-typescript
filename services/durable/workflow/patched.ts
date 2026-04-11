import {
  asyncLocalStorage,
  KeyService,
  KeyType,
  WorkerService,
  SerializerService,
} from './common';

/**
 * Enables safe code changes to running workflows by branching on a named
 * change marker. On first execution of a new workflow, `patched` writes a
 * durable marker and returns `true` — the workflow takes the new code path.
 * On replay of a workflow that was started **before** the patch existed,
 * no marker is found and `patched` returns `false` — the old code path
 * is followed.
 *
 * `patched` does **not** increment the execution counter, so it can be
 * inserted into existing workflow code without shifting the replay
 * positions of other durable operations.
 *
 * ## Lifecycle
 *
 * 1. **Add the patch:** wrap the new behavior with `if (await patched('id'))`.
 *    Keep the old behavior in the `else` branch.
 * 2. **Wait for drain:** once all workflows started before the patch have
 *    completed, the `else` branch is dead code.
 * 3. **Deprecate:** replace `patched` with `deprecatePatch` and remove the
 *    `else` branch.
 * 4. **Clean up:** remove both `deprecatePatch` and the `if` wrapper,
 *    leaving only the new code.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * export async function orderWorkflow(orderId: string): Promise<string> {
 *   const acts = Durable.workflow.proxyActivities<typeof activities>({
 *     activities,
 *     retryPolicy: { maximumAttempts: 3 },
 *   });
 *
 *   if (await Durable.workflow.patched('v2-validation')) {
 *     // New path: stricter validation
 *     await acts.validateOrderV2(orderId);
 *   } else {
 *     // Old path: legacy validation (for in-flight workflows)
 *     await acts.validateOrder(orderId);
 *   }
 *
 *   return await acts.processOrder(orderId);
 * }
 * ```
 *
 * ```typescript
 * // After all pre-patch workflows have drained:
 * export async function orderWorkflow(orderId: string): Promise<string> {
 *   const acts = Durable.workflow.proxyActivities<typeof activities>({ ... });
 *
 *   Durable.workflow.deprecatePatch('v2-validation');
 *   await acts.validateOrderV2(orderId);
 *
 *   return await acts.processOrder(orderId);
 * }
 * ```
 *
 * @param {string} changeId - A unique, stable identifier for this code change.
 *   Must not be reused across different changes.
 * @returns {Promise<boolean>} `true` for new workflows (take new path),
 *   `false` for pre-patch workflows being replayed (take old path).
 */
export async function patched(changeId: string): Promise<boolean> {
  const store = asyncLocalStorage.getStore();
  const replay = store.get('replay');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const patchKey = `-patch${workflowDimension}-${changeId}-`;

  // Marker exists from a previous execution — take new path
  if (patchKey in replay) {
    return true;
  }

  // Marker not found. Distinguish first execution from pre-patch replay
  // by checking for non-patch replay entries. On first execution, no
  // durable operations have completed yet, so only patch markers (from
  // earlier patched() calls in this same execution) can exist.
  const patchPrefix = `-patch${workflowDimension}-`;
  const hasNonPatchEntries = Object.keys(replay).some(
    (key) => !key.startsWith(patchPrefix),
  );

  if (hasNonPatchEntries) {
    // Pre-patch workflow: replay has entries from prior operations but
    // no marker for this change — follow the old code path
    return false;
  }

  // First execution of a new workflow: write the marker to the job hash
  // so it persists across re-entries and replays
  const workflowId = store.get('workflowId');
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const hotMesh = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  const keyParams = {
    appId: hotMesh.appId,
    jobId: workflowId,
  };
  const workflowGuid = KeyService.mintKey(
    hotMesh.namespace,
    KeyType.JOB_STATE,
    keyParams,
  );
  // Atomic write — incrementFieldByFloat returns 1 for first writer
  await hotMesh.engine.search.incrementFieldByFloat(
    workflowGuid,
    patchKey,
    1,
  );

  // Update in-memory replay so subsequent patched() calls in this
  // execution see the marker without a database round-trip
  replay[patchKey] = SerializerService.toString(true);

  return true;
}

/**
 * Declares that all workflows started before a given patch have drained
 * and the old code path can be removed. This is a **no-op** at runtime —
 * it exists purely as a migration signal in source code.
 *
 * ## Migration Steps
 *
 * 1. Replace `if (await patched('id')) { new } else { old }` with
 *    `deprecatePatch('id'); new`.
 * 2. Deploy and verify.
 * 3. In a subsequent release, remove both `deprecatePatch('id')` and
 *    the surrounding wrapper, leaving only the new code.
 *
 * @param {string} _changeId - The change ID being deprecated (unused at runtime).
 */
export function deprecatePatch(_changeId: string): void {
  // No-op: all pre-patch workflows have completed.
  // Safe to remove the old code path.
}
