import { StringStringType } from './common';
import { search } from './searchMethods';

/**
 * Adds custom key-value metadata to the workflow's backend HASH record
 * in a single call. This is a convenience wrapper around
 * `search().set(fields)` that handles session creation automatically.
 *
 * Enrichment runs exactly once per workflow execution — the underlying
 * search session ensures idempotency on replay.
 *
 * Use `enrich` for quick one-shot writes. For repeated reads/writes
 * within the same workflow, prefer acquiring a `search()` session
 * handle directly.
 *
 * ## Example
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * export async function onboardingWorkflow(userId: string): Promise<void> {
 *   // Tag the workflow record with queryable metadata
 *   await Durable.workflow.enrich({
 *     userId,
 *     stage: 'verification',
 *     startedAt: new Date().toISOString(),
 *   });
 *
 *   const { verifyIdentity } = Durable.workflow.proxyActivities<typeof activities>();
 *   await verifyIdentity(userId);
 *
 *   await Durable.workflow.enrich({ stage: 'complete' });
 * }
 * ```
 *
 * @param {StringStringType} fields - Key-value fields to write to the workflow record.
 * @returns {Promise<boolean>} `true` when enrichment is completed.
 */
export async function enrich(fields: StringStringType): Promise<boolean> {
  const searchSession = await search();
  await searchSession.set(fields);
  return true;
}
