import {
  sleepImmediate,
  DurableSleepError,
  HMSH_CODE_DURABLE_SLEEP,
  s,
  asyncLocalStorage,
} from './common';
import { didRun } from './didRun';

/**
 * Suspends workflow execution for a durable, crash-safe duration. Unlike
 * `setTimeout`, this sleep survives process restarts — the engine persists
 * the wake-up time and resumes the workflow when the timer expires.
 *
 * On replay, `sleepFor` returns immediately with the stored duration
 * (no actual waiting occurs). This makes it safe for deterministic
 * re-execution.
 *
 * ## Duration Formats
 *
 * Accepts any human-readable duration string parsed by the `ms` module:
 * `'5 seconds'`, `'30s'`, `'2 minutes'`, `'1m'`, `'1 hour'`, `'2h'`,
 * `'1 day'`, `'7d'`.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Simple delay before continuing
 * export async function reminderWorkflow(userId: string): Promise<void> {
 *   const { sendReminder } = Durable.workflow.proxyActivities<typeof activities>();
 *
 *   // Wait 24 hours (survives server restarts)
 *   await Durable.workflow.sleepFor('24 hours');
 *   await sendReminder(userId, 'Your trial expires tomorrow');
 *
 *   // Wait another 6 days
 *   await Durable.workflow.sleepFor('6 days');
 *   await sendReminder(userId, 'Your trial has expired');
 * }
 * ```
 *
 * ```typescript
 * // Exponential backoff with retry loop
 * export async function pollingWorkflow(resourceId: string): Promise<string> {
 *   const { checkStatus } = Durable.workflow.proxyActivities<typeof activities>();
 *
 *   for (let attempt = 0; attempt < 10; attempt++) {
 *     const status = await checkStatus(resourceId);
 *     if (status === 'ready') return status;
 *
 *     // Exponential backoff: 1s, 2s, 4s, 8s, ...
 *     const delay = Math.pow(2, attempt);
 *     await Durable.workflow.sleepFor(`${delay} seconds`);
 *   }
 *   return 'timeout';
 * }
 * ```
 *
 * ```typescript
 * // Race a sleep against an activity
 * const [result, _] = await Promise.all([
 *   activities.fetchData(id),
 *   Durable.workflow.sleepFor('30 seconds'),
 * ]);
 * ```
 *
 * @param {string} duration - A human-readable duration string.
 * @returns {Promise<number>} The resolved duration in seconds.
 */
export async function sleepFor(duration: string): Promise<number> {
  const [didRunAlready, execIndex, result] = await didRun('sleep');
  if (didRunAlready) {
    return (result as { completion: string; duration: number }).duration;
  }
  const store = asyncLocalStorage.getStore();
  const interruptionRegistry = store.get('interruptionRegistry');
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const interruptionMessage = {
    workflowId,
    duration: s(duration),
    index: execIndex,
    workflowDimension,
  };
  interruptionRegistry.push({
    code: HMSH_CODE_DURABLE_SLEEP,
    type: 'DurableSleepError',
    ...interruptionMessage,
  });

  await sleepImmediate();
  throw new DurableSleepError(interruptionMessage);
}
