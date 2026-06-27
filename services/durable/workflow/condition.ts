import {
  sleepImmediate,
  DurableWaitForError,
  DurableTelemetryService,
  HMSH_CODE_DURABLE_WAIT,
  s,
  asyncLocalStorage,
} from './common';
import { checkCancellation } from './cancellationScope';
import { didRun } from './didRun';
import { ConditionQueueConfig } from '../../../types/hmsh_escalations';

/**
 * Pauses the workflow until a signal with the given `signalId` is received.
 * The workflow suspends durably — it survives process restarts and will
 * resume exactly once when the matching `signal()` call delivers data.
 *
 * `condition` is the **receive** side of the signal coordination pair.
 * The **send** side is `signal()`, which can be called from another
 * workflow, a hook function, or externally via `client.workflow.signal()`.
 *
 * On replay, `condition` returns the previously stored signal payload
 * immediately — no actual suspension occurs.
 *
 * ## Basic usage
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * export async function approvalWorkflow(orderId: string): Promise<boolean> {
 *   const { submitForReview } = Durable.workflow.proxyActivities<typeof activities>();
 *   await submitForReview(orderId);
 *
 *   // Pause until a human approves or rejects
 *   const decision = await Durable.workflow.condition<{ approved: boolean }>('approval');
 *   return decision.approved;
 * }
 *
 * // From an API handler or another workflow:
 * await client.workflow.signal('approval', { approved: true });
 * ```
 *
 * ## With timeout
 *
 * Pass a duration string as the second argument to set a deadline.
 * `condition` returns `false` if the timeout fires before a signal arrives.
 *
 * ```typescript
 * const decision = await Durable.workflow.condition<{ approved: boolean }>(
 *   'approval',
 *   '72h',              // give reviewers 72 hours; returns false on timeout
 * );
 * if (decision === false) return 'auto-rejected-timeout';
 * return decision.approved ? 'approved' : 'rejected';
 * ```
 *
 * ## With escalation queue config
 *
 * Pass a {@link ConditionQueueConfig} as the second argument to surface the
 * pause as a claimable row in `public.hmsh_escalations`. The INSERT is
 * committed atomically with the workflow checkpoint — one write, no
 * enrichment step, no secondary round-trip.
 *
 * ```typescript
 * const decision = await Durable.workflow.condition<{ approved: boolean }>(
 *   'manager-approval',
 *   {
 *     role: 'manager',
 *     type: 'order-approval',
 *     subtype: 'regional',
 *     priority: 2,
 *     description: 'Approve or reject the regional order',
 *     metadata: { orderId, region },
 *     envelope: { instructions: 'Review the attached order' },
 *   },
 * );
 *
 * // Elsewhere: list, claim, then resolve (resumes the workflow)
 * const [item] = await client.escalations.list({ role: 'manager', status: 'pending' });
 * await client.escalations.claim({ id: item.id, assignee: 'alice@company.com' });
 * await client.escalations.resolve({ id: item.id, resolverPayload: { approved: true } });
 * ```
 *
 * ## Fan-in: wait for multiple signals in parallel
 *
 * ```typescript
 * const [name, score] = await Promise.all([
 *   Durable.workflow.condition<string>('name-signal'),
 *   Durable.workflow.condition<number>('score-signal'),
 * ]);
 * ```
 *
 * ## Paired with hook: spawn work, wait for its signal
 *
 * ```typescript
 * const signalId = `result-${Durable.workflow.random()}`;
 * await Durable.workflow.hook({
 *   taskQueue: 'processors',
 *   workflowName: 'processItem',
 *   args: [input, signalId],
 * });
 * return await Durable.workflow.condition<string>(signalId);
 * ```
 *
 * @template T - The type of data expected in the signal payload.
 * @param signalId - A unique signal identifier shared by the sender and receiver.
 * @param timeoutOrConfig - Optional timeout string (e.g. `'30s'`, `'24h'`) OR a
 *   {@link ConditionQueueConfig} that writes one row to `public.hmsh_escalations`
 *   atomically at suspension time. Cannot specify both; use the config object's
 *   `expiresAt` field for deadline enforcement when an escalation is involved.
 * @returns The signal payload, `false` if a timeout string was given and it expired,
 *   or `null` if the escalation was cancelled via `client.escalations.cancel()`.
 */
export async function condition<T>(
  signalId: string,
  timeoutOrConfig?: string | ConditionQueueConfig,
): Promise<T | false | null> {
  const timeout = typeof timeoutOrConfig === 'string' ? timeoutOrConfig : undefined;
  const queueConfig = timeoutOrConfig && typeof timeoutOrConfig === 'object' ? timeoutOrConfig : undefined;
  const [didRunAlready, execIndex, result] = await didRun('wait');
  checkCancellation();
  if (didRunAlready) {
    // Emit RETURN span in debug mode
    if (DurableTelemetryService.isVerbose() && result) {
      const store = asyncLocalStorage.getStore();
      const workflowTrace = store.get('workflowTrace');
      const workflowSpan = store.get('workflowSpan');
      if (workflowTrace && workflowSpan && result.ac && result.au) {
        DurableTelemetryService.emitDurationSpan(
          workflowTrace,
          workflowSpan,
          `RETURN/wait/${signalId}/${execIndex}`,
          DurableTelemetryService.parseTimestamp(result.ac),
          DurableTelemetryService.parseTimestamp(result.au),
          {
            'durable.operation.type': 'wait',
            'durable.signal.id': signalId,
            'durable.exec.index': execIndex,
          },
        );
      }
    }
    // If the condition timed out (timeout won the race), return false
    if (result?.timedOut) {
      return false;
    }
    const signalData = (result as { id: string; data: { data: T } }).data?.data;
    // If the escalation was cancelled via cancel(), the signal carries this marker.
    // Return null so the workflow can distinguish cancellation from a real resolution.
    if (signalData && typeof signalData === 'object' && (signalData as Record<string, unknown>).__escalation_cancelled === true) {
      return null;
    }
    return signalData as T;
  }

  const store = asyncLocalStorage.getStore();

  // Emit DISPATCH span in debug mode
  if (DurableTelemetryService.isVerbose()) {
    const workflowTrace = store.get('workflowTrace');
    const workflowSpan = store.get('workflowSpan');
    if (workflowTrace && workflowSpan) {
      DurableTelemetryService.emitPointSpan(
        workflowTrace,
        workflowSpan,
        `DISPATCH/wait/${signalId}/${execIndex}`,
        {
          'durable.operation.type': 'wait',
          'durable.signal.id': signalId,
          'durable.exec.index': execIndex,
        },
      );
    }
  }

  const interruptionRegistry = store.get('interruptionRegistry');
  const workflowId = store.get('workflowId');
  const workflowDimension = store.get('workflowDimension') ?? '';
  const interruptionMessage = {
    workflowId,
    signalId,
    index: execIndex,
    workflowDimension,
    type: 'DurableWaitForError',
    code: HMSH_CODE_DURABLE_WAIT,
    ...(timeout ? { duration: s(timeout) } : {}),
    // taskQueue + workflowName ride along so a collated wait (Promise.all) can
    // write a faithful hmsh_escalations row from inside the collator flow —
    // matching the inline waiter, which sources these from its trigger.
    ...(queueConfig
      ? {
          queueConfig,
          taskQueue: store.get('taskQueue'),
          workflowName: store.get('workflowName'),
        }
      : {}),
  };
  interruptionRegistry.push(interruptionMessage);

  await sleepImmediate();
  //if you are seeing this error in the logs, you might have forgotten to `await condition(...)`
  throw new DurableWaitForError(interruptionMessage);
}
