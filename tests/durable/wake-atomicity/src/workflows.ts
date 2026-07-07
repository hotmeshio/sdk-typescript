import { Durable } from '../../../../services/durable';

// Parks on condition(), writing one hmsh_escalations row at suspension.
// Resolving that row must wake this workflow even if the resolver process
// dies immediately after the resolve transaction commits.
export async function crashWindowWorkflow(
  runId: string,
): Promise<{ approved: boolean; via: string }> {
  const signalId = `wake-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean; via: string }>(
    signalId,
    {
      role: 'crash-approver',
      type: 'wake-atomicity',
      priority: 1,
      description: `crash-window wake for ${runId}`,
      metadata: { runId },
    },
  );
  return result as { approved: boolean; via: string };
}

// SLA-gated wait: carries a timeout leg (armed timehook). When the
// signal wins, the armed timer must be disarmed — it may not survive
// to fire against the settled wait.
export async function timeoutGatedWorkflow(
  runId: string,
): Promise<{ approved: boolean } | false | null> {
  const signalId = `disarm-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean }>(
    signalId,
    {
      role: 'disarm-approver',
      type: 'timeout-disarm',
      priority: 1,
      metadata: { runId },
      timeout: '120 seconds',
    },
  );
  return result;
}

// Same SLA-gated wait, but preceded by durable ops so the waiter
// executes at a CYCLED dimensional address — the armed timer and the
// signal composite address the wait through different dimension math,
// and the disarm must still find the timer.
export async function cycledTimeoutGatedWorkflow(
  runId: string,
): Promise<{ approved: boolean } | false | null> {
  await Durable.workflow.sleep('1 second');
  await Durable.workflow.sleep('1 second');

  const signalId = `disarm-cycled-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean }>(
    signalId,
    {
      role: 'disarm-approver',
      type: 'timeout-disarm-cycled',
      priority: 1,
      metadata: { runId },
      timeout: '120 seconds',
    },
  );
  return result;
}
