import { Durable } from '../../../../services/durable';

// Pauses until a signal arrives, writing an hmsh_escalations row at suspension.
export async function approvalWorkflow(orderId: string, region: string): Promise<{ approved: boolean; approvedBy: string }> {
  const signalId = `approval-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean; approvedBy: string }>(
    signalId,
    {
      role: 'approver',
      type: 'order-approval',
      subtype: 'regional',
      priority: 2,
      description: `Approve order ${orderId}`,
      metadata: { orderId, region },
      envelope: { instructions: 'Review and approve or reject the order' },
    },
  );
  return result as { approved: boolean; approvedBy: string };
}

// One wait, two outcomes: an escalation row AND a resume timer. Resolving the
// row delivers the payload; the timer firing first resumes with false (and the
// engine transitions the row to 'expired').
export async function slaGatedWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ outcome: 'resolved' | 'timed-out'; payload: unknown }> {
  const signalId = `sla-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean }>(
    signalId,
    {
      role: 'sla-approver',
      type: 'sla-gate',
      priority: 2,
      description: `SLA-gated approval for ${orderId}`,
      metadata: { orderId },
      timeout,
    },
  );
  return result === false
    ? { outcome: 'timed-out', payload: false }
    : { outcome: 'resolved', payload: result };
}

// Pauses at a condition and returns null when the escalation is cancelled,
// or the resolver payload when resolved normally.
export async function cancelAwareWorkflow(orderId: string): Promise<{ approved?: boolean; __escalation_cancelled?: boolean } | null> {
  const signalId = `cancel-aware-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved?: boolean }>(
    signalId,
    {
      role: 'cancel-test-approver',
      type: 'cancel-test',
      priority: 5,
      metadata: { orderId },
    },
  );
  // null = escalation was cancelled; pass it through so the test can assert it
  return result as { approved?: boolean } | null;
}
