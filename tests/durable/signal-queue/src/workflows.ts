import { Durable } from '../../../../services/durable';

export async function queuedApproval(orderId: string): Promise<Record<string, unknown>> {
  const signalId = `approve-order-${orderId}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(
    signalId,
    {
      role: 'pharmacist',
      type: 'approval',
      priority: 3,
      description: `Approve order ${orderId}`,
      metadata: { orderId, source: 'test' },
      envelope: { formSchema: { fields: [{ name: 'notes', type: 'text' }] } },
    },
  );
  if (result === false) return { timedOut: true };
  return result;
}

export async function queuedApprovalWithTimeout(orderId: string): Promise<Record<string, unknown>> {
  const signalId = `approve-order-timeout-${orderId}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(
    signalId,
    '10s',
    {
      role: 'pharmacist',
      type: 'approval',
      metadata: { orderId },
    },
  );
  if (result === false) return { timedOut: true };
  return result;
}
