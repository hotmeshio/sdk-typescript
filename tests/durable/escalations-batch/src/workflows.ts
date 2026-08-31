import { Durable } from '../../../../services/durable';
import { EscalationResolution } from '../../../../types/hmsh_escalations';

export type StationResult = { station: string; ok: boolean };
export type BatchCollection = Record<'cut' | 'weld' | 'paint', StationResult> & {
  $resolution?: EscalationResolution;
};

// One wait, three contributions: the escalation resolves only when the last
// declared item is filled, delivering the full collection.
export async function batchWorkflow(orderId: string): Promise<BatchCollection | false | null> {
  const signalId = `batch-${Durable.guid()}`;
  return Durable.workflow.condition<BatchCollection>(signalId, {
    role: 'assembly',
    type: 'batch-order',
    priority: 2,
    description: `Assemble order ${orderId}`,
    metadata: { orderId },
    envelope: { instructions: 'Each station submits its result' },
    batch: ['cut', 'weld', 'paint'],
  });
}

// Batch accumulator with an SLA: the timer firing first resumes with false
// and expires the row, preserving any partially filled items for audit.
export async function batchSlaWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ outcome: 'resolved' | 'timed-out'; payload: unknown }> {
  const signalId = `batch-sla-${Durable.guid()}`;
  const result = await Durable.workflow.condition<BatchCollection>(signalId, {
    role: 'assembly-sla',
    type: 'batch-sla',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
    timeout,
  });
  return result === false
    ? { outcome: 'timed-out', payload: false }
    : { outcome: 'resolved', payload: result };
}

// Batch accumulator that passes a cancellation (null) through to the test.
export async function batchCancelWorkflow(orderId: string): Promise<BatchCollection | false | null> {
  const signalId = `batch-cancel-${Durable.guid()}`;
  return Durable.workflow.condition<BatchCollection>(signalId, {
    role: 'assembly-cancel',
    type: 'batch-cancel',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
  });
}

// Two-item batch used by the distinct-final-key race: both remaining items
// are filled concurrently and exactly one caller must observe 'completed'.
export async function batchRaceWorkflow(orderId: string): Promise<BatchCollection | false | null> {
  const signalId = `batch-race-${Durable.guid()}`;
  return Durable.workflow.condition<BatchCollection>(signalId, {
    role: 'assembly-race',
    type: 'batch-race',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
  });
}
