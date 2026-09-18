import { Durable } from '../../../../services/durable';
import { AccumulatorResult } from '../../../../types/hmsh_escalations';

export type Bag = { weight: number };
export type BinResult = AccumulatorResult<Bag, { shippedBy?: string }>;

export type AccumulateOptions = { max?: number; resolveAtMax?: boolean; unique?: boolean };

// A container that fills over time: resolves at `max` (count), on the SLA
// timer (timeout), or on a manual resolve; never with false.
export async function binWorkflow(
  binKey: string,
  options: AccumulateOptions = {},
  timeout?: string,
): Promise<{ outcome: 'delivered' | 'false' | 'cancelled'; payload: BinResult | null }> {
  const signalId = `bin-${Durable.guid()}`;
  const result = await Durable.workflow.condition<BinResult>(signalId, {
    role: 'bin',
    type: 'rollup',
    priority: 2,
    description: `Bin ${binKey}`,
    metadata: { binKey },
    envelope: { instructions: 'Scan each bag into the bin' },
    accumulate: options,
    ...(timeout ? { timeout } : {}),
  });
  if (result === false) return { outcome: 'false', payload: null };
  if (result === null) return { outcome: 'cancelled', payload: null };
  return { outcome: 'delivered', payload: result };
}

// A member that joins exactly one container: its own accumulator of one.
export async function memberWorkflow(
  orderId: string,
  options: AccumulateOptions = { max: 1 },
): Promise<AccumulatorResult<Record<string, unknown>> | false | null> {
  const signalId = `member-${Durable.guid()}`;
  return Durable.workflow.condition<AccumulatorResult<Record<string, unknown>>>(signalId, {
    role: 'member',
    type: 'order',
    priority: 3,
    metadata: { orderId },
    accumulate: options,
  });
}

// Bounded batch that opts into partial delivery on timeout.
export async function batchPartialWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ outcome: 'delivered' | 'false'; payload: unknown }> {
  const signalId = `batch-partial-${Durable.guid()}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(signalId, {
    role: 'assembly-partial',
    type: 'batch-partial',
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
    partialOnTimeout: true,
    timeout,
  });
  return result === false ? { outcome: 'false', payload: false } : { outcome: 'delivered', payload: result };
}
