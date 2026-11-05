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

// Batch wait that starts with `cut` already satisfied: the row parks at
// one of three filled, and the collection delivered at completion carries
// the seeded entry with the two filled later.
export async function batchFilledWorkflow(
  orderId: string,
  cutAt: string,
): Promise<BatchCollection | false | null> {
  const signalId = `batch-filled-${Durable.guid()}`;
  return Durable.workflow.condition<BatchCollection>(signalId, {
    role: 'assembly-filled',
    type: 'batch-filled',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
    filled: {
      cut: { payload: { station: 'cut-0', ok: true }, at: cutAt, actor: 'user-cut' },
    },
  });
}

// Batch wait whose flags are recorded while the wait continues: the
// collection is delivered at completion as on any batch.
export async function batchFlagWorkflow(orderId: string): Promise<BatchCollection | false | null> {
  const signalId = `batch-flag-${Durable.guid()}`;
  return Durable.workflow.condition<BatchCollection>(signalId, {
    role: 'assembly-flag',
    type: 'batch-flag',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
  });
}

// Batch wait that a flag ends: the wait resumes with the items filled so
// far, `$flags`, and `$trigger: 'flag'`.
export async function batchWakeOnFlagWorkflow(orderId: string): Promise<Record<string, unknown> | false | null> {
  const signalId = `batch-wake-flag-${Durable.guid()}`;
  return Durable.workflow.condition<Record<string, unknown>>(signalId, {
    role: 'assembly-wake-flag',
    type: 'batch-wake-flag',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
    wakeOnFlag: true,
  });
}

// Batch wait that a fill ends: the wait resumes with the items filled so
// far, `$filled`, `$filledAt`, `$filledBy`, and `$trigger: 'fill'`. `keys`
// limits the wake to those keys; omitted, every fill wakes it.
export async function batchWakeOnFillWorkflow(
  orderId: string,
  keys?: string[],
): Promise<Record<string, unknown> | false | null> {
  const signalId = `batch-wake-fill-${Durable.guid()}`;
  return Durable.workflow.condition<Record<string, unknown>>(signalId, {
    role: 'assembly-wake-fill',
    type: 'batch-wake-fill',
    priority: 2,
    metadata: { orderId },
    batch: ['cut', 'weld', 'paint'],
    wakeOnFill: keys ?? true,
  });
}

// One wait per round: each fill wakes the workflow, which records the key
// and parks the next round with every fill so far carried forward through
// `filled`, until the last key completes the batch.
export async function batchFillRoundsWorkflow(
  orderId: string,
): Promise<{ woken: string[]; rounds: number; collection: Record<string, unknown> }> {
  const keys = ['cut', 'weld', 'paint'];
  const woken: string[] = [];
  let filled: Record<string, { payload: Record<string, unknown>; at: string; actor?: string }> | undefined;
  let rounds = 0;
  for (;;) {
    rounds++;
    const out = await Durable.workflow.condition<Record<string, any>>(`batch-rounds-${Durable.guid()}`, {
      role: 'assembly-rounds',
      type: 'batch-rounds',
      priority: 2,
      metadata: { orderId, round: rounds },
      batch: keys,
      wakeOnFill: true,
      ...(filled ? { filled } : {}),
    });
    if (!out || out.$trigger !== 'fill') {
      return { woken, rounds, collection: (out || {}) as Record<string, unknown> };
    }
    woken.push(...out.$filled);
    filled = {};
    for (const key of keys) {
      if (!(key in out)) continue;
      filled[key] = {
        payload: out[key],
        at: out.$filledAt[key],
        ...(out.$filledBy?.[key] ? { actor: out.$filledBy[key] } : {}),
      };
    }
  }
}
