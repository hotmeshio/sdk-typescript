/**
 * Timeout is a delivery, not a failure: the SLA timer resumes the waiter
 * with the collection held so far and `$trigger: 'timeout'`, the row is
 * `expired` with the same payload stored, and late adds name the deadline.
 * A batch row opts into the same delivery with `partialOnTimeout`.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import { Durable } from '../../../services/durable';
import { bootHarness, findPending, guid, sleepFor } from './src/harness';
import type { BinResult } from './src/workflows';

describe('DURABLE | escalations-accumulate | timeout | Postgres', () => {
  let client: Awaited<ReturnType<typeof bootHarness>>['client'];
  const taskQueue = 'escalation-accumulate-timeout-test';

  beforeAll(async () => {
    ({ client } = await bootHarness(taskQueue, ['binWorkflow', 'batchPartialWorkflow']));
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('delivers the partial collection of an unbounded accumulator when the timer wins', async () => {
    const binKey = guid();
    const handle = await client.workflow.start({
      args: [binKey, {}, '6s'], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    const row = await findPending(client, 'bin', { binKey });
    expect((row.metadata as any).accumulate_max).toBeNull();
    const add = await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-1', payload: { weight: 4 } });
    expect(add.ok && add.remaining === null).toBe(true);

    const output = await handle.result<{ outcome: string; payload: BinResult }>();
    expect(output.outcome).toBe('delivered');
    expect(output.payload.$trigger).toBe('timeout');
    expect(output.payload.$accumulated.map((i) => i.itemKey)).toEqual(['bag-1']);
    expect(output.payload.$accumulated[0].payload).toEqual({ weight: 4 });

    // row truth agrees with the delivered value
    const expired = await client.escalations.get(row.id);
    expect(expired!.status).toBe('expired');
    expect((expired!.resolver_payload as any).$trigger).toBe('timeout');
    expect((expired!.resolver_payload as any).$accumulated).toHaveLength(1);
    expect((expired!.metadata as any).accumulate_count).toBe(1);

    const late = await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-2' });
    expect(late.ok).toBe(false);
    if (!late.ok) expect(late.outcome).toBe('already-expired');
  }, 60_000);

  it('delivers an empty collection when nothing arrived before the deadline', async () => {
    const binKey = guid();
    const handle = await client.workflow.start({
      args: [binKey, { max: 3 }, '4s'], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    await findPending(client, 'bin', { binKey });
    const output = await handle.result<{ outcome: string; payload: BinResult }>();
    expect(output.outcome).toBe('delivered');
    expect(output.payload).toEqual({ $accumulated: [], $trigger: 'timeout' });
  }, 60_000);

  it('a batch row with partialOnTimeout delivers its filled items instead of false', async () => {
    const orderId = guid();
    const handle = await client.workflow.start({
      args: [orderId, '5s'], taskQueue, workflowName: 'batchPartialWorkflow', workflowId: guid(), expire: 180,
    });
    const row = await findPending(client, 'assembly-partial', { orderId });
    expect((row.envelope as any).batch_partial_on_timeout).toBe(true);
    await client.escalations.resolveBatchItem({ id: row.id, itemKey: 'cut', payload: { station: 'cut-1', ok: true } });

    const output = await handle.result<{ outcome: string; payload: Record<string, unknown> }>();
    expect(output.outcome).toBe('delivered');
    expect(output.payload).toEqual({ cut: { station: 'cut-1', ok: true }, $trigger: 'timeout' });
    const expired = await client.escalations.get(row.id);
    expect(expired!.status).toBe('expired');
    expect((expired!.resolver_payload as any).$trigger).toBe('timeout');
  }, 60_000);
});
