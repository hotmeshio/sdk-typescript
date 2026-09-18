/**
 * Reciprocal adds: the container and a member row are written in ONE
 * statement, both or neither. The member is an accumulator of one whose
 * item key is the container's id; each side's entry points at the other.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import { Durable } from '../../../services/durable';
import { AccumulatorResult } from '../../../types/hmsh_escalations';
import { bootHarness, findPending, guid, sleepFor } from './src/harness';
import type { BinResult } from './src/workflows';

describe('DURABLE | escalations-accumulate | reciprocal | Postgres', () => {
  let client: Awaited<ReturnType<typeof bootHarness>>['client'];
  const taskQueue = 'escalation-accumulate-reciprocal-test';

  const startBin = (binKey: string, options: Record<string, unknown>) =>
    client.workflow.start({ args: [binKey, options], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180 });
  const startMember = (orderId: string, options?: Record<string, unknown>) =>
    client.workflow.start({ args: options ? [orderId, options] : [orderId], taskQueue, workflowName: 'memberWorkflow', workflowId: guid(), expire: 180 });

  beforeAll(async () => {
    ({ client } = await bootHarness(taskQueue, ['binWorkflow', 'memberWorkflow']));
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('writes both rows, completes the member, and completes the container at max', async () => {
    const binKey = guid();
    const binHandle = await startBin(binKey, { max: 2 });
    const bin = await findPending(client, 'bin', { binKey });
    const orderA = guid();
    const orderB = guid();
    const memberAHandle = await startMember(orderA);
    const memberBHandle = await startMember(orderB);
    const memberA = await findPending(client, 'member', { orderId: orderA });
    const memberB = await findPending(client, 'member', { orderId: orderB });

    const first = await client.escalations.accumulateItem({
      id: bin.id, itemKey: orderA, payload: { weight: 1 }, actor: 'scanner-1',
      reciprocal: { id: memberA.id, payload: { slot: 1 } },
    });
    expect(first.ok).toBe(true);
    if (!first.ok) return;
    expect(first.outcome).toBe('accepted');
    expect(first.count).toBe(1);
    expect(first.reciprocal?.outcome).toBe('completed');
    expect(first.reciprocal?.count).toBe(1);
    expect(first.reciprocal?.entry.status).toBe('resolved');

    // each entry points at the other row
    const binEntry = (first.entry.envelope as any).accumulate_items[orderA];
    expect(binEntry.reciprocalId).toBe(memberA.id);
    expect(binEntry.actor).toBe('scanner-1');
    const memberEntry = (first.reciprocal!.entry.envelope as any).accumulate_items[bin.id];
    expect(memberEntry.reciprocalId).toBe(bin.id);
    expect(memberEntry.payload).toEqual({ slot: 1 });
    expect(memberEntry.actor).toBe('scanner-1');

    // the member's waiter resumed with its one-item collection
    const memberOut = await memberAHandle.result<AccumulatorResult>();
    expect(memberOut.$trigger).toBe('count');
    expect(memberOut.$accumulated).toHaveLength(1);
    expect(memberOut.$accumulated[0].itemKey).toBe(bin.id);

    // "which container holds this order" reads from the GIN facet
    const holders = await client.escalations.list({ role: 'bin', metadata: { accumulate_keys: [orderA] } });
    expect(holders.map((h) => h.id)).toEqual([bin.id]);

    const second = await client.escalations.accumulateItem({
      id: bin.id, itemKey: orderB, reciprocal: { signalKey: memberB.signal_key! },
    });
    expect(second.ok && second.outcome === 'completed' && second.reciprocal?.outcome === 'completed').toBe(true);

    const binOut = await binHandle.result<{ payload: BinResult }>();
    expect(binOut.payload.$trigger).toBe('count');
    expect(binOut.payload.$accumulated.map((i) => i.itemKey)).toEqual([orderA, orderB]);
    expect(binOut.payload.$accumulated[1].reciprocalId).toBe(memberB.id);
    const memberBOut = await memberBHandle.result<AccumulatorResult>();
    expect(memberBOut.$accumulated[0].itemKey).toBe(bin.id);
  }, 40_000);

  it('leaves the container untouched when the reciprocal blocks', async () => {
    const binKey = guid();
    await startBin(binKey, { max: 5 });
    const bin = await findPending(client, 'bin', { binKey });

    // a member already resolved: reciprocal-terminal
    const doneOrder = guid();
    await startMember(doneOrder);
    const done = await findPending(client, 'member', { orderId: doneOrder });
    await client.escalations.resolve({ id: done.id, resolverPayload: {} });
    const terminal = await client.escalations.accumulateItem({ id: bin.id, itemKey: doneOrder, reciprocal: { id: done.id } });
    expect(terminal.ok).toBe(false);
    if (!terminal.ok) expect(terminal.outcome).toBe('reciprocal-terminal');

    // a capped member already holding one: reciprocal-full
    const cappedOrder = guid();
    await startMember(cappedOrder, { max: 1, resolveAtMax: false });
    const capped = await findPending(client, 'member', { orderId: cappedOrder });
    await client.escalations.accumulateItem({ id: capped.id, itemKey: 'elsewhere' });
    const full = await client.escalations.accumulateItem({ id: bin.id, itemKey: cappedOrder, reciprocal: { id: capped.id } });
    expect(full.ok).toBe(false);
    if (!full.ok) expect(full.outcome).toBe('reciprocal-full');

    // a member that already holds this container: reciprocal-duplicate
    const heldOrder = guid();
    await startMember(heldOrder, { max: 3 });
    const held = await findPending(client, 'member', { orderId: heldOrder });
    await client.escalations.accumulateItem({ id: held.id, itemKey: bin.id });
    const dup = await client.escalations.accumulateItem({ id: bin.id, itemKey: heldOrder, reciprocal: { id: held.id } });
    expect(dup.ok).toBe(false);
    if (!dup.ok) expect(dup.outcome).toBe('reciprocal-duplicate');

    // an unknown reciprocal: reciprocal-not-found
    const missing = await client.escalations.accumulateItem({
      id: bin.id, itemKey: guid(), reciprocal: { id: '00000000-0000-4000-8000-000000000000' },
    });
    expect(missing.ok).toBe(false);
    if (!missing.ok) expect(missing.outcome).toBe('reciprocal-not-found');

    // a non-accumulator reciprocal
    const plain = await client.escalations.create({ role: 'plain', metadata: { k: guid() } });
    const notAcc = await client.escalations.accumulateItem({ id: bin.id, itemKey: guid(), reciprocal: { id: plain.id } });
    expect(notAcc.ok).toBe(false);
    if (!notAcc.ok) expect(notAcc.outcome).toBe('reciprocal-not-accumulator');

    const untouched = await client.escalations.get(bin.id);
    expect((untouched!.metadata as any).accumulate_count).toBe(0);
    expect((untouched!.metadata as any).accumulate_keys).toEqual([]);
    await client.escalations.cancel(bin.id);
  }, 40_000);

  it('removes from both rows or neither', async () => {
    const binKey = guid();
    await startBin(binKey, { max: 5 });
    const bin = await findPending(client, 'bin', { binKey });
    const orderId = guid();
    await startMember(orderId, { max: 3 });
    const member = await findPending(client, 'member', { orderId });
    await client.escalations.accumulateItem({ id: bin.id, itemKey: orderId, reciprocal: { id: member.id } });

    const removed = await client.escalations.removeAccumulatedItem({ id: bin.id, itemKey: orderId, reciprocal: { id: member.id } });
    expect(removed.ok && removed.outcome === 'removed' && removed.count === 0 && removed.reciprocal?.count === 0).toBe(true);

    const again = await client.escalations.removeAccumulatedItem({ id: bin.id, itemKey: orderId, reciprocal: { id: member.id } });
    expect(again.ok).toBe(false);
    if (!again.ok) expect(again.outcome).toBe('item-absent');

    await client.escalations.accumulateItem({ id: bin.id, itemKey: orderId });
    const partial = await client.escalations.removeAccumulatedItem({ id: bin.id, itemKey: orderId, reciprocal: { id: member.id } });
    expect(partial.ok).toBe(false);
    if (!partial.ok) expect(partial.outcome).toBe('reciprocal-absent');
    const stillHeld = await client.escalations.get(bin.id);
    expect((stillHeld!.metadata as any).accumulate_keys).toEqual([orderId]);
    await client.escalations.cancel(bin.id);
    await client.escalations.cancel(member.id);
  }, 40_000);
});
