/**
 * Batch fill wake: a row whose wait declared `wakeOnFill` resolves on a fill
 * and the waiter wakes with the items filled so far, `$filled`, `$filledAt`,
 * `$filledBy`, and `$trigger: 'fill'`. The last fill completes the wait as on
 * any batch, and a workflow can park its next round with every fill carried
 * forward through `filled`.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

describe('DURABLE | escalations-batch | fill wake | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-fill-wake-test';

  const start = async (workflowName: string, args: unknown[]) =>
    client.workflow.start({ args, taskQueue, workflowName, workflowId: guid(), expire: 180 });

  const findPending = async (role: string, match: Record<string, unknown>) => {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      const rows = await client.escalations.list({ role, status: 'pending' });
      const row = rows.find((e) =>
        Object.entries(match).every(([k, v]) => (e.metadata as any)?.[k] === v));
      if (row) return row;
      await sleepFor(25);
    }
    return undefined;
  };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
    client = new Client({ connection });
    for (const workflow of [workflows.batchWakeOnFillWorkflow, workflows.batchFillRoundsWorkflow]) {
      const worker = await Worker.create({ connection, taskQueue, workflow });
      await worker.run();
    }
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('stores the opt-in on the row at creation', async () => {
    const all = guid();
    const some = guid();
    await start('batchWakeOnFillWorkflow', [all]);
    await start('batchWakeOnFillWorkflow', [some, ['weld']]);
    expect(((await findPending('assembly-wake-fill', { orderId: all }))!.envelope as any).batch_wake_on_fill).toBe(true);
    expect(((await findPending('assembly-wake-fill', { orderId: some }))!.envelope as any).batch_wake_on_fill).toEqual(['weld']);
  }, 20_000);

  it('a fill ends the wait with the items so far, the key, its time and actor, and the trigger', async () => {
    const orderId = guid();
    const handle = await start('batchWakeOnFillWorkflow', [orderId]);
    const row = await findPending('assembly-wake-fill', { orderId });

    const fill = await client.escalations.resolveBatchItem({
      id: row!.id, itemKey: 'cut', payload: { station: 'cut-1', ok: true }, actor: 'user-1', resolvedBy: { id: 'user-1' },
    });
    expect(fill.ok && fill.outcome).toBe('woke');
    if (!fill.ok) return;
    expect(fill.remaining).toBe(2);
    expect(fill.entry.status).toBe('resolved');

    const output = (await handle.result()) as Record<string, any>;
    expect(output.cut).toEqual({ station: 'cut-1', ok: true });
    expect(output.weld).toBeUndefined();
    expect(output.$trigger).toBe('fill');
    expect(output.$filled).toEqual(['cut']);
    const stored = (fill.entry.envelope as any).batch_filled_at.cut;
    expect(Date.parse(output.$filledAt.cut)).toBe(Date.parse(stored));
    expect(output.$filledBy).toEqual({ cut: 'user-1' });
    expect(output.$resolution).toMatchObject({ escalationId: row!.id, resolvedBy: 'user-1' });

    const done = await client.escalations.get(row!.id);
    expect(done!.status).toBe('resolved');
    expect((done!.resolver_payload as any).$trigger).toBe('fill');
    expect((done!.resolver_payload as any).$resolution).toBeUndefined();
    expect((done!.metadata as any).batch_pending).toEqual(['weld', 'paint']);
    expect((done!.metadata as any).batch_count).toBe(2);

    expect(await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'weld', payload: { ok: true } }))
      .toEqual({ ok: false, outcome: 'already-resolved' });
  }, 30_000);

  it('a fill with no actor delivers its time and leaves $filledBy out', async () => {
    const orderId = guid();
    const handle = await start('batchWakeOnFillWorkflow', [orderId]);
    const row = await findPending('assembly-wake-fill', { orderId });
    await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'paint', payload: { ok: true } });
    const output = (await handle.result()) as Record<string, any>;
    expect(output.$filled).toEqual(['paint']);
    expect(Object.keys(output.$filledAt)).toEqual(['paint']);
    expect(output.$filledBy).toBeUndefined();
  }, 30_000);

  it('a key list wakes only on a listed key', async () => {
    const orderId = guid();
    const handle = await start('batchWakeOnFillWorkflow', [orderId, ['weld']]);
    const row = await findPending('assembly-wake-fill', { orderId });

    const cut = await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'cut', payload: { ok: true } });
    expect(cut.ok && cut.outcome).toBe('accepted');
    expect((await client.escalations.get(row!.id))!.status).toBe('pending');

    const weld = await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'weld', payload: { ok: true } });
    expect(weld.ok && weld.outcome).toBe('woke');
    const output = (await handle.result()) as Record<string, any>;
    expect(output).toMatchObject({ cut: { ok: true }, weld: { ok: true }, $filled: ['weld'], $trigger: 'fill' });
    expect(Object.keys(output.$filledAt).sort()).toEqual(['cut', 'weld']);
  }, 30_000);

  it('the last fill completes as on any batch', async () => {
    const orderId = guid();
    const handle = await start('batchWakeOnFillWorkflow', [orderId, ['paint']]);
    const row = await findPending('assembly-wake-fill', { orderId });
    for (const itemKey of ['cut', 'weld']) {
      await client.escalations.resolveBatchItem({ id: row!.id, itemKey, payload: { ok: true } });
    }
    const last = await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'paint', payload: { ok: true } });
    expect(last.ok && last.outcome).toBe('completed');
    if (!last.ok) return;
    expect(last.remaining).toBe(0);
    const output = (await handle.result()) as Record<string, any>;
    expect(output).toEqual({ cut: { ok: true }, weld: { ok: true }, paint: { ok: true } });
  }, 30_000);

  it('wakes through the by-metadata form', async () => {
    const orderId = guid();
    const handle = await start('batchWakeOnFillWorkflow', [orderId]);
    await findPending('assembly-wake-fill', { orderId });
    const fill = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId', value: orderId, roles: ['assembly-wake-fill'], itemKey: 'weld', payload: { ok: true }, actor: 'user-2',
    });
    expect(fill.ok && fill.outcome).toBe('woke');
    const output = (await handle.result()) as Record<string, any>;
    expect(output).toMatchObject({ weld: { ok: true }, $filled: ['weld'], $filledBy: { weld: 'user-2' }, $trigger: 'fill' });
  }, 30_000);

  it('lets exactly one of two concurrent fills wake the wait', async () => {
    const orderId = guid();
    const handle = await start('batchWakeOnFillWorkflow', [orderId]);
    const row = await findPending('assembly-wake-fill', { orderId });
    const [cut, weld] = await Promise.all(['cut', 'weld'].map((itemKey) =>
      client.escalations.resolveBatchItem({ id: row!.id, itemKey, payload: { ok: true } })));
    const woke = [cut, weld].filter((r) => r.ok && r.outcome === 'woke');
    const late = [cut, weld].filter((r) => !r.ok && r.outcome === 'already-resolved');
    expect(woke).toHaveLength(1);
    expect(late).toHaveLength(1);
    const output = (await handle.result()) as Record<string, any>;
    expect(output.$filled).toHaveLength(1);
    expect(output.$trigger).toBe('fill');
  }, 30_000);

  it('resolves a standalone row on its wake key and reports woke', async () => {
    const row = await client.escalations.create({
      role: 'assembly-wake-fill-standalone', metadata: { ticket: guid() }, batch: ['a', 'b'], wakeOnFill: ['a'],
    });
    const fill = await client.escalations.resolveBatchItem({ id: row.id, itemKey: 'a', payload: { ok: true } });
    expect(fill.ok && fill.outcome).toBe('woke');
    const done = await client.escalations.get(row.id);
    expect(done!.status).toBe('resolved');
    expect((done!.resolver_payload as any)).toMatchObject({ a: { ok: true }, $filled: ['a'], $trigger: 'fill' });
  }, 20_000);

  it('parks round after round with every fill carried forward until the last key completes', async () => {
    const orderId = guid();
    const handle = await start('batchFillRoundsWorkflow', [orderId]);

    const first = await findPending('assembly-rounds', { orderId, round: 1 });
    await client.escalations.resolveBatchItem({ id: first!.id, itemKey: 'weld', payload: { box: 'W' }, actor: 'dock-1' });
    const firstAt = (await client.escalations.get(first!.id))!.envelope as any;

    const second = await findPending('assembly-rounds', { orderId, round: 2 });
    expect((second!.metadata as any).batch_pending).toEqual(['cut', 'paint']);
    expect((second!.envelope as any).batch_items).toEqual({ weld: { box: 'W' } });
    expect(Date.parse((second!.envelope as any).batch_filled_at.weld)).toBe(Date.parse(firstAt.batch_filled_at.weld));
    expect((second!.envelope as any).batch_filled_by).toEqual({ weld: 'dock-1' });
    await client.escalations.resolveBatchItem({ id: second!.id, itemKey: 'cut', payload: { box: 'C' } });

    const third = await findPending('assembly-rounds', { orderId, round: 3 });
    expect((third!.metadata as any).batch_pending).toEqual(['paint']);
    const last = await client.escalations.resolveBatchItem({ id: third!.id, itemKey: 'paint', payload: { box: 'P' } });
    expect(last.ok && last.outcome).toBe('completed');

    const output = (await handle.result()) as { woken: string[]; rounds: number; collection: Record<string, unknown> };
    expect(output.woken).toEqual(['weld', 'cut']);
    expect(output.rounds).toBe(3);
    expect(output.collection).toEqual({ weld: { box: 'W' }, cut: { box: 'C' }, paint: { box: 'P' } });
  }, 45_000);
});
