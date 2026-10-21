/**
 * Batch flags: a pending key can be flagged (and unflagged) in one guarded
 * statement without changing what the batch waits for. A row whose wait
 * declared `wakeOnFlag` resolves on the flag and the waiter wakes with the
 * items filled so far, `$flags`, and `$trigger: 'flag'`.
 */
import { randomUUID } from 'crypto';

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

describe('DURABLE | escalations-batch | flag | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-flag-test';

  const start = async (workflowName: string, orderId: string) =>
    client.workflow.start({ args: [orderId], taskQueue, workflowName, workflowId: guid(), expire: 180 });

  const findByOrder = async (role: string, orderId: string) => {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      const rows = await client.escalations.list({ role, status: 'pending' });
      const row = rows.find((e) => (e.metadata as any)?.orderId === orderId);
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
    for (const workflow of [workflows.batchFlagWorkflow, workflows.batchWakeOnFlagWorkflow]) {
      const worker = await Worker.create({ connection, taskQueue, workflow });
      await worker.run();
    }
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('without wakeOnFlag', () => {
    it('records a flag, keeps the key pending, and completes as any batch', async () => {
      const orderId = guid();
      const handle = await start('batchFlagWorkflow', orderId);
      const row = await findByOrder('assembly-flag', orderId);
      expect(row).toBeDefined();

      const flag = await client.escalations.flagBatchItem({
        id: row!.id, itemKey: 'weld', label: 'Missing', payload: { note: 'not on the pallet' }, actor: 'user-1',
        metadata: { lastFlag: 'weld' },
      });
      expect(flag.ok).toBe(true);
      if (!flag.ok) return;
      expect(flag.outcome).toBe('flagged');
      expect(flag.entry.status).toBe('pending');
      const stored = (flag.entry.envelope as any).batch_flags.weld;
      expect(stored).toMatchObject({ label: 'Missing', payload: { note: 'not on the pallet' }, actor: 'user-1' });
      expect(Date.parse(stored.at)).not.toBeNaN();
      expect((flag.entry.metadata as any).batch_pending).toEqual(['cut', 'weld', 'paint']);
      expect((flag.entry.metadata as any).batch_count).toBe(3);
      expect((flag.entry.metadata as any).lastFlag).toBe('weld');

      // flagging again replaces the flag; nothing else changes
      const again = await client.escalations.flagBatchItem({ id: row!.id, itemKey: 'weld', label: 'With a manager' });
      expect(again.ok && again.outcome).toBe('flagged');
      if (!again.ok) return;
      expect((again.entry.envelope as any).batch_flags.weld.label).toBe('With a manager');
      expect((again.entry.envelope as any).batch_flags.weld.actor).toBeUndefined();

      // the flagged key still fills, and the wait completes with the normal collection
      for (const itemKey of ['cut', 'weld', 'paint']) {
        const fill = await client.escalations.resolveBatchItem({
          id: row!.id, itemKey, payload: { station: `${itemKey}-1`, ok: true },
        });
        expect(fill.ok).toBe(true);
      }
      const output = (await handle.result()) as Record<string, any>;
      expect(output.weld).toEqual({ station: 'weld-1', ok: true });
      expect(output.$trigger).toBeUndefined();
      expect(output.$flags).toBeUndefined();

      // the flag stays on the terminal row for audit
      const done = await client.escalations.get(row!.id);
      expect(done!.status).toBe('resolved');
      expect((done!.envelope as any).batch_flags.weld.label).toBe('With a manager');
    }, 30_000);

    it('unflags a pending key once', async () => {
      const orderId = guid();
      await start('batchFlagWorkflow', orderId);
      const row = await findByOrder('assembly-flag', orderId);
      await client.escalations.flagBatchItem({ id: row!.id, itemKey: 'cut', label: 'Missing' });
      await client.escalations.flagBatchItem({ id: row!.id, itemKey: 'paint', label: 'Missing' });

      const cleared = await client.escalations.unflagBatchItem({ id: row!.id, itemKey: 'cut' });
      expect(cleared.ok).toBe(true);
      if (!cleared.ok) return;
      expect((cleared.entry.envelope as any).batch_flags).toEqual({
        paint: expect.objectContaining({ label: 'Missing' }),
      });
      expect(cleared.entry.status).toBe('pending');

      const twice = await client.escalations.unflagBatchItem({ id: row!.id, itemKey: 'cut' });
      expect(twice).toEqual({ ok: false, outcome: 'not-flagged' });
      const unknown = await client.escalations.unflagBatchItem({ id: row!.id, itemKey: 'nope' });
      expect(unknown).toEqual({ ok: false, outcome: 'unknown-item' });
    }, 20_000);

    it('keeps every flag when different keys are flagged at once', async () => {
      const orderId = guid();
      await start('batchFlagWorkflow', orderId);
      const row = await findByOrder('assembly-flag', orderId);
      const results = await Promise.all(['cut', 'weld', 'paint'].map((itemKey) =>
        client.escalations.flagBatchItem({ id: row!.id, itemKey, label: `flag-${itemKey}` })));
      expect(results.every((r) => r.ok && r.outcome === 'flagged')).toBe(true);
      const after = await client.escalations.get(row!.id);
      expect(Object.keys((after!.envelope as any).batch_flags).sort()).toEqual(['cut', 'paint', 'weld']);
      expect(after!.status).toBe('pending');
    }, 20_000);

    it('refuses a filled key, an unknown key, a missing row and a non-batch row', async () => {
      const orderId = guid();
      await start('batchFlagWorkflow', orderId);
      const row = await findByOrder('assembly-flag', orderId);
      await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'cut', payload: { ok: true } });

      expect(await client.escalations.flagBatchItem({ id: row!.id, itemKey: 'cut', label: 'Missing' }))
        .toEqual({ ok: false, outcome: 'already-filled' });
      expect(await client.escalations.flagBatchItem({ id: row!.id, itemKey: 'nope', label: 'Missing' }))
        .toEqual({ ok: false, outcome: 'unknown-item' });
      expect(await client.escalations.flagBatchItem({ id: randomUUID(), itemKey: 'cut', label: 'Missing' }))
        .toEqual({ ok: false, outcome: 'not-found' });

      const plain = await client.escalations.create({ role: 'assembly-flag-plain', metadata: { orderId } });
      expect(await client.escalations.flagBatchItem({ id: plain.id, itemKey: 'cut', label: 'Missing' }))
        .toEqual({ ok: false, outcome: 'not-batch' });
    }, 20_000);

    it('refuses a flag and an unflag on a row that is no longer pending', async () => {
      const row = await client.escalations.create({
        role: 'assembly-flag-standalone', metadata: { ticket: guid() }, batch: ['a', 'b'],
      });
      await client.escalations.flagBatchItem({ id: row.id, itemKey: 'b', label: 'Missing' });
      const cancelled = await client.escalations.cancel(row.id);
      expect(cancelled.ok).toBe(true);
      expect(await client.escalations.flagBatchItem({ id: row.id, itemKey: 'a', label: 'Missing' }))
        .toEqual({ ok: false, outcome: 'already-cancelled' });
      expect(await client.escalations.unflagBatchItem({ id: row.id, itemKey: 'b' }))
        .toEqual({ ok: false, outcome: 'already-cancelled' });
    }, 20_000);

    it('rejects a reserved batch key in the metadata patch before any write', async () => {
      await expect(client.escalations.flagBatchItem({
        id: guid(), itemKey: 'a', label: 'x', metadata: { batch_pending: [] },
      })).rejects.toThrow(/reserved/);
    });
  });

  describe('with wakeOnFlag', () => {
    it('stores the opt-in on the row at creation', async () => {
      const orderId = guid();
      await start('batchWakeOnFlagWorkflow', orderId);
      const row = await findByOrder('assembly-wake-flag', orderId);
      expect((row!.envelope as any).batch_wake_on_flag).toBe(true);
    }, 20_000);

    it('ends the wait with the items so far, the flags and the trigger', async () => {
      const orderId = guid();
      const handle = await start('batchWakeOnFlagWorkflow', orderId);
      const row = await findByOrder('assembly-wake-flag', orderId);
      await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'cut', payload: { station: 'cut-1', ok: true } });

      const flag = await client.escalations.flagBatchItem({
        id: row!.id, itemKey: 'weld', label: 'Missing', payload: { note: 'not on the pallet' },
        actor: 'user-1', resolvedBy: { id: 'user-1' },
      });
      expect(flag.ok && flag.outcome).toBe('completed');

      const output = (await handle.result()) as Record<string, any>;
      expect(output.cut).toEqual({ station: 'cut-1', ok: true });
      expect(output.weld).toBeUndefined();
      expect(output.$trigger).toBe('flag');
      expect(output.$flags.weld).toMatchObject({ label: 'Missing', payload: { note: 'not on the pallet' }, actor: 'user-1' });
      expect(output.$resolution).toMatchObject({ escalationId: row!.id, resolvedBy: 'user-1' });

      const done = await client.escalations.get(row!.id);
      expect(done!.status).toBe('resolved');
      expect((done!.resolver_payload as any).$trigger).toBe('flag');
      expect((done!.resolver_payload as any).$resolution).toBeUndefined();
      expect((done!.metadata as any).batch_pending).toEqual(['weld', 'paint']);

      const late = await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'paint', payload: { ok: true } });
      expect(late).toEqual({ ok: false, outcome: 'already-resolved' });
      expect(await client.escalations.flagBatchItem({ id: row!.id, itemKey: 'paint', label: 'Missing' }))
        .toEqual({ ok: false, outcome: 'already-resolved' });
    }, 30_000);

    it('lets exactly one of a flag and the completing fill end the wait', async () => {
      const orderId = guid();
      const handle = await start('batchWakeOnFlagWorkflow', orderId);
      const row = await findByOrder('assembly-wake-flag', orderId);
      await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'cut', payload: { ok: true } });
      await client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'weld', payload: { ok: true } });

      const [fill, flag] = await Promise.all([
        client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'paint', payload: { ok: true } }),
        client.escalations.flagBatchItem({ id: row!.id, itemKey: 'paint', label: 'Missing' }),
      ]);
      const winners = [fill.ok && fill.outcome === 'completed', flag.ok && flag.outcome === 'completed'];
      expect(winners.filter(Boolean)).toHaveLength(1);

      const output = (await handle.result()) as Record<string, any>;
      if (winners[0]) {
        expect(output.paint).toEqual({ ok: true });
        expect(output.$trigger).toBeUndefined();
        expect(flag).toEqual({ ok: false, outcome: 'already-resolved' });
      } else {
        expect(output.$trigger).toBe('flag');
        expect(fill).toEqual({ ok: false, outcome: 'already-resolved' });
      }
    }, 30_000);
  });
});
