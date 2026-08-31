/**
 * Proves batched escalation resolution: one hmsh_escalations row accumulates
 * N item payloads via resolveBatchItem() and only resolves — waking the
 * condition() waiter with the full collection — when the LAST item lands.
 * Every fill is one atomic statement (guarded fill + facet recompute +
 * resolve-on-zero + wake), consonant with resolve()/resolveByMetadata().
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | escalations-batch | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-test';

  const startBatch = async (workflowName: string, orderId: string) =>
    client.workflow.start({
      args: [orderId],
      taskQueue,
      workflowName,
      workflowId: guid(),
    });

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

    const conn = await Connection.connect(connection);
    expect(conn).toBeDefined();
    client = new Client({ connection });

    for (const workflow of [
      workflows.batchWorkflow,
      workflows.batchRaceWorkflow,
    ]) {
      const worker = await Worker.create({ connection, taskQueue, workflow });
      await worker.run();
    }
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('batch fold — Leg1 atomicity', () => {
    it('writes the accumulator shape in the same commit as the wait', async () => {
      const orderId = guid();
      await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);
      expect(row).toBeDefined();
      // First sighting is complete: facets, accumulator, and routing together.
      expect((row!.metadata as any).batch_pending).toEqual(['cut', 'weld', 'paint']);
      expect((row!.metadata as any).batch_count).toBe(3);
      expect((row!.metadata as any).batch_keys).toEqual(['cut', 'weld', 'paint']);
      expect((row!.envelope as any).batch_items).toEqual({});
      expect((row!.envelope as any).instructions).toBe('Each station submits its result');
      expect(row!.signal_key).not.toBeNull();

      // batch_pending is a GIN-queryable facet: find rows still missing 'weld'.
      const missingWeld = await client.escalations.list({
        role: 'assembly',
        metadata: { batch_pending: ['weld'] },
      });
      expect(missingWeld.some((e) => e.id === row!.id)).toBe(true);

      await client.escalations.cancel(row!.id);
    }, 20_000);
  });

  describe('fill lifecycle: accepted → accepted → completed', () => {
    it('resolves only on the last item and resumes the workflow with the full collection', async () => {
      const orderId = guid();
      const handle = await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);
      expect(row).toBeDefined();

      const first = await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'cut',
        payload: { station: 'cut-1', ok: true },
      });
      expect(first.ok).toBe(true);
      if (!first.ok) return;
      expect(first.outcome).toBe('accepted');
      expect(first.remaining).toBe(2);
      expect(first.entry.status).toBe('pending');
      expect((first.entry.metadata as any).batch_pending).toEqual(['weld', 'paint']);
      expect((first.entry.metadata as any).batch_count).toBe(2);
      expect((first.entry.envelope as any).batch_items.cut).toEqual({ station: 'cut-1', ok: true });

      const second = await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'weld',
        payload: { station: 'weld-1', ok: true },
        metadata: { weldedBy: 'alice' },
      });
      expect(second.ok).toBe(true);
      if (!second.ok) return;
      expect(second.outcome).toBe('accepted');
      expect(second.remaining).toBe(1);
      // the caller's metadata patch merged in the same statement, without
      // touching the computed batch facets
      expect((second.entry.metadata as any).weldedBy).toBe('alice');
      expect((second.entry.metadata as any).batch_count).toBe(1);

      const resultPromise = handle.result();
      const last = await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'paint',
        payload: { station: 'paint-1', ok: true },
        resolvedBy: { id: 'painter-1', email: 'painter-1@example.com' },
      });
      expect(last.ok).toBe(true);
      if (!last.ok) return;
      expect(last.outcome).toBe('completed');
      expect(last.remaining).toBe(0);
      expect(last.entry.status).toBe('resolved');
      // the stored resolver_payload is the assembled collection
      expect((last.entry.resolver_payload as any).cut).toEqual({ station: 'cut-1', ok: true });
      expect((last.entry.resolver_payload as any).weld).toEqual({ station: 'weld-1', ok: true });
      expect((last.entry.resolver_payload as any).paint).toEqual({ station: 'paint-1', ok: true });
      expect((last.entry.resolver_payload as any).$resolution).toBeUndefined();

      // the waiting workflow received the full collection + $resolution
      const output = (await resultPromise) as workflows.BatchCollection;
      expect(output.cut).toEqual({ station: 'cut-1', ok: true });
      expect(output.weld).toEqual({ station: 'weld-1', ok: true });
      expect(output.paint).toEqual({ station: 'paint-1', ok: true });
      expect(output.$resolution?.escalationId).toBe(row!.id);
      expect(output.$resolution?.resolvedBy).toBe('painter-1');
      expect(output.$resolution?.resolvedByEmail).toBe('painter-1@example.com');
    }, 30_000);
  });

  describe('fill guards', () => {
    let rowId: string;

    beforeAll(async () => {
      const orderId = guid();
      await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);
      rowId = row!.id;
      const filled = await client.escalations.resolveBatchItem({
        id: rowId,
        itemKey: 'cut',
        payload: { station: 'cut-1', ok: true },
      });
      expect(filled.ok).toBe(true);
    }, 20_000);

    it('rejects a duplicate fill of an already-filled key without touching the row', async () => {
      const dup = await client.escalations.resolveBatchItem({
        id: rowId,
        itemKey: 'cut',
        payload: { station: 'cut-2', ok: false },
      });
      expect(dup.ok).toBe(false);
      if (dup.ok) return;
      expect(dup.outcome).toBe('duplicate-item');

      const row = await client.escalations.get(rowId);
      expect((row!.envelope as any).batch_items.cut).toEqual({ station: 'cut-1', ok: true });
      expect((row!.metadata as any).batch_count).toBe(2);
    }, 5_000);

    it('rejects an undeclared item key', async () => {
      const unknown = await client.escalations.resolveBatchItem({
        id: rowId,
        itemKey: 'polish',
        payload: { station: 'polish-1', ok: true },
      });
      expect(unknown.ok).toBe(false);
      if (unknown.ok) return;
      expect(unknown.outcome).toBe('unknown-item');
    }, 5_000);

    it('rejects a fill against a row with no batch declaration', async () => {
      const plain = await client.escalations.create({
        role: 'assembly',
        type: 'plain',
        metadata: { plainTest: guid() },
      });
      const result = await client.escalations.resolveBatchItem({
        id: plain.id,
        itemKey: 'cut',
        payload: { ok: true },
      });
      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.outcome).toBe('not-batch');
    }, 5_000);

    it('returns not-found for a nonexistent id', async () => {
      const result = await client.escalations.resolveBatchItem({
        id: '00000000-0000-0000-0000-000000000000',
        itemKey: 'cut',
        payload: { ok: true },
      });
      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.outcome).toBe('not-found');
    }, 5_000);

    it('requires exactly one selector', async () => {
      await expect(
        client.escalations.resolveBatchItem({
          itemKey: 'cut',
          payload: { ok: true },
        }),
      ).rejects.toThrow(/exactly one/);
    }, 5_000);
  });

  describe('assertClaim guard', () => {
    it('blocks a fill with claimed-by-other while another assignee holds a live claim', async () => {
      const orderId = guid();
      await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);
      await client.escalations.claim({ id: row!.id, assignee: 'bob', durationMinutes: 30 });

      const blocked = await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'cut',
        payload: { ok: true },
        assertClaim: 'alice',
      });
      expect(blocked.ok).toBe(false);
      if (blocked.ok) return;
      expect(blocked.outcome).toBe('claimed-by-other');

      // claim-agnostic default: the same fill without assertClaim lands
      const open = await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'cut',
        payload: { ok: true },
      });
      expect(open.ok).toBe(true);
      await client.escalations.cancel(row!.id);
    }, 20_000);
  });

  describe('signalKey selector', () => {
    it('fills an item selecting the row by signal_key', async () => {
      const orderId = guid();
      await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);

      const result = await client.escalations.resolveBatchItem({
        signalKey: row!.signal_key!,
        itemKey: 'cut',
        payload: { station: 'cut-1', ok: true },
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.outcome).toBe('accepted');
      expect(result.remaining).toBe(2);
      await client.escalations.cancel(row!.id);
    }, 20_000);
  });

  describe('admin override: plain resolve() on a batch row', () => {
    it('resolves the whole row with the given payload and wakes the waiter with it', async () => {
      const orderId = guid();
      const handle = await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);

      await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'cut',
        payload: { station: 'cut-1', ok: true },
      });

      const resultPromise = handle.result();
      const override = await client.escalations.resolve({
        id: row!.id,
        resolverPayload: { overridden: true },
      });
      expect(override.ok).toBe(true);
      const output = await resultPromise;
      expect((output as any).overridden).toBe(true);
    }, 30_000);
  });

  describe('concurrency (TOCTOU safety)', () => {
    it('exactly one of two same-key racers wins; the loser sees duplicate-item', async () => {
      const orderId = guid();
      await startBatch('batchWorkflow', orderId);
      const row = await findByOrder('assembly', orderId);

      const [r1, r2] = await Promise.all([
        client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'cut', payload: { racer: 1 } }),
        client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'cut', payload: { racer: 2 } }),
      ]);
      const winners = [r1, r2].filter((r) => r.ok);
      const duplicates = [r1, r2].filter((r) => !r.ok && r.outcome === 'duplicate-item');
      expect(winners.length).toBe(1);
      expect(duplicates.length).toBe(1);

      const final = await client.escalations.get(row!.id);
      const stored = (final!.envelope as any).batch_items.cut;
      expect([1, 2]).toContain(stored.racer);
      expect((final!.metadata as any).batch_count).toBe(2);
      await client.escalations.cancel(row!.id);
    }, 20_000);

    it('two distinct final keys racing: exactly one completed, one accepted, one delivery', async () => {
      const orderId = guid();
      const handle = await startBatch('batchRaceWorkflow', orderId);
      const row = await findByOrder('assembly-race', orderId);

      const seed = await client.escalations.resolveBatchItem({
        id: row!.id,
        itemKey: 'cut',
        payload: { station: 'cut-1', ok: true },
      });
      expect(seed.ok).toBe(true);

      const resultPromise = handle.result();
      const [rA, rB] = await Promise.all([
        client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'weld', payload: { station: 'weld-1', ok: true } }),
        client.escalations.resolveBatchItem({ id: row!.id, itemKey: 'paint', payload: { station: 'paint-1', ok: true } }),
      ]);
      const outcomes = [rA, rB].filter((r) => r.ok).map((r) => (r as any).outcome).sort();
      expect(outcomes).toEqual(['accepted', 'completed']);

      // the single delivery carries all three items
      const output = (await resultPromise) as workflows.BatchCollection;
      expect(output.cut).toEqual({ station: 'cut-1', ok: true });
      expect(output.weld).toEqual({ station: 'weld-1', ok: true });
      expect(output.paint).toEqual({ station: 'paint-1', ok: true });

      const final = await client.escalations.get(row!.id);
      expect(final!.status).toBe('resolved');
    }, 30_000);
  });

  describe('invalid batch declarations fail loudly', () => {
    it('rejects an empty batch on create()', async () => {
      await expect(
        client.escalations.create({ role: 'assembly', batch: [] }),
      ).rejects.toThrow(/non-empty/);
    }, 5_000);

    it('rejects duplicate item keys on create()', async () => {
      await expect(
        client.escalations.create({ role: 'assembly', batch: ['cut', 'cut'] }),
      ).rejects.toThrow(/unique/);
    }, 5_000);

    it('rejects reserved metadata collisions on create()', async () => {
      await expect(
        client.escalations.create({
          role: 'assembly',
          batch: ['cut'],
          metadata: { batch_count: 99 },
        }),
      ).rejects.toThrow(/reserved/);
    }, 5_000);
  });
});
