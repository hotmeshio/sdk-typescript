/**
 * Proves the open accumulator: one hmsh_escalations row holds items added
 * over time via accumulateItem(); the waiter resumes with the ordered
 * collection and the trigger that ended the wait. Every add is one guarded
 * statement (append + facet recompute + resolve-at-max + wake).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import { Durable } from '../../../services/durable';
import { bootHarness, findPending, guid, sleepFor } from './src/harness';
import type { BinResult } from './src/workflows';

describe('DURABLE | escalations-accumulate | Postgres', () => {
  let client: Awaited<ReturnType<typeof bootHarness>>['client'];
  const taskQueue = 'escalation-accumulate-test';

  const startBin = async (binKey: string, options: Record<string, unknown> = {}, timeout?: string) =>
    client.workflow.start({
      args: [binKey, options, ...(timeout ? [timeout] : [])],
      taskQueue,
      workflowName: 'binWorkflow',
      workflowId: guid(),
      expire: 180,
    });

  beforeAll(async () => {
    ({ client } = await bootHarness(taskQueue, ['binWorkflow']));
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('fold — Leg1 atomicity', () => {
    it('writes the accumulator shape in the same commit as the wait', async () => {
      const binKey = guid();
      await startBin(binKey, { max: 3 });
      const row = await findPending(client, 'bin', { binKey });
      expect((row.metadata as any).accumulate_count).toBe(0);
      expect((row.metadata as any).accumulate_max).toBe(3);
      expect((row.metadata as any).accumulate_keys).toEqual([]);
      expect((row.envelope as any).accumulate_items).toEqual({});
      expect((row.envelope as any).accumulate_config).toEqual({ unique: true, resolveAtMax: true });
      expect((row.envelope as any).instructions).toBe('Scan each bag into the bin');
      expect(row.signal_key).not.toBeNull();
      await client.escalations.cancel(row.id);
    }, 20_000);
  });

  describe('count trigger: accepted → accepted → completed', () => {
    it('resolves at max and resumes the workflow with the ordered collection', async () => {
      const binKey = guid();
      const handle = await startBin(binKey, { max: 3 });
      const row = await findPending(client, 'bin', { binKey });

      const first = await client.escalations.accumulateItem({
        id: row.id, itemKey: 'bag-1', payload: { weight: 1 }, actor: 'scanner-7',
      });
      expect(first.ok).toBe(true);
      if (!first.ok) return;
      expect(first.outcome).toBe('accepted');
      expect(first.count).toBe(1);
      expect(first.remaining).toBe(2);
      expect(first.entry.status).toBe('pending');
      expect((first.entry.metadata as any).accumulate_keys).toEqual(['bag-1']);
      const entry = (first.entry.envelope as any).accumulate_items['bag-1'];
      expect(entry.payload).toEqual({ weight: 1 });
      expect(entry.actor).toBe('scanner-7');
      expect(new Date(entry.at).getTime()).toBeGreaterThan(0);
      expect(entry.reciprocalId).toBeUndefined();

      // the held key is a GIN facet
      const holders = await client.escalations.list({ role: 'bin', metadata: { accumulate_keys: ['bag-1'] } });
      expect(holders.some((e) => e.id === row.id)).toBe(true);

      const second = await client.escalations.accumulateItem({
        id: row.id, itemKey: 'bag-2', payload: { weight: 2 }, metadata: { lastScanner: 'alice' },
      });
      expect(second.ok && second.outcome === 'accepted' && second.count === 2).toBe(true);
      if (!second.ok) return;
      expect((second.entry.metadata as any).lastScanner).toBe('alice');
      expect((second.entry.metadata as any).accumulate_count).toBe(2);

      const resultPromise = handle.result<{ outcome: string; payload: BinResult }>();
      const last = await client.escalations.accumulateItem({
        id: row.id, itemKey: 'bag-3', payload: { weight: 3 },
        resolvedBy: { id: 'scanner-7', email: 's7@example.com' },
      });
      expect(last.ok).toBe(true);
      if (!last.ok) return;
      expect(last.outcome).toBe('completed');
      expect(last.count).toBe(3);
      expect(last.remaining).toBe(0);
      expect(last.entry.status).toBe('resolved');
      expect((last.entry.resolver_payload as any).$trigger).toBe('count');

      const output = await resultPromise;
      expect(output.outcome).toBe('delivered');
      expect(output.payload.$trigger).toBe('count');
      expect(output.payload.$accumulated.map((i) => i.itemKey)).toEqual(['bag-1', 'bag-2', 'bag-3']);
      expect(output.payload.$accumulated[2].payload).toEqual({ weight: 3 });
      expect(output.payload.$resolution).toEqual({
        escalationId: row.id, resolvedBy: 'scanner-7', resolvedByEmail: 's7@example.com',
      });

      // the stored payload is the bare collection: no $resolution
      const stored = await client.escalations.get(row.id);
      expect((stored!.resolver_payload as any).$resolution).toBeUndefined();

      const late = await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-4' });
      expect(late.ok).toBe(false);
      if (!late.ok) expect(late.outcome).toBe('already-resolved');
    }, 30_000);
  });

  describe('guards', () => {
    it('answers duplicate-item without touching the row, and replaces with unique: false', async () => {
      const binKey = guid();
      await startBin(binKey, { max: 5 });
      const row = await findPending(client, 'bin', { binKey });
      await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-1', payload: { weight: 1 } });
      const dup = await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-1', payload: { weight: 9 } });
      expect(dup.ok).toBe(false);
      if (!dup.ok) expect(dup.outcome).toBe('duplicate-item');
      const after = await client.escalations.get(row.id);
      expect((after!.envelope as any).accumulate_items['bag-1'].payload).toEqual({ weight: 1 });
      expect((after!.metadata as any).accumulate_count).toBe(1);

      const replaceKey = guid();
      await startBin(replaceKey, { max: 5, unique: false });
      const replaceable = await findPending(client, 'bin', { binKey: replaceKey });
      await client.escalations.accumulateItem({ id: replaceable.id, itemKey: 'bag-1', payload: { weight: 1 } });
      const replaced = await client.escalations.accumulateItem({ id: replaceable.id, itemKey: 'bag-1', payload: { weight: 9 } });
      expect(replaced.ok && replaced.outcome === 'accepted' && replaced.count === 1).toBe(true);
      if (replaced.ok) {
        expect((replaced.entry.envelope as any).accumulate_items['bag-1'].payload).toEqual({ weight: 9 });
      }
      await client.escalations.cancel(row.id);
      await client.escalations.cancel(replaceable.id);
    }, 30_000);

    it('resolveAtMax: false makes max a cap: the last slot answers accepted, the next answers full', async () => {
      const binKey = guid();
      await startBin(binKey, { max: 2, resolveAtMax: false });
      const row = await findPending(client, 'bin', { binKey });
      await client.escalations.accumulateItem({ id: row.id, itemKey: 'a' });
      const capped = await client.escalations.accumulateItem({ id: row.id, itemKey: 'b' });
      expect(capped.ok && capped.outcome === 'accepted' && capped.remaining === 0).toBe(true);
      const full = await client.escalations.accumulateItem({ id: row.id, itemKey: 'c' });
      expect(full.ok).toBe(false);
      if (!full.ok) expect(full.outcome).toBe('full');
      const still = await client.escalations.get(row.id);
      expect(still!.status).toBe('pending');
      expect((still!.metadata as any).accumulate_keys).toEqual(['a', 'b']);
      await client.escalations.cancel(row.id);
    }, 30_000);

    it('rejects reserved metadata keys and bad item keys before any statement', async () => {
      await expect(
        client.escalations.accumulateItem({ id: guid(), itemKey: 'x', metadata: { accumulate_max: 1 } }),
      ).rejects.toThrow(/reserved/);
      await expect(client.escalations.accumulateItem({ id: guid(), itemKey: '' })).rejects.toThrow(/non-empty/);
      await expect(client.escalations.accumulateItem({ itemKey: 'x' })).rejects.toThrow(/exactly one/);
    });

    it('answers not-found for an unknown row and not-batch for resolveBatchItem on an accumulator', async () => {
      const missing = await client.escalations.accumulateItem({ id: '00000000-0000-4000-8000-000000000000', itemKey: 'x' });
      expect(missing.ok).toBe(false);
      if (!missing.ok) expect(missing.outcome).toBe('not-found');

      const binKey = guid();
      await startBin(binKey, { max: 2 });
      const row = await findPending(client, 'bin', { binKey });
      const wrong = await client.escalations.resolveBatchItem({ id: row.id, itemKey: 'x', payload: {} });
      expect(wrong.ok).toBe(false);
      if (!wrong.ok) expect(wrong.outcome).toBe('not-batch');
      await client.escalations.cancel(row.id);
    }, 20_000);
  });

  describe('remove', () => {
    it('removes a held item without waking, then the key can be re-added', async () => {
      const binKey = guid();
      const handle = await startBin(binKey, { max: 2 });
      const row = await findPending(client, 'bin', { binKey });
      await client.escalations.accumulateItem({ id: row.id, itemKey: 'a', payload: { weight: 1 } });
      const removed = await client.escalations.removeAccumulatedItem({ id: row.id, itemKey: 'a' });
      expect(removed.ok && removed.outcome === 'removed' && removed.count === 0).toBe(true);
      if (removed.ok) {
        expect((removed.entry.metadata as any).accumulate_keys).toEqual([]);
        expect((removed.entry.envelope as any).accumulate_items).toEqual({});
        expect(removed.entry.status).toBe('pending');
      }
      const absent = await client.escalations.removeAccumulatedItem({ id: row.id, itemKey: 'a' });
      expect(absent.ok).toBe(false);
      if (!absent.ok) expect(absent.outcome).toBe('item-absent');

      await client.escalations.accumulateItem({ id: row.id, itemKey: 'a', payload: { weight: 5 } });
      const done = await client.escalations.accumulateItem({ id: row.id, itemKey: 'b' });
      expect(done.ok && done.outcome === 'completed').toBe(true);
      const output = await handle.result<{ payload: BinResult }>();
      expect(output.payload.$accumulated.map((i) => i.itemKey)).toEqual(['a', 'b']);
      expect(output.payload.$accumulated[0].payload).toEqual({ weight: 5 });
    }, 30_000);
  });

  describe('race — two adds compete for the final slot', () => {
    it('exactly one caller observes completed and the waiter wakes once', async () => {
      const binKey = guid();
      const handle = await startBin(binKey, { max: 2 });
      const row = await findPending(client, 'bin', { binKey });
      await client.escalations.accumulateItem({ id: row.id, itemKey: 'a' });
      const [x, y] = await Promise.all([
        client.escalations.accumulateItem({ id: row.id, itemKey: 'b' }),
        client.escalations.accumulateItem({ id: row.id, itemKey: 'c' }),
      ]);
      // the loser locks after the winner's resolve committed and sees a
      // terminal row, never a second completion
      const outcomes = [x, y].map((r) => r.outcome).sort();
      expect(outcomes).toEqual(['already-resolved', 'completed']);
      const output = await handle.result<{ payload: BinResult }>();
      expect(output.payload.$accumulated).toHaveLength(2);
      expect(output.payload.$trigger).toBe('count');
    }, 30_000);
  });
});
