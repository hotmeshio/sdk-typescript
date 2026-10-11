/**
 * Batch provenance: `filled` entries start a batch row satisfied (folded
 * into the creation INSERT, counted by progress and completion, delivered
 * with the collection), and a fill that names an `actor` records it under
 * `envelope.batch_filled_by` in the same statement as the fill.
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

describe('DURABLE | escalations-batch | filled | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-filled-test';
  const cutAt = '2026-01-02T03:04:05.000Z';

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
    const worker = await Worker.create({
      connection,
      taskQueue,
      workflow: workflows.batchFilledWorkflow,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('parks a condition() batch with its filled entries and completes on the rest', async () => {
    const orderId = guid();
    const handle = await client.workflow.start({
      args: [orderId, cutAt],
      taskQueue,
      workflowName: 'batchFilledWorkflow',
      workflowId: guid(),
      expire: 180,
    });
    const row = await findByOrder('assembly-filled', orderId);
    expect(row).toBeDefined();
    expect((row!.metadata as any).batch_pending).toEqual(['weld', 'paint']);
    expect((row!.metadata as any).batch_count).toBe(2);
    expect((row!.metadata as any).batch_keys).toEqual(['cut', 'weld', 'paint']);
    expect((row!.envelope as any).batch_items).toEqual({ cut: { station: 'cut-0', ok: true } });
    expect((row!.envelope as any).batch_filled_at).toEqual({ cut: cutAt });
    expect((row!.envelope as any).batch_filled_by).toEqual({ cut: 'user-cut' });

    // a seeded key is already filled
    const again = await client.escalations.resolveBatchItem({
      id: row!.id,
      itemKey: 'cut',
      payload: { station: 'cut-9', ok: true },
    });
    expect(again).toEqual({ ok: false, outcome: 'duplicate-item' });

    const weld = await client.escalations.resolveBatchItem({
      id: row!.id,
      itemKey: 'weld',
      payload: { station: 'weld-1', ok: true },
      actor: 'user-weld',
    });
    expect(weld.ok && weld.outcome).toBe('accepted');
    if (!weld.ok) return;
    expect(weld.remaining).toBe(1);
    expect((weld.entry.envelope as any).batch_filled_by).toEqual({
      cut: 'user-cut',
      weld: 'user-weld',
    });

    const resultPromise = handle.result();
    const paint = await client.escalations.resolveBatchItem({
      id: row!.id,
      itemKey: 'paint',
      payload: { station: 'paint-1', ok: true },
    });
    expect(paint.ok && paint.outcome).toBe('completed');
    if (!paint.ok) return;
    // a fill without an actor leaves batch_filled_by untouched
    expect((paint.entry.envelope as any).batch_filled_by).toEqual({
      cut: 'user-cut',
      weld: 'user-weld',
    });
    expect(Object.keys((paint.entry.envelope as any).batch_filled_at).sort())
      .toEqual(['cut', 'paint', 'weld']);

    const output = (await resultPromise) as workflows.BatchCollection;
    expect(output.cut).toEqual({ station: 'cut-0', ok: true });
    expect(output.weld).toEqual({ station: 'weld-1', ok: true });
    expect(output.paint).toEqual({ station: 'paint-1', ok: true });
  }, 30_000);

  it('records the actor of a fill selected by metadata', async () => {
    const orderId = guid();
    await client.escalations.create({
      role: 'assembly-filled-meta',
      metadata: { orderId },
      batch: ['cut', 'weld'],
    });
    const result = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId',
      value: orderId,
      itemKey: 'weld',
      payload: { station: 'weld-2', ok: true },
      actor: 'user-meta',
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect((result.entry.envelope as any).batch_filled_by).toEqual({ weld: 'user-meta' });
  }, 10_000);

  it('leaves the envelope shape unchanged without filled or actor', async () => {
    const row = await client.escalations.create({
      role: 'assembly-filled-plain',
      metadata: { orderId: guid() },
      batch: ['cut', 'weld'],
    });
    expect(row.envelope).toEqual({ batch_items: {}, batch_filled_at: {} });
    const fill = await client.escalations.resolveBatchItem({
      id: row.id,
      itemKey: 'cut',
      payload: { ok: true },
    });
    expect(fill.ok).toBe(true);
    if (!fill.ok) return;
    expect((fill.entry.envelope as any).batch_filled_by).toBeUndefined();
  }, 10_000);

  it('completes a standalone filled row on its last open key', async () => {
    const row = await client.escalations.create({
      role: 'assembly-filled-standalone',
      batch: ['cut', 'weld'],
      filled: { cut: { payload: { ok: true }, at: cutAt } },
    });
    expect((row.metadata as any).batch_count).toBe(1);
    expect((row.envelope as any).batch_filled_by).toBeUndefined();
    const last = await client.escalations.resolveBatchItem({
      id: row.id,
      itemKey: 'weld',
      payload: { ok: true },
      actor: 'user-weld',
    });
    expect(last.ok && last.outcome).toBe('completed');
    if (!last.ok) return;
    expect(last.entry.resolver_payload).toEqual({ cut: { ok: true }, weld: { ok: true } });
  }, 10_000);

  it.each([
    [{ batch: ['cut'], filled: { cut: { payload: {}, at: cutAt } } }, /nothing to wait for/],
    [{ batch: ['cut', 'weld'], filled: { paint: { payload: {}, at: cutAt } } }, /not declared/],
    [{ batch: ['cut', 'weld'], filled: { cut: { payload: {}, at: 'yesterday' } } }, /ISO-8601/],
    [{ filled: { cut: { payload: {}, at: cutAt } } }, /requires a batch/],
  ])('rejects an invalid filled declaration on create() %#', async (config, message) => {
    await expect(
      client.escalations.create({ role: 'assembly-filled-bad', ...(config as any) }),
    ).rejects.toThrow(message);
  }, 5_000);
});
