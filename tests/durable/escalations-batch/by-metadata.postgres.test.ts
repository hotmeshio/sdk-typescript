/**
 * resolveBatchItemByMetadata: the fill selects its row by GIN facet
 * (highest-priority pending match), mirroring resolveByMetadata's selector.
 * Also proves standalone batch rows (create() with batch, no signal_key)
 * accumulate and complete identically with the wake a no-op.
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

describe('DURABLE | escalations-batch | by-metadata | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-meta-test';

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    client = new Client({ connection });
    const worker = await Worker.create({
      connection,
      taskQueue,
      workflow: workflows.batchWorkflow,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('fills and completes a waiting batch selected by metadata facet', async () => {
    const orderId = guid();
    const handle = await client.workflow.start({
      args: [orderId],
      taskQueue,
      workflowName: 'batchWorkflow',
      workflowId: guid(),
      expire: 180,
    });
    await sleepFor(2000);

    const first = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId',
      value: orderId,
      roles: ['assembly'],
      itemKey: 'cut',
      payload: { station: 'cut-1', ok: true },
    });
    expect(first.ok).toBe(true);
    if (!first.ok) return;
    expect(first.outcome).toBe('accepted');
    expect(first.remaining).toBe(2);

    const second = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId',
      value: orderId,
      itemKey: 'weld',
      payload: { station: 'weld-1', ok: true },
    });
    expect(second.ok).toBe(true);

    const resultPromise = handle.result();
    const last = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId',
      value: orderId,
      itemKey: 'paint',
      payload: { station: 'paint-1', ok: true },
      resolvedBy: { id: 'painter-meta' },
    });
    expect(last.ok).toBe(true);
    if (!last.ok) return;
    expect(last.outcome).toBe('completed');

    const output = (await resultPromise) as workflows.BatchCollection;
    expect(output.cut).toEqual({ station: 'cut-1', ok: true });
    expect(output.weld).toEqual({ station: 'weld-1', ok: true });
    expect(output.paint).toEqual({ station: 'paint-1', ok: true });
    expect(output.$resolution?.resolvedBy).toBe('painter-meta');
  }, 60_000);

  it('rejects a fill when the role filter excludes the row', async () => {
    const orderId = guid();
    await client.workflow.start({
      args: [orderId],
      taskQueue,
      workflowName: 'batchWorkflow',
      workflowId: guid(),
      expire: 180,
    });
    await sleepFor(2000);

    const result = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId',
      value: orderId,
      roles: ['some-other-role'],
      itemKey: 'cut',
      payload: { ok: true },
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.outcome).toBe('not-found');
  }, 30_000);

  it('returns not-found when no metadata matches', async () => {
    const result = await client.escalations.resolveBatchItemByMetadata({
      key: 'orderId',
      value: `no-such-order-${guid()}`,
      itemKey: 'cut',
      payload: { ok: true },
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.outcome).toBe('not-found');
  }, 5_000);

  describe('standalone batch rows (no signal_key)', () => {
    it('accumulates and completes with the wake a no-op', async () => {
      const ticketId = `standalone-${guid()}`;
      const row = await client.escalations.create({
        role: 'batch-standalone',
        type: 'standalone-batch',
        metadata: { ticketId },
        batch: ['docs', 'photos'],
      });
      expect(row.signal_key).toBeNull();
      expect((row.metadata as any).batch_pending).toEqual(['docs', 'photos']);
      expect((row.envelope as any).batch_items).toEqual({});

      const first = await client.escalations.resolveBatchItem({
        id: row.id,
        itemKey: 'docs',
        payload: { uploaded: 3 },
      });
      expect(first.ok).toBe(true);
      if (!first.ok) return;
      expect(first.outcome).toBe('accepted');
      expect(first.remaining).toBe(1);

      const last = await client.escalations.resolveBatchItem({
        id: row.id,
        itemKey: 'photos',
        payload: { uploaded: 12 },
      });
      expect(last.ok).toBe(true);
      if (!last.ok) return;
      expect(last.outcome).toBe('completed');
      expect(last.entry.status).toBe('resolved');
      expect((last.entry.resolver_payload as any).docs).toEqual({ uploaded: 3 });
      expect((last.entry.resolver_payload as any).photos).toEqual({ uploaded: 12 });
    }, 20_000);
  });
});
