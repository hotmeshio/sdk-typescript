/**
 * Batch accumulator vs the SLA timer: the timer firing first resumes the
 * wait with false and expires the row; partially filled items persist on
 * the terminal row for audit, and late fills fail as already-expired.
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

describe('DURABLE | escalations-batch | timeout | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-timeout-test';

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    client = new Client({ connection });
    const worker = await Worker.create({
      connection,
      taskQueue,
      workflow: workflows.batchSlaWorkflow,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('expires a partially filled batch, preserving the filled items', async () => {
    const orderId = guid();
    const handle = await client.workflow.start({
      args: [orderId, '8s'],
      taskQueue,
      workflowName: 'batchSlaWorkflow',
      workflowId: guid(),
      expire: 180,
    });

    let row: Awaited<ReturnType<typeof client.escalations.list>>[number] | undefined;
    const deadline = Date.now() + 10_000;
    while (!row && Date.now() < deadline) {
      const rows = await client.escalations.list({ role: 'assembly-sla', status: 'pending' });
      row = rows.find((e) => (e.metadata as any)?.orderId === orderId);
      if (!row) await sleepFor(25);
    }
    expect(row).toBeDefined();

    const fill = await client.escalations.resolveBatchItem({
      id: row!.id,
      itemKey: 'cut',
      payload: { station: 'cut-1', ok: true },
    });
    expect(fill.ok).toBe(true);

    // the SLA timer wins — the wait resumes with false
    const output = (await handle.result()) as { outcome: string };
    expect(output.outcome).toBe('timed-out');

    // the row is expired with the partial fill preserved for audit
    const expired = await client.escalations.get(row!.id);
    expect(expired!.status).toBe('expired');
    expect((expired!.envelope as any).batch_items.cut).toEqual({ station: 'cut-1', ok: true });
    // the partial timeline survives expiry alongside the partial items
    expect((expired!.envelope as any).batch_filled_at.cut).toBeTruthy();
    expect((expired!.envelope as any).batch_filled_at.weld).toBeUndefined();
    expect((expired!.metadata as any).batch_pending).toEqual(['weld', 'paint']);
    expect((expired!.metadata as any).batch_count).toBe(2);

    // a late fill names the deadline instead of pretending to land
    const late = await client.escalations.resolveBatchItem({
      id: row!.id,
      itemKey: 'weld',
      payload: { station: 'weld-1', ok: true },
    });
    expect(late.ok).toBe(false);
    if (late.ok) return;
    expect(late.outcome).toBe('already-expired');
  }, 60_000);
});
