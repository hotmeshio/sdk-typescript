/**
 * Cancelling a partially filled batch escalation resumes the wait with null
 * (standard cancel contract); the filled items persist on the cancelled row
 * and late fills fail as already-cancelled.
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

describe('DURABLE | escalations-batch | cancel | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'escalation-batch-cancel-test';

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    client = new Client({ connection });
    const worker = await Worker.create({
      connection,
      taskQueue,
      workflow: workflows.batchCancelWorkflow,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('cancels a partially filled batch and resumes the wait with null', async () => {
    const orderId = guid();
    const handle = await client.workflow.start({
      args: [orderId],
      taskQueue,
      workflowName: 'batchCancelWorkflow',
      workflowId: guid(),
      expire: 180,
    });

    let row: Awaited<ReturnType<typeof client.escalations.list>>[number] | undefined;
    const deadline = Date.now() + 10_000;
    while (!row && Date.now() < deadline) {
      const rows = await client.escalations.list({ role: 'assembly-cancel', status: 'pending' });
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

    const cancelled = await client.escalations.cancel(row!.id);
    expect(cancelled.ok).toBe(true);

    const output = await handle.result<null>();
    expect(output).toBeNull();

    const terminal = await client.escalations.get(row!.id);
    expect(terminal!.status).toBe('cancelled');
    expect((terminal!.envelope as any).batch_items.cut).toEqual({ station: 'cut-1', ok: true });

    const late = await client.escalations.resolveBatchItem({
      id: row!.id,
      itemKey: 'weld',
      payload: { station: 'weld-1', ok: true },
    });
    expect(late.ok).toBe(false);
    if (late.ok) return;
    expect(late.outcome).toBe('already-cancelled');
  }, 60_000);
});
