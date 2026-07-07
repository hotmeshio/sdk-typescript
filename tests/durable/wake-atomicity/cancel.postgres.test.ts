import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { EscalationClientService } from '../../../services/escalations/client';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

/**
 * cancel() must wake the awaiting workflow (condition() returns null)
 * with the same durability as resolve(): the cancellation wake commits
 * WITH the status='cancelled' transaction. Watcher patterns cancel in
 * bursts (N ponds self-cancelling within one wave), so the burst path
 * is exercised natively as well.
 */
describe('DURABLE | wake-atomicity | cancel', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const POND_COUNT = 6;

  const parkOne = async (
    runId: string,
  ): Promise<WorkflowHandleService> =>
    client.workflow.start({
      args: [runId],
      taskQueue: 'wake-cancel',
      workflowName: 'crashWindowWorkflow',
      workflowId: guid(),
      expire: 600,
    });

  const findRow = async (runId: string, timeoutMs = 20_000) => {
    const start = Date.now();
    for (;;) {
      const list = await client.escalations.list({
        role: 'crash-approver',
        status: 'pending',
      });
      const row = list.find((e) => (e.metadata as any)?.runId === runId);
      if (row) return row;
      if (Date.now() - start > timeoutMs) {
        throw new Error(`timed out waiting for escalation row ${runId}`);
      }
      await sleepFor(100);
    }
  };

  const awaitWake = async (
    handle: WorkflowHandleService,
    label: string,
  ): Promise<unknown> =>
    Promise.race([
      handle.result(),
      sleepFor(25_000).then(() => {
        throw new Error(`workflow never woke after cancel: ${label}`);
      }),
    ]);

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    client = new Client({ connection });
    const worker = await Worker.create({
      connection,
      taskQueue: 'wake-cancel',
      workflow: workflows.crashWindowWorkflow,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    vi.restoreAllMocks();
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  it('should wake the workflow with null when the post-cancel delivery step dies', async () => {
    const runId = guid();
    const handle = await parkOne(runId);
    const row = await findRow(runId);

    //simulate death between the cancel commit and the post-commit wake
    const spy = vi
      .spyOn(
        EscalationClientService.prototype as any,
        '_deliverEscalationSignal',
      )
      .mockResolvedValue(false);

    try {
      const result = await client.escalations.cancel(row.id);
      expect(result.ok).toBe(true);

      const dbRow = await postgresClient.query(
        `SELECT status FROM public.hmsh_escalations WHERE id = $1`,
        [row.id],
      );
      expect(dbRow.rows[0].status).toBe('cancelled');

      //condition() resumes with null on cancellation
      const output = await awaitWake(handle, runId);
      expect(output).toBeNull();
    } finally {
      spy.mockRestore();
    }
  }, 60_000);

  it('should wake every workflow in a burst of concurrent cancels', async () => {
    //the field wave: N ponds self-cancel within ~400ms of each other
    const runs = Array.from({ length: POND_COUNT }, () => guid());
    const handles: WorkflowHandleService[] = [];
    for (const runId of runs) {
      handles.push(await parkOne(runId));
    }
    const rows = [];
    for (const runId of runs) {
      rows.push(await findRow(runId));
    }

    const results = await Promise.all(
      rows.map((row) => client.escalations.cancel(row.id)),
    );
    expect(results.every((r) => r.ok)).toBe(true);

    const outputs = await Promise.all(
      handles.map((handle, i) => awaitWake(handle, runs[i])),
    );
    expect(outputs.every((o) => o === null)).toBe(true);
  }, 90_000);
});
