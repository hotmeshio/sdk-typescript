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

  it('should exclude live waiters from bulk resolution', async () => {
    //resolveMany carries no wake: a row backing a condition() waiter
    //must stay pending (targeted resolve/cancel owns its wake) rather
    //than resolve silently and strand the workflow
    const runId = guid();
    const handle = await parkOne(runId);
    const row = await findRow(runId);

    const resolved = await client.escalations.resolveMany({
      ids: [row.id],
      resolverPayload: { bulk: true },
    });
    expect(resolved.length).toBe(0);

    const after = await client.escalations.list({
      role: 'crash-approver',
      status: 'pending',
    });
    expect(after.some((e) => e.id === row.id)).toBe(true);

    //targeted cancel still settles the waiter (condition returns null)
    const cancel = await client.escalations.cancel(row.id);
    expect(cancel.ok).toBe(true);
    const output = await awaitWake(handle, runId);
    expect(output).toBeNull();
  }, 60_000);

  it('should wake both siblings when their resolves land in the same tick', async () => {
    //field case: two sibling workers parked on signal_key waits, both
    //resolved near-simultaneously — the second wake was lost. The wake
    //INSERT commits with each resolve, so same-tick resolves must both
    //deliver.
    const runA = guid();
    const runB = guid();
    const handleA = await parkOne(runA);
    const handleB = await parkOne(runB);
    const rowA = await findRow(runA);
    const rowB = await findRow(runB);

    const results = await Promise.all([
      client.escalations.resolve({
        id: rowA.id,
        resolverPayload: { approved: true, via: 'sibling-a' },
      }),
      client.escalations.resolve({
        id: rowB.id,
        resolverPayload: { approved: true, via: 'sibling-b' },
      }),
    ]);
    expect(results.every((r) => r.ok)).toBe(true);

    const [outA, outB] = (await Promise.all([
      awaitWake(handleA, runA),
      awaitWake(handleB, runB),
    ])) as Array<{ via: string }>;
    expect(outA.via).toBe('sibling-a');
    expect(outB.via).toBe('sibling-b');
  }, 90_000);

  it('should resolve the row AND wake the workflow under a concurrent claim', async () => {
    //field case: a concurrent claim interleaved with the resolve — the
    //signal was delivered but the row stayed pending+claimed forever,
    //asserting work was owed on a completed order. Signal delivery and
    //row settle are one atomic statement; a claim cannot split them.
    const runId = guid();
    const handle = await parkOne(runId);
    const row = await findRow(runId);

    const claimA = await client.escalations.claim({
      id: row.id,
      assignee: 'associate-a',
      durationMinutes: 5,
    });
    expect(claimA.ok).toBe(true);

    //associate B re-claims while C resolves by metadata, same tick
    const [claimB, resolved] = await Promise.all([
      client.escalations.claim({
        id: row.id,
        assignee: 'associate-b',
        durationMinutes: 5,
      }),
      client.escalations.resolveByMetadata({
        key: 'runId',
        value: runId,
        resolverPayload: { approved: true, via: 'raced-resolve' },
      }),
    ]);
    expect(resolved.ok).toBe(true);
    void claimB; //may win or lose the interleave; the row must settle either way

    //the row cannot lie: it is resolved, and the workflow woke
    const dbRow = await postgresClient.query(
      `SELECT status FROM public.hmsh_escalations WHERE id = $1`,
      [row.id],
    );
    expect(dbRow.rows[0].status).toBe('resolved');

    const output = (await awaitWake(handle, runId)) as { via: string };
    expect(output.via).toBe('raced-resolve');
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
