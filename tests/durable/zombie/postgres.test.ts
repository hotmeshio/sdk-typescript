import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';
import * as activities from './src/activities';

const { Connection, Client, Worker } = Durable;

/**
 * Terminating a workflow must purge its queued/reserved stream messages.
 * A terminated workflow's activity message left live in worker_streams is
 * redelivered when its reservation lapses (or a worker restarts) and the
 * activity's side effects re-execute against live state ("zombie batches").
 */
describe('DURABLE | zombie | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'zombie-world';
  const workflowId = guid();

  const liveRowsForJid = async (jid: string): Promise<number> => {
    const res = await postgresClient.query(
      `SELECT COUNT(*)::int AS count FROM durable.worker_streams
       WHERE jid = $1 AND expired_at IS NULL AND dead_lettered_at IS NULL`,
      [jid],
    );
    return res.rows[0].count;
  };

  const until = async (
    predicate: () => boolean,
    timeoutMs: number,
    label: string,
  ): Promise<void> => {
    const start = Date.now();
    while (!predicate()) {
      if (Date.now() - start > timeoutMs) {
        throw new Error(`timed out waiting for: ${label}`);
      }
      await sleepFor(100);
    }
  };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  describe('Worker and Client', () => {
    it('should run the worker and start the workflow', async () => {
      const workerConnection = (await Connection.connect(connection)) as any;
      expect(workerConnection).toBeDefined();

      const worker = await Worker.create({
        connection,
        taskQueue,
        workflow: workflows.zombieExample,
        options: {
          logLevel: HMSH_LOGLEVEL,
        },
      });
      await worker.run();

      activities.resetSideEffectCount();
      const client = new Client({ connection });
      handle = await client.workflow.start({
        args: ['zombie-run-1'],
        taskQueue,
        workflowName: 'zombieExample',
        workflowId,
        expire: 600,
      });
      expect(handle.workflowId).toBe(workflowId);

      //wait until the slow activity is executing (its message is reserved)
      await until(
        () => activities.getSideEffectCount() === 1,
        10_000,
        'activity to start executing',
      );
    }, 20_000);
  });

  describe('Terminate', () => {
    it('should purge the terminated workflow`s stream messages', async () => {
      //the activity message is reserved and mid-execution; terminate now
      await handle.terminate();
      const status = await handle.status();
      expect(status).toBeLessThan(0);

      //no message belonging to the terminated jid may remain deliverable
      expect(await liveRowsForJid(workflowId)).toBe(0);
    }, 10_000);

    it('should never re-execute the terminated workflow`s activity', async () => {
      //simulate a lapsed reservation (the mechanism by which zombie
      //messages resurface after slow activities or worker restarts):
      //backdate any live reservation for the jid past the claim window
      await postgresClient.query(
        `UPDATE durable.worker_streams
         SET reserved_at = NOW() - INTERVAL '600 seconds', visible_at = NOW()
         WHERE jid = $1 AND expired_at IS NULL AND dead_lettered_at IS NULL`,
        [workflowId],
      );

      //wake every worker-stream consumer that has rows for this jid
      //(expired rows included: soft-deleted rows still record the topic)
      const topics = await postgresClient.query(
        `SELECT DISTINCT stream_name FROM durable.worker_streams WHERE jid = $1`,
        [workflowId],
      );
      for (const row of topics.rows) {
        const channel = `wrk_${row.stream_name}`.substring(0, 63);
        await postgresClient.query(`SELECT pg_notify($1, $2)`, [
          channel,
          JSON.stringify({
            stream_name: row.stream_name,
            table_type: 'worker',
          }),
        ]);
      }

      //allow time for a zombie redelivery to claim and execute; the
      //original execution (5s sleep) also settles within this window
      await sleepFor(7_500);

      //the side effect fired exactly once — on the original execution
      expect(activities.getSideEffectCount()).toBe(1);
      expect(await liveRowsForJid(workflowId)).toBe(0);
    }, 20_000);
  });
});
