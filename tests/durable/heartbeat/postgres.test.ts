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
 * An activity that runs longer than the base reservation window (30s)
 * must execute exactly once and deliver its result — with a SECOND
 * worker actively polling the same task queue, so a lapsed lease would
 * be reclaimed and re-dispatched for real. The consumer heartbeats the
 * reservation (reserved_at refresh) while the activity callback runs,
 * so the message never becomes claimable mid-execution; the declared
 * startToCloseTimeout (120s) bounds the run without affecting the
 * lease. A crashed consumer stops heartbeating and its message is
 * rescued within one window (proven at the provider layer in
 * tests/functional/stream/providers/postgres/extension.test.ts).
 */
describe('DURABLE | heartbeat | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'heartbeat-world';
  const workflowId = guid();

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
    it('should run two competing workers and start the workflow', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue,
        workflow: workflows.heartbeatExample,
        options: {
          logLevel: HMSH_LOGLEVEL,
        },
      });
      await worker.run();

      // A rival consumer on the same queue: if the lease lapsed mid-run,
      // this worker would reclaim the message and execute a duplicate.
      const rival = await Worker.create({
        connection,
        taskQueue,
        workflow: workflows.heartbeatExample,
        options: {
          logLevel: HMSH_LOGLEVEL,
        },
      });
      await rival.run();

      activities.resetExecutionCount();
      const client = new Client({ connection });
      handle = await client.workflow.start({
        args: ['run-1'],
        taskQueue,
        workflowName: 'heartbeatExample',
        workflowId,
        expire: 600,
      });
      expect(handle.workflowId).toBe(workflowId);
    }, 15_000);
  });

  describe('Slow activity', () => {
    it('should execute the activity exactly once and deliver its result', async () => {
      //the activity sleeps 45s — past the 30s reservation window; the
      //heartbeat holds the lease so the single execution's result lands
      const result = await handle.result();
      expect(result).toBe('leased:run-1');
      expect(activities.getExecutionCount()).toBe(1);

      //the message was acked by its one and only execution
      const res = await postgresClient.query(
        `SELECT COUNT(*)::int AS count FROM durable.worker_streams
         WHERE jid = $1 AND expired_at IS NULL AND dead_lettered_at IS NULL`,
        [workflowId],
      );
      expect(res.rows[0].count).toBe(0);
    }, 110_000);
  });
});
