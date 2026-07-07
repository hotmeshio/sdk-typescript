import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

/**
 * A resolved SLA-gated wait must disarm its timeout leg. The timeout-won
 * path already cleans the signal side (deleteWebHookSignal +
 * expireEscalationOnTimeout); the signal-won path needs the mirror —
 * otherwise every resolved wait leaves an armed timehook that fires at
 * its original due time against the settled workflow (one benign error
 * per resolved wait; a fleet's evening leaves a drizzle of them that
 * operators read as a crash loop).
 */
describe('DURABLE | wake-atomicity | timeout disarm', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const runId = guid();

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    client = new Client({ connection });
    const worker = await Worker.create({
      connection,
      taskQueue: 'disarm-world',
      workflow: workflows.timeoutGatedWorkflow,
    });
    await worker.run();
    const cycledWorker = await Worker.create({
      connection,
      taskQueue: 'disarm-world',
      workflow: workflows.cycledTimeoutGatedWorkflow,
    });
    await cycledWorker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  it('should disarm the timeout leg when the signal wins', async () => {
    const handle = await client.workflow.start({
      args: [runId],
      taskQueue: 'disarm-world',
      workflowName: 'timeoutGatedWorkflow',
      workflowId: guid(),
      expire: 600,
    });

    //wait for the park (escalation row + armed timehook)
    let escalation: { id: string } | undefined;
    const start = Date.now();
    while (!escalation) {
      const list = await client.escalations.list({
        role: 'disarm-approver',
        status: 'pending',
      });
      escalation = list.find((e) => (e.metadata as any)?.runId === runId);
      if (!escalation) {
        if (Date.now() - start > 20_000) {
          throw new Error('timed out waiting for escalation row');
        }
        await sleepFor(250);
      }
    }

    //the timeout leg is armed: a future-visible timehook exists
    const armed = await postgresClient.query(
      `SELECT COUNT(*)::int AS count FROM durable.engine_streams
       WHERE expired_at IS NULL AND visible_at > NOW()
         AND message LIKE '%"type":"timehook"%'`,
    );
    expect(armed.rows[0].count).toBe(1);

    //signal wins the race
    const result = await client.escalations.resolve({
      id: escalation.id,
      resolverPayload: { approved: true },
    });
    expect(result.ok).toBe(true);

    const output = (await Promise.race([
      handle.result(),
      sleepFor(25_000).then(() => {
        throw new Error('workflow never woke');
      }),
    ])) as { approved: boolean };
    expect(output.approved).toBe(true);

    //the armed timer must be disarmed with the settled wait — no
    //future-visible timehook corpse remains to fire and error later
    await sleepFor(2_000);
    const corpses = await postgresClient.query(
      `SELECT COUNT(*)::int AS count FROM durable.engine_streams
       WHERE expired_at IS NULL AND visible_at > NOW()
         AND message LIKE '%"type":"timehook"%'`,
    );
    expect(corpses.rows[0].count).toBe(0);
  }, 60_000);

  it('should disarm the timeout leg for a CYCLED-dimension wait', async () => {
    //pre-wait durable ops advance the cycle index, so the waiter runs
    //at a cycled dimensional address — the harder disarm case
    const cycledRunId = guid();
    const handle = await client.workflow.start({
      args: [cycledRunId],
      taskQueue: 'disarm-world',
      workflowName: 'cycledTimeoutGatedWorkflow',
      workflowId: guid(),
      expire: 600,
    });

    let escalation: { id: string } | undefined;
    const start = Date.now();
    while (!escalation) {
      const list = await client.escalations.list({
        role: 'disarm-approver',
        status: 'pending',
      });
      escalation = list.find(
        (e) => (e.metadata as any)?.runId === cycledRunId,
      );
      if (!escalation) {
        if (Date.now() - start > 25_000) {
          throw new Error('timed out waiting for cycled escalation row');
        }
        await sleepFor(250);
      }
    }

    const result = await client.escalations.resolve({
      id: escalation.id,
      resolverPayload: { approved: true },
    });
    expect(result.ok).toBe(true);

    const output = (await Promise.race([
      handle.result(),
      sleepFor(25_000).then(() => {
        throw new Error('cycled workflow never woke');
      }),
    ])) as { approved: boolean };
    expect(output.approved).toBe(true);

    await sleepFor(2_000);
    const corpses = await postgresClient.query(
      `SELECT COUNT(*)::int AS count FROM durable.engine_streams
       WHERE expired_at IS NULL AND visible_at > NOW()
         AND message LIKE '%"type":"timehook"%'`,
    );
    expect(corpses.rows[0].count).toBe(0);
  }, 90_000);
});
