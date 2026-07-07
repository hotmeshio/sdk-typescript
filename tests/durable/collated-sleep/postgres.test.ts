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
 * Field topology regression: `Promise.all([executeChild x4, sleep branch])`.
 * The sleep's timehook must be MINTED (visible as an engine_streams row,
 * live or consumed) and the converge must settle. The field report's wedge
 * showed the inverse: a director whose collated sleep never produced a
 * timehook row at all — absence, not consumption.
 */
describe('DURABLE | collated-sleep | Postgres', () => {
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
    const parentWorker = await Worker.create({
      connection,
      taskQueue: 'converge-queue',
      workflow: workflows.convergeWithSleep,
    });
    await parentWorker.run();
    const childWorker = await Worker.create({
      connection,
      taskQueue: 'converge-queue',
      workflow: workflows.castMember,
    });
    await childWorker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  it('should mint the collated sleep timehook and settle the converge', async () => {
    const handle = await client.workflow.start({
      args: [runId],
      taskQueue: 'converge-queue',
      workflowName: 'convergeWithSleep',
      workflowId: guid(),
      expire: 600,
    });

    //the timehook row must exist (live or consumed — no liveness filter,
    //matching the field forensics query) within the cast window
    let minted = false;
    const start = Date.now();
    while (!minted) {
      const res = await postgresClient.query(
        `SELECT COUNT(*)::int AS count FROM durable.engine_streams
         WHERE message LIKE '%timehook%'`,
      );
      minted = res.rows[0].count > 0;
      if (!minted) {
        if (Date.now() - start > 20_000) {
          throw new Error(
            'collated sleep timehook was never minted (field wedge shape)',
          );
        }
        await sleepFor(250);
      }
    }
    expect(minted).toBe(true);

    //the converge settles: 4 children + the retired sleeper
    const result = (await Promise.race([
      handle.result(),
      sleepFor(60_000).then(() => {
        throw new Error('converge never settled');
      }),
    ])) as string;
    expect(result).toContain('retired');
    expect(result.split('|')).toHaveLength(5);
  }, 90_000);
});
