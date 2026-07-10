/**
 * Proves the concurrent-condition contract for harvest fan-out: one
 * workflow opens N condition() waits via Promise.all while signalers
 * fire at ALL of them immediately — before any wait has registered.
 * Every signal that races ahead of registration is buffered as a
 * pending signal (default TTL 10 minutes, extensible per signal) and
 * delivered when its condition registers. No signal is lost and every
 * wait resumes with its own payload, so fan-out is sized by the
 * pending-signal TTL, not by a registration-count bound.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

const FANOUT = 25;

describe('DURABLE | fanout | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
    await Connection.connect(connection);

    const worker = await Worker.create({
      connection,
      taskQueue: 'fanout',
      workflow: workflows.fanout,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  it(`should harvest ${FANOUT} concurrent conditions with signalers racing ahead of registration`, async () => {
    const client = new Client({ connection });
    const caseId = guid();

    const handle = await client.workflow.start({
      args: [caseId, FANOUT],
      taskQueue: 'fanout',
      workflowName: 'fanout',
      workflowId: `fanout-${caseId}`,
      expire: 120,
    });

    // Fire every signal NOW — the workflow's first tick may not have run,
    // so most (often all) land before their condition registers and take
    // the pending-signal buffering path.
    await Promise.all(
      Array.from({ length: FANOUT }, (_, i) =>
        handle.signal(`fanout-${caseId}-${i}`, { v: `payload-${i}` }),
      ),
    );

    const results = (await handle.result()) as string[];
    expect(results).toHaveLength(FANOUT);
    // Promise.all preserves order: each wait resumed with its own payload.
    expect(results).toEqual(
      Array.from({ length: FANOUT }, (_, i) => `payload-${i}`),
    );
  }, 120_000);
});
