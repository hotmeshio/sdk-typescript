import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

const CONCURRENCY = 100;

describe('DURABLE | signal-race | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
    await Connection.connect({ class: Postgres, options: postgres_options });

    // Create workers for both parent and child workflows
    for (const wf of [workflows.parent, workflows.child]) {
      const worker = await Worker.create({
        connection,
        taskQueue: 'signal-race',
        workflow: wf,
      });
      await worker.run();
    }
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  it(`should deliver all signals under concurrent load (${CONCURRENCY} workflows)`, async () => {
    const client = new Client({ connection });

    // Generate IDs upfront so we can launch parents and children together
    const ids = Array.from({ length: CONCURRENCY }, () => guid());

    // Fire ALL workflows at once — parents and children simultaneously.
    // This floods the engine stream so WEBHOOK messages interleave with
    // hook registration messages, reproducing the race condition.
    const allPromises = ids.flatMap((id) => [
      // Parent: registers condition(signalId) → engine processes interruption → setHookSignal
      client.workflow.start({
        args: [id],
        taskQueue: 'signal-race',
        workflowName: 'parent',
        workflowId: `parent-${id}`,
        expire: 60,
      }),
      // Child: calls signal(signalId) → engine publishes WEBHOOK → processWebHookSignal
      client.workflow.start({
        args: [`race-signal-${id}`, `payload-${id}`],
        taskQueue: 'signal-race',
        workflowName: 'child',
        workflowId: `child-${id}`,
        expire: 60,
      }),
    ]);

    const t0 = Date.now();
    console.log(`[signal-race] All ${CONCURRENCY * 2} workflows launched. Waiting for results...`);

    const handles = await Promise.all(allPromises);
    const parentHandles = handles.filter(
      (_, i) => i % 2 === 0,
    ) as WorkflowHandleService[];

    let doneCount = 0;
    const results = await Promise.all(
      parentHandles.map((h) => h.result().then((r) => {
        doneCount++;
        if (doneCount % 20 === 0 || doneCount === CONCURRENCY) {
          console.log(`[signal-race] [${((Date.now() - t0) / 1000).toFixed(1)}s] Completed: ${doneCount}/${CONCURRENCY}`);
        }
        return r;
      })),
    );

    const completed = results.filter((r) => r !== 'timed_out');
    const timedOut = results.filter((r) => r === 'timed_out');

    if (timedOut.length > 0) {
      console.log(
        `SIGNAL RACE: ${timedOut.length}/${CONCURRENCY} signals lost (timed out)`,
      );
    }

    expect(timedOut.length).toBe(0);
    expect(completed.length).toBe(CONCURRENCY);
  }, 120_000);
});
