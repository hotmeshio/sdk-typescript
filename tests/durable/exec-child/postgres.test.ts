import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../../types/provider';

const { Connection, Client, Worker } = Durable;

import * as workflows from './src/workflows';

describe('DURABLE | execChild | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
    await Connection.connect({ class: Postgres, options: postgres_options });

    // Create all workers
    for (const wf of [
      workflows.childVoid,
      workflows.childString,
      workflows.parentVoidChild,
      workflows.parentStringChild,
    ]) {
      const worker = await Worker.create({
        connection,
        taskQueue: 'ec-queue',
        workflow: wf,
      });
      await worker.run();
    }
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('void child: parent should complete', async () => {
    const client = new Client({ connection });
    const handle = await client.workflow.start({
      args: ['Alice'],
      taskQueue: 'ec-queue',
      workflowName: 'parentVoidChild',
      workflowId: `ec-void-${guid()}`,
      expire: 30,
    });
    const result = await handle.result();
    expect(result).toBe('void-done');
  }, 15_000);

  it('string child: parent should receive child return value', async () => {
    const client = new Client({ connection });
    const handle = await client.workflow.start({
      args: ['Bob'],
      taskQueue: 'ec-queue',
      workflowName: 'parentStringChild',
      workflowId: `ec-str-${guid()}`,
      expire: 30,
    });
    const result = await handle.result();
    expect(result).toBe('got: hello Bob');
  }, 15_000);
});
