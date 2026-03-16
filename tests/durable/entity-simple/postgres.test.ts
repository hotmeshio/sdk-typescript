import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../../types/provider';

const { Connection, Client, Worker } = Durable;
import * as workflows from './src/workflows';

describe('DURABLE | entity-simple | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
    await Connection.connect({ class: Postgres, options: postgres_options });

    for (const wf of [
      workflows.entitySetGet,
      workflows.entityMerge,
      workflows.simpleChild,
      workflows.entityWithChild,
      workflows.entityChild,
      workflows.entityExecChildWithEntity,
    ]) {
      const worker = await Worker.create({
        connection,
        taskQueue: 'entity-q',
        workflow: wf,
      });
      await worker.run();
    }
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('should set and get entity data', async () => {
    const client = new Client({ connection });
    const handle = await client.workflow.start({
      args: ['Alice'],
      taskQueue: 'entity-q',
      workflowName: 'entitySetGet',
      workflowId: `ent-sg-${guid()}`,
      expire: 30,
    });
    const result = await handle.result();
    expect(result).toBe('hello Alice');
  }, 15_000);

  it('should merge entity data', async () => {
    const client = new Client({ connection });
    const handle = await client.workflow.start({
      args: ['Bob'],
      taskQueue: 'entity-q',
      workflowName: 'entityMerge',
      workflowId: `ent-mg-${guid()}`,
      expire: 30,
    });
    const result = await handle.result();
    expect(result).toEqual({ name: 'Bob', age: 30 });
  }, 15_000);

  it('should use entity and execChild together', async () => {
    const client = new Client({ connection });
    const handle = await client.workflow.start({
      args: ['Eve'],
      taskQueue: 'entity-q',
      workflowName: 'entityWithChild',
      workflowId: `ent-ec-${guid()}`,
      expire: 30,
    });
    const result = await handle.result();
    expect(result).toEqual({ status: 'done', childResult: 'child-Eve' });
  }, 15_000);

  it('should use entity + execChild with entity param', async () => {
    const client = new Client({ connection });
    const handle = await client.workflow.start({
      args: ['Frank'],
      taskQueue: 'entity-q',
      workflowName: 'entityExecChildWithEntity',
      workflowId: `ent-ece-${guid()}`,
      expire: 30,
    });
    const result = await handle.result();
    expect(result.user).toBe('Frank');
    expect(result.childResult).toEqual({ product: 'Frank', created: true });
  }, 15_000);
});
