import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderConfig } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';

import * as workflows from './src/workflows';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | loopactivity | Postgres', () => {
  const workflowId = `workflow-${guid()}`;
  const workflowName = 'example';
  const taskQueue = 'loop-world';
  let handle: WorkflowHandleService;
  let postgresClient: any;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await MemFlow.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the config', async () => {
        const connection = (await Connection.connect({
          class: Postgres,
          options: postgres_options,
        })) as ProviderConfig;
        expect(connection).toBeDefined();
        expect(connection.options).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({
          connection,
        });

        handle = await client.workflow.start({
          args: [],
          taskQueue,
          workflowName,
          workflowId,
        });
        expect(handle.workflowId).toBeDefined();
      }, 20_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue,
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 20_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('run await 3 functions and one sleepFor in parallel', async () => {
        const client = new Client({
          connection,
        });

        const localHandle = await client.workflow.getHandle(
          taskQueue,
          'example',
          workflowId,
        );

        const [localResult, globalResult] = await Promise.all([
          localHandle.result(),
          handle.result(),
        ]);

        expect(localResult).toEqual(['Hello, 1!', 'Hello, 2!', 'Hello, 3!', 5]);
        expect(globalResult).toEqual([
          'Hello, 1!',
          'Hello, 2!',
          'Hello, 3!',
          5,
        ]);
      }, 20_000);
    });
  });
});
