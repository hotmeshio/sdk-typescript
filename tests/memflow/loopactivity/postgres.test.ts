import { Pool as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { guid } from '../../../modules/utils';
import { ProviderConfig } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | loopactivity | Postgres', () => {
  const workflowId = `workflow-${guid()}`;
  const workflowName = workflows.example.name;
  const taskQueue = 'loop-world';
  let handle: WorkflowHandleService;
  let postgresPoolClient: any;

  beforeAll(async () => {
    //instance a native postgres poolClient
    postgresPoolClient = new Postgres(postgres_options);

    //drop old tables (full clean start) ...  did it release?
    await dropTables(postgresPoolClient);
  });

  afterAll(async () => {
    await MemFlow.shutdown();
  }, 15_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the config', async () => {
        const connection = (await Connection.connect({
          class: postgresPoolClient,
          options: {},
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
          connection: {
            class: postgresPoolClient,
            options: {},
          },
        });

        handle = await client.workflow.start({
          args: [],
          taskQueue,
          workflowName,
          workflowId,
          expire: 120,
        });
        expect(handle.workflowId).toBeDefined();
      }, 20_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: postgresPoolClient,
            options: {},
          },
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
          connection: {
            class: postgresPoolClient,
            options: {},
          },
        });

        const localHandle = await client.workflow.getHandle(
          taskQueue,
          workflows.example.name,
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
