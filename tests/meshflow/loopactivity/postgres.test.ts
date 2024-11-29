import { Pool as Postgres } from 'pg';

import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { guid } from '../../../modules/utils';
import { ProviderConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | loopactivity | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresPoolClient: any;

  beforeAll(async () => {
    //drop old tables (full clean start)
    const client = new Postgres(postgres_options);
    await dropTables(client);
    await client.end();

    //initialize the Pool (share with all instances)
    postgresPoolClient = new Postgres(postgres_options);
  });

  afterAll(async () => {
    await MeshFlow.shutdown();
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
        const client = new Client({ connection: {
          class: postgresPoolClient,
          options: {},
        }});

        handle = await client.workflow.start({
          args: [],
          taskQueue: 'loop-world',
          workflowName: 'example',
          workflowId: 'workflow-' + guid(),
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
          taskQueue: 'loop-world',
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
        const result = await handle.result();
        expect(result).toEqual(['Hello, 1!', 'Hello, 2!', 'Hello, 3!', 5]);
      }, 20_000);
    });
  });
});
