import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { ClientService } from '../../../services/memflow/client';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as workflows from './src/workflows';
import * as childWorkflows from './child/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | hook & search | Postgres', () => {
  const namespace = 'staging';
  const prefix = 'bye-world-';
  let client: ClientService;
  let workflowGuid: string;
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await MemFlow.shutdown();
  }, 15_000);

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
        client = new Client({
          connection,
        });
        workflowGuid = prefix + guid();

        const handle = await client.workflow.start({
          namespace,
          guid: 'client',
          args: ['HookMesh'],
          taskQueue: 'hook-world',
          workflowName: 'example',
          workflowId: workflowGuid,
          expire: 600,
          //SEED the initial workflow state with data
          search: {
            data: {
              fred: 'flintstone',
              barney: 'rubble',
            },
          },
        });
        expect(handle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create a worker', async () => {
        const worker = await Worker.create({
          namespace,
          guid: 'parent-worker',
          connection,
          taskQueue: 'hook-world',
          workflow: workflows.example,

          search: {
            index: 'bye-bye',
            prefix: [prefix],
            schema: {
              custom1: {
                type: 'TEXT',
                sortable: true,
              },
              custom2: {
                type: 'NUMERIC', //or TAG
                sortable: true,
              },
            },
          },
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);

      it('should create and run the CHILD workflow worker', async () => {
        //the main flow has an execChild command which will be serviced
        //by this worker
        const worker = await Worker.create({
          namespace,
          guid: 'child-worker',
          connection,
          taskQueue: 'child-world',
          workflow: childWorkflows.childExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 15_000);

      it('should create a hook worker', async () => {
        const worker = await Worker.create({
          namespace,
          guid: 'hook-worker',
          connection,
          taskQueue: 'hook-world',
          workflow: workflows.exampleHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);

      it('should spawn a hook and run the hook function', async () => {
        //sleep so the main thread fully executes and gets into a paused state
        //where it is awaiting a signal
        await sleepFor(2_500);

        //send a `hook` to spawn a hook thread attached to this workflow
        //the exampleHook function will be invoked in job context, allowing
        //it to read/write/augment shared job state with transactional integrity
        await client.workflow.hook({
          namespace,
          taskQueue: 'hook-world',
          workflowName: 'exampleHook',
          workflowId: workflowGuid,
          args: ['HotMeshHook'],
        });
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        //get the workflow handle and wait for the result
        const handle = await client.workflow.getHandle(
          'hook-world',
          workflows.example.name,
          workflowGuid,
          namespace,
        );
        const result = await handle.result({ state: true });
        expect(result).toEqual('Hello, HookMesh! - Goodbye, HookMesh!');
        const exported = await handle.export({
          allow: ['timeline', 'status', 'data', 'state'],
          values: false,
        });
        expect(exported.status).not.toBeUndefined();
        expect(exported.data?.fred).toBe('flintstone');
        expect(exported.state?.data.done).toBe(true);

        //execute raw SQL to locate the custom data attribute
        //NOTE: always include an underscore prefix before the search term (e.g., `_custom1`).
        //      HotMesh uses this to avoid collisions with reserved words
        const results = (await client.workflow.search(
          'hook-world',
          workflows.example.name,
          namespace,
          'sql',
          'SELECT job_id FROM staging.jobs_attributes WHERE field = $1 and value = $2',
          '_custom1',
          'memflow',
        )) as unknown as { job_id: string }[];
        expect(results.length).toEqual(1);
        expect(results[0].job_id).toBeDefined();
      }, 35_000);
    });
  });
});
