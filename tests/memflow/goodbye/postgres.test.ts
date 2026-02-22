import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { ClientService } from '../../../services/memflow/client';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | goodbye | `search, waitFor` | Postgres', () => {
  const prefix = 'bye-world-';
  const namespace = 'prod';
  let client: ClientService;
  let workflowGuid: string;
  let postgresClient: ProviderNativeClient;

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
      it('should echo the Postgres config', async () => {
        const connection = (await Connection.connect({
          class: Postgres,
          options: postgres_options,
        })) as ProvidersConfig;
        expect(connection).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        client = new Client({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
        });

        workflowGuid = prefix + guid();

        const handle = await client.workflow.start({
          namespace,
          args: ['HotMesh'],
          taskQueue: 'goodbye-world',
          workflowName: 'example',
          workflowId: workflowGuid,
          expire: 120, //keep in DB after completion for 120 seconds (expire is a soft-delete)
          //SEED the initial workflow state with data (this is
          //different than the 'args' input data which the workflow
          //receives as its first argument...this data is available
          //to the workflow via the 'search' object)
          //NOTE: data can be updated during workflow execution
          search: {
            data: {
              fred: 'flintstone',
              barney: 'rubble',
            },
          },
        });
        expect(handle.workflowId).toBeDefined();
      }, 60_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create a worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'goodbye-world',
          workflow: workflows.example,

          //INDEX the search space; if the index doesn't exist, it will be created
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
      }, 60_000);

      it('should create a hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'goodbye-world',
          workflow: workflows.exampleHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 60_000);

      it('should create a hook client and publish to invoke a hook', async () => {
        //sleep so the main thread gets into a paused state
        await sleepFor(2_000);

        //send a hook to spawn a hook thread attached to this workflow
        await client.workflow.hook({
          namespace,
          taskQueue: 'goodbye-world',
          workflowName: 'exampleHook',
          workflowId: workflowGuid,
          args: ['HotMeshHook'],
        });
      }, 60_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        const handle = await client.workflow.getHandle(
          'goodbye-world',
          workflows.example.name,
          workflowGuid,
          namespace,
        );
        const result = await handle.result();
        expect(result).toEqual('Hello, HotMesh! - Goodbye, HotMesh!');

        //call a search query to look for
        //NOTE: include an underscore before the search term (e.g., `_term`).
        //      (HotMesh uses `_` to avoid collisions with reserved words
        const results = (await client.workflow.search(
          'goodbye-world',
          workflows.example.name,
          namespace,
          'sql',
          'SELECT job_id FROM prod.jobs_attributes WHERE field = $1 and value = $2',
          '_custom1',
          'memflow',
        )) as unknown as { job_id: string }[];

        expect(results.length).toEqual(1);
        expect(results[0].job_id).toBeDefined();
      }, 60_000);

      it('should retrieve all user data fields', async () => {
        // Get all user data using the new static method
        const hotMeshClient = await client.getHotMeshClient('goodbye-world', namespace);  // Use test namespace
        if (!hotMeshClient) throw new Error('HotMesh client not initialized');

        const allUserData = await MemFlow.Search.findAllUserData(workflowGuid, hotMeshClient);

        // Verify initial data (set in workflow.start)
        expect(allUserData.fred).toBe('flintstone');
        expect(allUserData.barney).toBe('rubble');
        
        // Verify workflow-set data (set via search.set)
        expect(allUserData.custom1).toBe('memflow');
        expect(allUserData.custom2).toBe('55');
        expect(allUserData.jimbo).toBe('jones');
        expect(allUserData.counter).toBe('111'); // 10 + 1 + 100 from workflow and hook
        expect(allUserData.multer).toBe('120'); // 12 * 10 (restored from log value and rounded)
      }, 60_000);
    });
  });
});
