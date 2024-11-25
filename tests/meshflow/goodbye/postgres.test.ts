import * as Redis from 'redis';
import { Client as Postgres } from 'pg';

import config from '../../$setup/config';
import { MeshFlow } from '../../../services/meshflow';
import { RedisConnection } from '../../../services/connector/providers/redis';
import { ClientService } from '../../../services/meshflow/client';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient, RedisRedisClassType } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  redis_options,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | goodbye | `search, waitFor` | Postgres', () => {
  const prefix = 'bye-world-';
  const namespace = 'prod';
  let client: ClientService;
  let workflowGuid: string;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis as unknown as RedisRedisClassType,
      redis_options,
    );

    redisConnection.getClient().flushDb();
  });

  afterAll(async () => {
    await MeshFlow.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the EXPANDED Redis config', async () => {
        const connection = (await Connection.connect({
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Redis, options: redis_options },
        })) as ProvidersConfig;
        expect(connection).toBeDefined();
        expect(connection.sub).toBeDefined();
        expect(connection.stream).toBeDefined();
        expect(connection.store).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });

        workflowGuid = prefix + guid();

        const handle = await client.workflow.start({
          namespace,
          args: ['HotMesh'],
          taskQueue: 'goodbye-world',
          workflowName: 'example',
          workflowId: workflowGuid,
          expire: 120, //keep in Redis after completion for 120 seconds
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
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create a worker', async () => {
        const worker = await Worker.create({
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          namespace,
          taskQueue: 'goodbye-world',
          workflow: workflows.example,

          //INDEX the search space; if the index doesn't exist, it will be created
          //(this is supported by Redis backends with the FT module enabled)
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
      });

      it('should create a hook worker', async () => {
        const worker = await Worker.create({
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          namespace,
          taskQueue: 'goodbye-world',
          workflow: workflows.exampleHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

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
      }, 10_000);
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
          'SELECT job_id FROM hotmesh_prod_jobs_attributes WHERE field = $1 and value = $2',
          '_custom1',
          'meshflow',
        )) as unknown as { job_id: string }[];

        expect(results.length).toEqual(1);
        expect(results[0].job_id).toBeDefined();
      }, 7_500);
    });
  });
});
