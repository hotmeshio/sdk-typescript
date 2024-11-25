import Redis from 'ioredis';
import { Client as Postgres } from 'pg';

import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient, ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | loopactivity | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      redis_options,
    );
    redisConnection.getClient().flushdb();
  });

  afterAll(async () => {
    await sleepFor(1500);
    await MeshFlow.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Redis config', async () => {
        const connection = (await Connection.connect({
          store: { class: Postgres, options: postgres_options }, //and search
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Redis, options: redis_options },
        })) as ProvidersConfig;
        expect(connection).toBeDefined();
        expect(connection.sub).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({ connection: {
          store: { class: Postgres, options: postgres_options }, //and search
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Redis, options: redis_options },
        }});
        //NOTE: `handle` is a global variable.
        handle = await client.workflow.start({
          args: [],
          taskQueue: 'loop-world',
          workflowName: 'example',
          workflowId: 'workflow-' + guid(),
          expire: 120, //ensures the failed workflows aren't scrubbed too soon (so they can be reviewed)
        });
        expect(handle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          taskQueue: 'loop-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('run await 3 functions and one sleepFor in parallel', async () => {
        const result = await handle.result();
        expect(result).toEqual(['Hello, 1!', 'Hello, 2!', 'Hello, 3!', 5]);
      }, 15_000);
    });
  });
});
