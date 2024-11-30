import Redis from 'ioredis';
import { Client as Postgres } from 'pg';

import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { guid, sleepFor } from '../../../modules/utils';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';
import { ProviderNativeClient, ProvidersConfig } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | retry | Postgres', () => {
  let handle: WorkflowHandleService;
  const errorCycles = 3;
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
          store: { class: Postgres, options: postgres_options },
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
        const client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });
        //NOTE: `handle` is a global variable.
        handle = await client.workflow.start({
          args: [{ amount: errorCycles }],
          taskQueue: 'sleep-world',
          workflowName: 'example',
          workflowId: guid(),
          expire: 120,
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
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          taskQueue: 'sleep-world',
          workflow: workflows.default.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        const result = await handle.result();
        expect(result).toEqual(errorCycles);
      }, 20_000);
    });
  });
});
