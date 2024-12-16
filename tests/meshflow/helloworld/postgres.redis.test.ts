import * as Redis from 'redis';
import { Client as Postgres } from 'pg';

import config from '../../$setup/config';
import { deterministicRandom, guid, sleepFor } from '../../../modules/utils';
import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { RedisConnection } from '../../../services/connector/providers/redis';
import { ProviderNativeClient, RedisRedisClassType } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import { dropTables } from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | hello | `Random Hello-World` | Postgres+Redis', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;
  const redis_options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };
  const postgres_options = {
    user: config.POSTGRES_USER,
    host: config.POSTGRES_HOST,
    database: config.POSTGRES_DB,
    password: config.POSTGRES_PASSWORD,
    port: config.POSTGRES_PORT,
  };

  beforeAll(async () => {
    // Initialize Postgres and drop tables (and data) from prior tests
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis as unknown as RedisRedisClassType,
      redis_options,
    );
    redisConnection.getClient().flushDb();
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
        expect(connection.stream).toBeDefined();
        expect(connection.store).toBeDefined();
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
          args: ['HotMesh'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: 'workflow-' + guid(),
          expire: 180,
          //NOTE: default is true; set to false to optimize any workflow that doesn't use hooks
          signalIn: false,
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
          taskQueue: 'hello-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        const result = await handle.result();
        //note: testing the deterministic random number generator
        const r1 = deterministicRandom(1);
        const r2 = deterministicRandom(3);
        expect(result).toEqual(`${r1} Hello, HotMesh! ${r2}`);
      }, 10_000);
    });
  });
});
