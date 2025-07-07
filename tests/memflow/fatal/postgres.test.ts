import Redis from 'ioredis';
import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { MemFlowFatalError } from '../../../modules/errors';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient, ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | fatal | Postgres', () => {
  const NAME = 'hot-mess';
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
    await MemFlow.shutdown();
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
        handle = await client.workflow.start({
          args: [{ name: NAME }],
          taskQueue: 'fatal-world',
          workflowName: 'example',
          workflowId: guid(),
          expire: 120, //ensures the failed workflows aren't scrubbed too soon (so they can be reviewed (but unnecessary for the test to succeed))
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
          taskQueue: 'fatal-world',
          workflow: workflows.default.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should throw the fatal error with `code` and `message`', async () => {
        try {
          //the activity will throw `598 [MemFlowFatalError]; the workflow will rethrow;`
          await handle.result();
          throw new Error('This should not be thrown');
        } catch (err) {
          expect(err.message).toEqual(`stop-retrying-please-${NAME}`);
          expect(err.code).toEqual(new MemFlowFatalError('').code);
        }
      });
    });
  });
});
