import Redis from 'ioredis';
import { Client as Postgres } from 'pg';

import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { guid, sleepFor } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { ProviderNativeClient, ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as parentWorkflows from './parent/workflows';
import * as childWorkflows from './child/workflows';
const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | interrupt | Postgres', () => {
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
      it('should connect a client and start a PARENT workflow execution', async () => {
        const client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });
        handle = await client.workflow.start({
          args: ['PARENT'],
          taskQueue: 'parent-world',
          workflowName: 'parentExample',
          workflowId: guid(),
          expire: 600,
        });
        expect(handle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the CHILD workflow worker', async () => {
        const worker = await Worker.create({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          taskQueue: 'child-world',
          workflow: childWorkflows.childExample,
          options: {
            logLevel: HMSH_LOGLEVEL,
          },
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create and run the PARENT workflow worker', async () => {
        const worker = await Worker.create({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          taskQueue: 'parent-world',
          workflow: parentWorkflows.parentExample,
          options: {
            logLevel: HMSH_LOGLEVEL,
          },
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  //add long test, spawn a timout that will throw a 410, await the handle
  //verify the error code is 410

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should run a PARENT that starts and then interrupts a CHILD workflow', async () => {
        const expectedOutput = {
          childWorkflowOutput: 'interrupt childActivity, PARENT to CHILD!',
          cancelledWorkflowId: 'jimbo2',
        };
        const result = (await handle.result()) as {
          cancelledWorkflowId: string;
        };
        expect(result).toEqual(expectedOutput);
        const client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });
        //get a handle to the interrupted workflow
        handle = await client.workflow.getHandle(
          'child-world', //task queue
          'childExample', //workflow
          result.cancelledWorkflowId,
        );
        const state = await handle.state(true);
        //job state (js) is @ -1billion when interrupted (depending upon semaphore state when decremented)
        expect(state.metadata.js).toBeLessThan(-1_000_000);
        const rslt = await handle.result({ state: true });
        //result is undefined, since it was interrupted; there is no return;
        expect(rslt).toBeUndefined();
      }, 15_000);
    });
  });
});
