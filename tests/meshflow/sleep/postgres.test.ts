import Redis from 'ioredis';
import { Client as Postgres } from 'pg';

import { HMSH_CODE_INTERRUPT } from '../../../modules/enums';
import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { guid, sleepFor } from '../../../modules/utils';
import { StreamError } from '../../../types';
import { ProvidersConfig, ProviderNativeClient } from '../../../types/provider';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | sleep | Postgres', () => {
  let handle: WorkflowHandleService;
  let workflowGuid: string;
  let interruptedWorkflowGuid: string;
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

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
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

  describe('Client', () => {
    describe('start', () => {
      workflowGuid = guid();
      interruptedWorkflowGuid = guid();
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });

        handle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: workflowGuid,
        });
        expect(handle.workflowId).toBeDefined();

        const localHandle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: interruptedWorkflowGuid,
        });
        expect(localHandle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should interrupt a workflow execution and throw a `410` error', async () => {
        const client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });
        const localHandle = await client.workflow.getHandle(
          'hello-world',
          workflows.example.name,
          interruptedWorkflowGuid,
        );
        const hotMesh = localHandle.hotMesh;
        await sleepFor(1_000); //let the activity start running
        hotMesh.interrupt(`${hotMesh.appId}.execute`, interruptedWorkflowGuid, {
          descend: true,
          expire: 1,
        });
        try {
          //subscribe to the workflow result (this will throw an `interrupted` error)
          await localHandle.result();
        } catch (e: any) {
          expect((e as StreamError).job_id).toEqual(interruptedWorkflowGuid);
          expect((e as StreamError).code).toEqual(HMSH_CODE_INTERRUPT);
        }
      });

      it('should return the workflow execution result', async () => {
        const client = new Client({
          connection: {
            store: { class: Postgres, options: postgres_options }, //and search
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
        });
        const localHandle = await client.workflow.getHandle(
          'hello-world',
          workflows.example.name,
          workflowGuid,
        );
        const result = await localHandle.result();
        expect(result).toEqual('Hello, ColdMush!');
        //allow signals to self-clean
        await sleepFor(5_000);
      }, 60_000);
    });
  });
});
