import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as workflows from './src/workflows';
import { nanoid } from 'nanoid';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';
import { sleepFor } from '../../../modules/utils';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | sleep | `Durable.workflow.sleepFor`', () => {
  let handle: WorkflowHandleService;
  let workflowGuid: string;
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(nanoid(), Redis, options);
    redisConnection.getClient().flushdb();
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.Client.shutdown();
    await Durable.Worker.shutdown();
    await StreamSignaler.stopConsuming();
    await RedisConnection.disconnectAll();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Redis config', async () => {
        const connection = await Connection.connect({
          class: Redis,
          options,
        });
        expect(connection).toBeDefined();
        expect(connection.options).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      workflowGuid = nanoid();
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({ connection: { class: Redis, options }});
        //NOTE: `handle` is a global variable.
        handle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: workflowGuid,
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Redis,
            options,
          },
          taskQueue: 'hello-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        const client = new Client({ connection: { class: Redis, options }});
        const localHandle = await client.workflow.getHandle(
          'hello-world',
          workflows.example.name,
          workflowGuid
        );
        const result = await localHandle.result();
        expect(result).toEqual('Hello, ColdMush!');
        //allow signals to self-clean
        await sleepFor(1_000);
      }, 60_000);
    });
  });
});
