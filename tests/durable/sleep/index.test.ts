import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as workflows from './src/workflows';
import { nanoid } from 'nanoid';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | sleep | `Durable.workflow.sleep`', () => {
  let handle: WorkflowHandleService;
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
    await Durable.Client.shutdown();
    await Durable.Worker.shutdown();
    await StreamSignaler.stopConsuming();
    await RedisConnection.disconnectAll();
  });

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
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({ connection: { class: Redis, options }});
        //NOTE: `handle` is a global variable.
        handle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: nanoid(),
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
        const result = await handle.result();
        expect(result).toEqual('Hello, ColdMush!');
      }, 60_000);
    });
  });
});