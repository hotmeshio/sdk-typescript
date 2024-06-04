import Redis from 'ioredis';

import config from '../../$setup/config';
import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { guid, sleepFor } from '../../../modules/utils';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | signal | `Durable.workflow.signal`', () => {
  let handle: WorkflowHandleService;
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      options,
    );
    redisConnection.getClient().flushdb();
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
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
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({ connection: { class: Redis, options } });
        //NOTE: `handle` is a global variable.
        handle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: guid(),
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
        //signal using the original client handle
        await sleepFor(3_000);
        await handle.signal('abcdefg', { name: 'WarmMash' });

        //signal by instancing a new client connection
        await sleepFor(1_000);
        const client = new Client({ connection: { class: Redis, options } });
        await client.workflow.signal('hijklmnop', { name: 'WarnCrash' });

        const result = await handle.result();
        expect(result).toEqual([
          'Hello, stranger!',
          { name: 'WarmMash' },
          { name: 'WarnCrash' },
          'Hello, ColdMush!',
        ]);
      }, 15_000);
    });
  });
});
