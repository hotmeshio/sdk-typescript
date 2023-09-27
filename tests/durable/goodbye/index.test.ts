import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as activities from './src/activities';
import { nanoid } from 'nanoid';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';

const { Connection, Client, NativeConnection, Worker } = Durable;

describe('DURABLE | goodbye | `Workflow Promise.all proxyActivities`', () => {
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
        //connect the client to Redis
        const connection = await Connection.connect({
          class: Redis,
          options,
        });
        const client = new Client({
          connection,
        });
        //`handle` is a global variable.
        //start a workflow execution (it will remain in the queue until a worker starts up)
        handle = await client.workflow.start({
          args: ['HotMesh'],
          taskQueue: 'goodbye-world',
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
        //connect to redis
        const connection = await NativeConnection.connect({
          class: Redis,
          options,
        });
        //create a worker (drains items from the queue/stream)
        const worker = await Worker.create({
          connection,
          namespace: 'default',
          taskQueue: 'goodbye-world',
          workflowsPath: require.resolve('./src/workflows'),
          activities,
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
        expect(result).toEqual('Hello, HotMesh! - Goodbye, HotMesh!');
      });
    });
  });
});
