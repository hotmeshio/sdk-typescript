import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as activities from './src/activities';
import { v4 as uuidv4 } from 'uuid';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';


const { Connection, Client, NativeConnection, Worker } = Durable;

describe('DURABLE | loopactivity | `Iterate Same Activity`', () => {
  let handle: WorkflowHandleService;
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(uuidv4(), Redis, options);
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
        //start the workflow (it will be executed by the worker...see below)
        handle = await client.workflow.start({
          args: ['HotMesh'],
          taskQueue: 'loop-world',
          workflowName: 'example',
          workflowId: 'workflow-' + uuidv4(),
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
          taskQueue: 'loop-world',
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
        expect(result).toEqual(['Hello, 1!', 'Hello, 2!', 'Hello, 3!']);
      }, 10_000);
    });
  });
});
