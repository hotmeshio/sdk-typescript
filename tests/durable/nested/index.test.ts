import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as parentWorkflows from './parent/workflows';
import * as childWorkflows from './child/workflows';
import { v4 as uuidv4 } from 'uuid';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';

const { Connection, Client, NativeConnection, Worker } = Durable;

describe('DURABLE | nested | `workflow.executeChild`', () => {
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
      it('should connect a client and start a PARENT workflow execution', async () => {
        //connect the client to Redis
        const connection = await Connection.connect({
          class: Redis,
          options,
        });
        const client = new Client({
          connection,
        });
        handle = await client.workflow.start({
          args: ['PARENT'],
          taskQueue: 'parent-world',
          workflowName: 'parentExample',
          workflowId: uuidv4(),
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the PARENT workflow worker', async () => {
        //connect to redis
        const connection = await NativeConnection.connect({
          class: Redis,
          options,
        });
        //create a worker (drains items added by the client)
        const worker = await Worker.create({
          connection,
          namespace: 'default',
          taskQueue: 'parent-world',
          workflow: parentWorkflows.parentExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create and run the CHILD workflow worker', async () => {
        //connect to redis
        const connection = await NativeConnection.connect({
          class: Redis,
          options,
        });
        //create a worker (drains items added by the client)
        const worker = await Worker.create({
          connection,
          namespace: 'default',
          taskQueue: 'child-world',
          workflow: childWorkflows.childExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the PARENT workflow execution result', async () => {
        const expectedOutput = {
          activityOutput: 'parentActivity, PARENT!',
          childWorkflowOutput: 'childActivity, PARENT to CHILD!',
        };
        const result = await handle.result();
        expect(result).toEqual(expectedOutput);
      }, 10_000);
    });
  });
});
