import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as parentWorkflows from './parent/workflows';
import * as childWorkflows from './child/workflows';
import { nanoid } from 'nanoid';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { sleepFor } from '../../../modules/utils';

const { Connection, Client, Worker } = Durable;

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
    const redisConnection = await RedisConnection.connect(nanoid(), Redis, options);
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
      it('should connect a client and start a PARENT workflow execution', async () => {
        const client = new Client({ connection: { class: Redis, options }});
        handle = await client.workflow.start({
          args: ['PARENT'],
          taskQueue: 'parent-world',
          workflowName: 'parentExample',
          workflowId: nanoid(),
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the PARENT workflow worker', async () => {
        const worker = await Worker.create({
          connection: { class: Redis, options },
          taskQueue: 'parent-world',
          workflow: parentWorkflows.parentExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create and run the CHILD workflow worker', async () => {
        const worker = await Worker.create({
          connection: { class: Redis, options },
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
