import Redis from 'ioredis';

import config from '../../$setup/config';
import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { guid, sleepFor } from '../../../modules/utils';
import { DurableMaxedError } from '../../../modules/errors';

import { example, state as STATE } from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | unknown | `Workflow Retryable Unknown Error`', () => {
  let handle: WorkflowHandleService;
  const toThrowCount = 3;
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
        handle = await client.workflow.start({
          args: [toThrowCount],
          taskQueue: 'unknown-world',
          workflowName: 'example',
          workflowId: guid(),
          expire: 120,
          config: {
            //speed up the default retry strategy (so the test completes in time)
            maximumAttempts: toThrowCount + 1,
            backoffCoefficient: 1,
            maximumInterval: '1s',
          },
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
          taskQueue: 'unknown-world',
          workflow: example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return successfully after retrying a workflow-generated error', async () => {
        const result = await handle.result();
        expect(result).toBe(toThrowCount);
      }, 15_000);
    });
  });

  describe('End to End', () => {
    it('should connect a client, start a workflow, and throw max retries exceeded', async () => {
      //reset counter that increments with each workflow run
      STATE.count = 0;

      //instance a durable client and start the workflow
      const client = new Client({ connection: { class: Redis, options } });
      const handle = await client.workflow.start({
        args: [toThrowCount],
        taskQueue: 'unknown-world',
        workflowName: 'example',
        workflowId: guid(),
        expire: 120,
        config: {
          //if allowed max is 1 less than errors, 597 should be thrown (max exceeded)
          maximumAttempts: toThrowCount - 1,
          backoffCoefficient: 1,
          maximumInterval: '1s',
        },
      });
      expect(handle.workflowId).toBeDefined();

      try {
        await handle.result();
        throw new Error('This should not be thrown');
      } catch (error) {
        //the workflow throws this error
        expect(error.message).toEqual('recurring-test-error');

        //...but the final error response will be a DurableMaxedError after the workflow gives up
        expect(error.code).toEqual(new DurableMaxedError('').code);

        //expect a stack trace
        expect(error.stack).toBeDefined();
      }
    }, 15_000);
  });
});
