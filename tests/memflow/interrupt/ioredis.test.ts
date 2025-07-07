import Redis from 'ioredis';

import config from '../../$setup/config';
import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { guid, sleepFor } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { ProviderConfig } from '../../../types/provider';

import * as childWorkflows from './child/workflows';
import * as parentWorkflows from './parent/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | interrupt | `workflow.interrupt`', () => {
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
    await MemFlow.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Redis config', async () => {
        const connection = (await Connection.connect({
          class: Redis,
          options,
        })) as ProviderConfig;
        expect(connection).toBeDefined();
        expect(connection.options).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a PARENT workflow execution', async () => {
        const client = new Client({ connection: { class: Redis, options } });
        handle = await client.workflow.start({
          args: ['PARENT'],
          taskQueue: 'parent-world',
          workflowName: 'parentExample',
          workflowId: guid(),
          expire: 600,
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the CHILD workflow worker', async () => {
        const worker = await Worker.create({
          connection: { class: Redis, options },
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
          connection: { class: Redis, options },
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
        const client = new Client({ connection: { class: Redis, options } });
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
