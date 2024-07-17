import Redis from 'ioredis';

import config from '../../$setup/config';
import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { guid, sleepFor } from '../../../modules/utils';

import * as childWorkflows from './child/workflows';
import * as parentWorkflows from './parent/workflows';
import { APP_VERSION } from '../../../services/durable/schemas/factory';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | nested | `workflow.execChild`', () => {
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
      it('should connect a client and start a PARENT workflow execution', async () => {
        try {
          const client = new Client({ connection: { class: Redis, options } });
          const h = client.workflow.start({
            args: ['PARENT', false], //setting to false optimizes workflow by suppressing the reentrant branch
            taskQueue: 'parent-world',
            workflowName: 'parentExample',
            workflowId: guid(),
            signalIn: false, //setting to false optimizes workflow by suppressing the reentrant branch
            expire: 500,
          });
          //start another workflow to simulate startup collisions
          let handle2: WorkflowHandleService;
          const localH = client.workflow.start({
            args: ['PARENT', false],
            taskQueue: 'parent-world',
            workflowName: 'parentExample',
            workflowId: guid(),
            signalIn: false,
            expire: 500,
          });
          [handle, handle2] = await Promise.all([h, localH]);
          expect(handle.workflowId).toBeDefined();
        } catch (e) {
          console.error(e);
        }
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
      }, 15_000);
    });
  });

  describe('Durable Control Plane', () => {
    describe('deployAndActivate', () => {
      it('should deploy the distributed executable', async () => {
        const client = new Client({ connection: { class: Redis, options } });
        //deploy next version
        await client.deployAndActivate('durable', (Number(APP_VERSION) + 1).toString());
      }, 25_000);
    });
  });

});
