import * as Redis from 'redis';

import config from '../../$setup/config'
import { guid, sleepFor } from '../../../modules/utils';
import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/redis';
import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | collision | `Naming Conflict Fatal Error`', () => {
  const CONFLICTING_NAME = 'collision-child';
  let handle: WorkflowHandleService;
  const options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(guid(), Redis, options);
    redisConnection.getClient().flushDb();
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
        const client = new Client({ connection: { class: Redis, options }});
        handle = await client.workflow.start({
          args: ['HotMesh'],
          taskQueue: 'collision-world',
          workflowName: 'example',
          workflowId: CONFLICTING_NAME, //the child will attempt to use this id
          expire: 600,
          config: {
            maximumAttempts: 4, //try 4 times and give up
            maximumInterval: '1 second',
            backoffCoefficient: 1,
          },
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the workers', async () => {
        const worker = await Worker.create({
          connection: {
            class: Redis,
            options,
          },
          taskQueue: 'collision-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();

        const childWorker = await Worker.create({
          connection: {
            class: Redis,
            options,
          },
          taskQueue: 'collision-world',
          workflow: workflows.childExample,
        });
        await childWorker.run();
        expect(childWorker).toBeDefined();

        const fixableWorker = await Worker.create({
          connection: {
            class: Redis,
            options,
          },
          taskQueue: 'collision-world',
          workflow: workflows.fixableExample,
        });
        await fixableWorker.run();
        expect(fixableWorker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should throw a \'DuplicateName\' error and stop due to insufficient retries', async () => {
        try {
          const result = await handle.result();
          expect(result).toEqual(`Hello, HotMesh! Hello, HotMesh!`);
        } catch (error) {
          expect(error.message).toEqual(`Duplicate job: ${CONFLICTING_NAME}`);
        }
      }, 12_500);
    });
  });

  describe('End to End', () => {
    it('should throw a \'DuplicateName\' error and then > retry, resolve (fix the name), succeed', async () => {
      const client = new Client({ connection: { class: Redis, options }});
      const badCount = 1;
      const handle = await client.workflow.start({
        args: [badCount],
        taskQueue: 'collision-world',
        workflowName: 'fixableExample',
        workflowId: `fixable-${CONFLICTING_NAME}`,
        expire: 600,
        config: {
          maximumAttempts: 3, //on try 3 we'll fix, and it will succeed
          maximumInterval: '1 second',
          backoffCoefficient: 1,
        },
      });
      expect(handle.workflowId).toBe(`fixable-${CONFLICTING_NAME}`);
      const outcome = await handle.result<string>({ throwOnError: false });
      expect(outcome).toEqual('Hello, FIXED!');
    }, 12_500);
  });
});
