import Redis from 'ioredis';

import config from '../../$setup/config';
import { MeshFlow } from '../../../services/meshflow';
import { WorkflowHandleService } from '../../../services/meshflow/handle';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { MeshFlowFatalError } from '../../../modules/errors';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderConfig } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MeshFlow;

describe('MESHFLOW | fatal | `Workflow Promise.all proxyActivities`', () => {
  const NAME = 'hot-mess';
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
    await MeshFlow.shutdown();
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
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({ connection: { class: Redis, options } });
        handle = await client.workflow.start({
          args: [{ name: NAME }],
          taskQueue: 'fatal-world',
          workflowName: 'example',
          workflowId: guid(),
          expire: 120, //ensures the failed workflows aren't scrubbed too soon (so they can be reviewed (but unnecessary for the test to succeed))
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
          taskQueue: 'fatal-world',
          workflow: workflows.default.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should throw the fatal error with `code` and `message`', async () => {
        try {
          //the activity will throw `598 [MeshFlowFatalError]; the workflow will rethrow;`
          await handle.result();
          throw new Error('This should not be thrown');
        } catch (err) {
          expect(err.message).toEqual(`stop-retrying-please-${NAME}`);
          expect(err.code).toEqual(new MeshFlowFatalError('').code);
        }
      });
    });
  });
});
