import * as Redis from 'redis';

import config from '../../$setup/config'
import { deterministicRandom, guid, sleepFor } from '../../../modules/utils';
import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { RedisConnection } from '../../../services/connector/clients/redis';
import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | basic | `Durable Foundational`', () => {
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
          taskQueue: 'basic-world',
          workflowName: 'example',
          workflowId: 'workflow-' + guid(),
          expire: 600,
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      const connection = {
        class: Redis,
        options,
      }
      const taskQueue = 'basic-world';

      it('should create and run a parent worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue,
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create and run a child worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue,
          workflow: workflows.childExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        const signalId = 'abcdefg';
        //the test workflow calls   Durable.workflow.sleepFor('2s') 
        //...sleep to make sure the workfow is fully paused
        await sleepFor(15_000);
        //the test workflow uses  Durable.workflow.waitFor(signalId)  
        //...signal it and then await the result
        const signalPayload = { id: signalId, data: { hello: 'world', id: signalId } }
        await handle.signal(signalId, signalPayload);
        const result = await handle.result();
        const r1 = deterministicRandom(1);
        const r2 = deterministicRandom(4);
        expect(result).toEqual({
          jobBody: 'Hello from child workflow, start-HotMeshy!', 
          jobId: 'MyWorkflowId123', 
          oneTimeGreeting: {'complex': 'Basic, HotMesh!'}, 
          payload: {'data': {'hello': 'world', 'id': 'abcdefg'}, 'id': 'abcdefg'}, 
          proxyGreeting: {'complex': 'Basic, HotMesh!'},
          proxyGreeting3: {'complex': 'Basic, HotMesh3!'},
          proxyGreeting4: {'complex': 'Basic, HotMesh4!'},
          random1: r1,
          random2: r2,
        });
      }, 30_000);
    });
  });
});
