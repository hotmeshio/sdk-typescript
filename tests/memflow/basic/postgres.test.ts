import { Client as Postgres } from 'pg';

import { dropTables, postgres_options } from '../../$setup/postgres';
import { deterministicRandom, guid, sleepFor } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { ProviderConfig, ProviderNativeClient } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | baseline | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await sleepFor(1500);
    await MemFlow.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Redis config', async () => {
        const connection = (await Connection.connect({
          class: Postgres,
          options: postgres_options,
        })) as ProviderConfig;
        expect(connection).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
        });
        handle = await client.workflow.start({
          args: ['HotMesh'],
          taskQueue: 'basic-world',
          workflowName: 'example',
          workflowId: 'workflow-' + guid(),
          expire: 600,
        });
        expect(handle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      const connection = {
        class: Postgres,
        options: postgres_options,
      };
      const taskQueue = 'basic-world';

      it('should create and run a parent worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue,
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 15_000);

      it('should create and run a child worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue,
          workflow: workflows.childExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        const signalId = 'abcdefg';
        //the test workflow calls
        //...sleep to make sure the workfow is fully paused
        await sleepFor(10_000);
        //the test workflow uses  MemFlow.workflow.waitFor(signalId)
        //...signal it and then await the result
        const signalPayload = {
          id: signalId,
          data: { hello: 'world', id: signalId },
        };
        await handle.signal(signalId, signalPayload);
        const result = await handle.result();
        const r1 = deterministicRandom(1);
        const r2 = deterministicRandom(4);
        expect(result).toEqual({
          jobId: 'MyWorkflowId123',
          payload: { data: { hello: 'world', id: 'abcdefg' }, id: 'abcdefg' },
          proxyGreeting: { complex: 'Basic, HotMesh!' },
          proxyGreeting3: { complex: 'Basic, HotMesh3!' },
          proxyGreeting4: { complex: 'Basic, HotMesh4!' },
          random1: r1,
          random2: r2,
        });
      }, 45_000);
    });
  });
});
