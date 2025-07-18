import { Client as Postgres } from 'pg';

import { HMSH_CODE_INTERRUPT } from '../../../modules/enums';
import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { guid, sleepFor } from '../../../modules/utils';
import { StreamError } from '../../../types';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | sleep | Postgres', () => {
  let handle: WorkflowHandleService;
  let workflowGuid: string;
  let interruptedWorkflowGuid: string;
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

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
      it('should echo the config', async () => {
        const connection = (await Connection.connect({
          class: Postgres,
          options: postgres_options,
        })) as ProviderConfig;
        expect(connection).toBeDefined();
        expect(connection.options).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue: 'hello-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('Client', () => {
    describe('start', () => {
      workflowGuid = guid();
      interruptedWorkflowGuid = guid();
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({
          connection,
        });

        handle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: workflowGuid,
        });
        expect(handle.workflowId).toBeDefined();

        const localHandle = await client.workflow.start({
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: interruptedWorkflowGuid,
        });
        expect(localHandle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should interrupt a workflow execution and throw a `410` error', async () => {
        const client = new Client({
          connection,
        });
        const localHandle = await client.workflow.getHandle(
          'hello-world',
          workflows.example.name,
          interruptedWorkflowGuid,
        );
        const hotMesh = localHandle.hotMesh;
        await sleepFor(1_000); //let the activity start running
        hotMesh.interrupt(`${hotMesh.appId}.execute`, interruptedWorkflowGuid, {
          descend: true,
          expire: 1,
        });
        try {
          //subscribe to the workflow result (this will throw an `interrupted` error)
          await localHandle.result();
        } catch (e: any) {
          expect((e as StreamError).job_id).toEqual(interruptedWorkflowGuid);
          expect((e as StreamError).code).toEqual(HMSH_CODE_INTERRUPT);
        }
      });

      it('should return the workflow execution result', async () => {
        const client = new Client({
          connection,
        });
        const localHandle = await client.workflow.getHandle(
          'hello-world',
          workflows.example.name,
          workflowGuid,
        );
        const result = await localHandle.result();
        expect(result).toEqual('Hello, ColdMush!');
        //allow signals to self-clean
        await sleepFor(5_000);
      }, 60_000);
    });
  });
});
