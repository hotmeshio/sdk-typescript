import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as parentWorkflows from './parent/workflows';
import * as childWorkflows from './child/workflows';
const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | interrupt | Postgres', () => {
  let handle: WorkflowHandleService;
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

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a PARENT workflow execution', async () => {
        const client = new Client({
          connection,
        });
        handle = await client.workflow.start({
          args: ['PARENT'],
          taskQueue: 'parent-world',
          workflowName: 'parentExample',
          workflowId: guid(),
          expire: 600,
        });
        expect(handle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the CHILD workflow worker', async () => {
        const worker = await Worker.create({
          connection,
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
          connection,
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
        const client = new Client({
          connection,
        });
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
