import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { guid, s, sleepFor } from '../../../modules/utils';
import { APP_VERSION } from '../../../services/memflow/schemas/factory';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as childWorkflows from './child/workflows';
import * as parentWorkflows from './parent/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | nested | Postgres', () => {
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
        try {
          const client = new Client({
            connection,
          });
          const h = client.workflow.start({
            args: ['PARENT', false], //setting to false optimizes workflow by suppressing the reentrant branch
            taskQueue: 'parent-world',
            workflowName: 'parentExample',
            workflowId: guid(),
            signalIn: false, //setting to false optimizes workflow by suppressing the reentrant branch
            expire: s('1m'),
          });
          //start another workflow to simulate startup collisions
          let handle2: WorkflowHandleService;
          const localH = client.workflow.start({
            args: ['PARENT', false],
            taskQueue: 'parent-world',
            workflowName: 'parentExample',
            workflowId: guid(),
            signalIn: false,
            expire: s('90s'),
          });
          [handle, handle2] = await Promise.all([h, localH]);
          expect(handle.workflowId).toBeDefined();
        } catch (e) {
          console.error(e);
        }
      }, 15_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the PARENT workflow worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue: 'parent-world',
          workflow: parentWorkflows.parentExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create and run the CHILD workflow worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue: 'child-world',
          workflow: childWorkflows.childExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 15_000);
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

  describe('MemFlow Control Plane', () => {
    describe('deployAndActivate', () => {
      it('should deploy the distributed executable', async () => {
        const client = new Client({
          connection,
        });

        //deploy next version
        await client.deployAndActivate(
          'memflow',
          (Number(APP_VERSION) + 1).toString(),
        );
      }, 25_000);
    });
  });
});
