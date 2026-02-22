import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { guid, sleepFor } from '../../../modules/utils';
import { MemFlow } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { ProviderNativeClient } from '../../../types';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | collision | Postgres', () => {
  const CONFLICTING_NAME = 'collision-child';
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
      it('should connect a client and start a workflow execution', async () => {
        const client = new Client({
          connection,
        });
        handle = await client.workflow.start({
          args: ['HotMesh'],
          taskQueue: 'collision-world',
          workflowName: 'example',
          workflowId: CONFLICTING_NAME, //the child will attempt to use this id
          expire: 600,
          config: {
            maximumAttempts: 3, //try 3 times and give up
            maximumInterval: '1 second',
            backoffCoefficient: 1,
          },
        });
        expect(handle.workflowId).toBeDefined();
      }, 95_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run the workers', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue: 'collision-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();

        const childWorker = await Worker.create({
          connection,
          taskQueue: 'collision-world',
          workflow: workflows.childExample,
        });
        await childWorker.run();
        expect(childWorker).toBeDefined();

        const fixableWorker = await Worker.create({
          connection,
          taskQueue: 'collision-world',
          workflow: workflows.fixableExample,
        });
        await fixableWorker.run();
        expect(fixableWorker).toBeDefined();
      }, 95_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it("should throw a 'DuplicateName' error and stop due to insufficient retries", async () => {
        try {
          const result = await handle.result();
          expect(result).toEqual(`Hello, HotMesh! Hello, HotMesh!`);
        } catch (error) {
          expect(error.message).toEqual(`Duplicate job: ${CONFLICTING_NAME}`);
        }
      }, 25_000);
    });
  });

  describe('End to End', () => {
    it("should throw a 'DuplicateName' error and then > retry, resolve (fix the name), succeed", async () => {
      const client = new Client({
        connection,
      });
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
    }, 25_000);
  });
});
