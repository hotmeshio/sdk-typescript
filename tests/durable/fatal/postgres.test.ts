import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { DurableFatalError } from '../../../modules/errors';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | fatal | Postgres', () => {
  const NAME = 'hot-mess';
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
    await Durable.shutdown();
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
          args: [{ name: NAME }],
          taskQueue: 'fatal-world',
          workflowName: 'example',
          workflowId: guid(),
          expire: 120, //ensures the failed workflows aren't scrubbed too soon (so they can be reviewed (but unnecessary for the test to succeed))
        });
        expect(handle.workflowId).toBeDefined();
      }, 10_000);
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create and run a worker', async () => {
        const worker = await Worker.create({
          connection,
          taskQueue: 'fatal-world',
          workflow: workflows.default.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should throw the fatal error with `code` and `message`', async () => {
        try {
          //the activity will throw `598 [DurableFatalError]; the workflow will rethrow;`
          await handle.result();
          throw new Error('This should not be thrown');
        } catch (err) {
          expect(err.message).toEqual(`stop-retrying-please-${NAME}`);
          expect(err.code).toEqual(new DurableFatalError('').code);
        }
      });
    });
  });
});
