import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | signal | Postgres', () => {
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
          args: ['ColdMush'],
          taskQueue: 'hello-world',
          workflowName: 'example',
          workflowId: guid(),
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
          taskQueue: 'hello-world',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      }, 10_000);
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        //signal using the original client handle
        await sleepFor(3_000);
        await handle.signal('abcdefg', { name: 'WarmMash' });

        //signal by instancing a new client connection
        await sleepFor(1_000);
        const client = new Client({
          connection,
        });
        await client.workflow.signal('hijklmnop', { name: 'WarnCrash' });

        const result = await handle.result();
        expect(result).toEqual([
          'Hello, stranger!',
          { name: 'WarmMash' },
          { name: 'WarnCrash' },
          'Hello, ColdMush!',
        ]);
      }, 15_000);
    });

    describe('export', () => {
      it('should order proxy activity before condition in execution timeline', async () => {
        const execution = await handle.exportExecution();

        const firstActivity = execution.events.findIndex(
          e => e.event_type === 'activity_task_scheduled',
        );
        const firstSignal = execution.events.findIndex(
          e => e.event_type === 'workflow_execution_signaled',
        );

        expect(firstActivity).toBeLessThan(firstSignal);
      }, 10_000);

      it('signal_wait_started should have a later timestamp than workflow start', async () => {
        const execution = await handle.exportExecution();

        const workflowStart = execution.events.find(
          e => e.event_type === 'workflow_execution_started',
        );
        const waitStarted = execution.events.find(
          e => e.event_type === 'signal_wait_started',
        );

        expect(workflowStart).toBeDefined();
        expect(waitStarted).toBeDefined();

        // signal_wait_started must have a DIFFERENT (later) timestamp than workflow start
        // This proves ac comes from the waiter hook Leg1, not $job.metadata.jc
        const startTime = new Date(workflowStart.event_time).getTime();
        const waitTime = new Date(waitStarted.event_time).getTime();

        expect(waitTime).toBeGreaterThan(startTime);
      }, 10_000);
    });
  });
});
