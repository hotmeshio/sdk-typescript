import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { resetCallCount, getCallCount } from './src/activities';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

describe('DURABLE | Config Parity | Postgres', () => {
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

  describe('initialInterval', () => {
    beforeEach(() => {
      resetCallCount();
    });

    it('should create worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'initial-interval',
        workflow: workflows.initialIntervalWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should retry with initialInterval and succeed on third attempt', async () => {
      const client = new Client({ connection });
      const startTime = Date.now();
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'initial-interval',
        workflowName: 'initialIntervalWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      const elapsed = Date.now() - startTime;

      expect(result).toBe('success');
      expect(getCallCount()).toBe(3);
      // With initialInterval=5s, backoff=2: retry1=5s, retry2=10s
      // Total should be > 10s (2 retries with backoff)
      expect(elapsed).toBeGreaterThan(8_000);
    }, 60_000);
  });

  describe('continueAsNew', () => {
    it('should create worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'continue-as-new',
        workflow: workflows.continueAsNewWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should restart workflow with new args and accumulate results across generations (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [1, 0],
        taskQueue: 'continue-as-new',
        workflowName: 'continueAsNewWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // 3 batches of 10 items each = 30 total
      expect(result).toBe(30);
    }, 30_000);

    it('should create terminal worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'continue-terminal',
        workflow: workflows.continueAsNewTerminalWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should prove continueAsNew is terminal — code after it never runs (negative)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [0],
        taskQueue: 'continue-terminal',
        workflowName: 'continueAsNewTerminalWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // If continueAsNew weren't terminal, we'd get 'unreachable'
      // Instead, workflow restarts with iteration=1 and returns 'completed-at-1'
      expect(result).toBe('completed-at-1');
    }, 30_000);
  });

  describe('startToCloseTimeout', () => {
    it('should create worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'start-to-close',
        workflow: workflows.startToCloseWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should succeed when activity completes within timeout', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'start-to-close',
        workflowName: 'startToCloseWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBe('fast');
    }, 15_000);

    it('should timeout when activity exceeds startToCloseTimeout', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'start-to-close-fail',
        workflow: workflows.startToCloseTimeoutWorkflow,
      });
      await worker.run();

      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'start-to-close-fail',
        workflowName: 'startToCloseTimeoutWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // Activity exceeded 5s timeout, so workflow gets error object via throwOnError:false
      expect(result).toBe('timeout_error');
    }, 30_000);
  });
});
