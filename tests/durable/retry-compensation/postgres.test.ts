import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderConfig } from '../../../types/provider';
import {
  HMSH_CODE_DURABLE_FATAL,
  HMSH_CODE_DURABLE_MAXED,
} from '../../../modules/enums';

import * as workflows from './src/workflows';
import { state } from './src/state';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | retry-compensation | Postgres', () => {
  let handle: WorkflowHandleService;
  const orderId = 'order-abc';
  const taskQueue = 'retry-compensation-world';
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

  describe('Client', () => {
    describe('start', () => {
      it('should start a workflow whose activity always fails', async () => {
        const client = new Client({ connection });
        //NOTE: `handle` is a global variable.
        handle = await client.workflow.start({
          args: [{ orderId }],
          taskQueue,
          workflowName: 'example',
          workflowId: guid(),
          expire: 120,
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
          taskQueue,
          workflow: workflows.default.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('catches the retry-exhausted activity error and returns successfully', async () => {
        // The workflow completes normally (result resolves, never rejects),
        // carrying the caught error details in its return envelope.
        const result = (await handle.result()) as Record<string, any>;

        // Parent workflow returned successfully after handling the failure.
        expect(result.ok).toBe(false);
        expect(result.orderId).toBe(orderId);

        // The terminal retry-exhausted error was communicated back. A plain
        // retryable failure that runs out of attempts surfaces as a fatal
        // terminal code (598); maxed (597) is accepted for robustness.
        expect(result.error).toBeDefined();
        expect([HMSH_CODE_DURABLE_FATAL, HMSH_CODE_DURABLE_MAXED]).toContain(
          result.error.code,
        );
        expect(typeof result.error.message).toBe('string');
        expect(result.error.message.length).toBeGreaterThan(0);

        // The static retry policy drove more than the initial attempt before
        // the framework gave up (maximumAttempts: 2 -> initial + 2 retries).
        expect(result.attempts).toBeGreaterThanOrEqual(2);
        expect(state.attempts).toBeGreaterThanOrEqual(2);
      }, 30_000);
    });
  });
});
