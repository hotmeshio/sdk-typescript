import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

describe('DURABLE | condition-timeout | Postgres', () => {
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

  describe('Signal wins', () => {
    it('should create worker and return signal data', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'cond-sig',
        workflow: workflows.signalWins,
      });
      await worker.run();

      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'cond-sig',
        workflowName: 'signalWins',
        workflowId: guid(),
        expire: 120,
      });

      await sleepFor(3_000);
      await handle.signal('test-signal', { msg: 'hello' });

      const result = await handle.result();
      expect(result).toBe('hello');
    }, 30_000);
  });

  describe('Timeout wins', () => {
    it('should return timed_out when no signal arrives', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'cond-timeout',
        workflow: workflows.timeoutWins,
      });
      await worker.run();

      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'cond-timeout',
        workflowName: 'timeoutWins',
        workflowId: guid(),
        expire: 120,
      });

      const result = await handle.result();
      expect(result).toBe('timed_out');
    }, 30_000);
  });

  describe('No timeout (backward compat)', () => {
    it('should wait indefinitely and return signal data', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'cond-compat',
        workflow: workflows.noTimeout,
      });
      await worker.run();

      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'cond-compat',
        workflowName: 'noTimeout',
        workflowId: guid(),
        expire: 120,
      });

      await sleepFor(3_000);
      await handle.signal('compat-signal', { msg: 'works' });

      const result = await handle.result();
      expect(result).toBe('works');
    }, 30_000);
  });
});
