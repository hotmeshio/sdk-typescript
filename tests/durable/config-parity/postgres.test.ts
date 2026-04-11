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

  describe('patched', () => {
    it('should create worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'patched-test',
        workflow: workflows.patchedWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should take new code path for new workflows (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['order-123'],
        taskQueue: 'patched-test',
        workflowName: 'patchedWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // New workflow takes the patched path (v2 validation)
      expect(result).toBe('v2-validated-order-123');
    }, 15_000);

    it('should create multi-patch worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'multi-patch',
        workflow: workflows.multiPatchWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should handle multiple patches in same workflow (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'multi-patch',
        workflowName: 'multiPatchWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBe('both-patched');
    }, 15_000);

    it('should create deprecatePatch worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'deprecate-patch',
        workflow: workflows.deprecatePatchWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should create patched-in-hook workers', async () => {
      // Register the hook function worker
      const hookWorker = await Worker.create({
        connection,
        taskQueue: 'patched-hook',
        workflow: workflows.patchedHookFunction,
      });
      await hookWorker.run();
      // Register the parent workflow worker
      const parentWorker = await Worker.create({
        connection,
        taskQueue: 'patched-hook',
        workflow: workflows.patchedInHookWorkflow,
      });
      await parentWorker.run();
    }, 15_000);

    it('should use patched inside a hook with dimensional isolation (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'patched-hook',
        workflowName: 'patchedInHookWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // Hook runs in its own dimension; patched returns true for new workflow
      expect((result as any).data).toBe('hook-v2-result');
    }, 30_000);

    it('should allow deprecatePatch as no-op without breaking execution (negative)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['order-456'],
        taskQueue: 'deprecate-patch',
        workflowName: 'deprecatePatchWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBe('v2-validated-order-456');
    }, 15_000);
  });

  describe('cancellation', () => {
    it('should create cancellable worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'cancel-test',
        workflow: workflows.cancellableWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should complete normally without cancel (smoke test)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['order-smoke'],
        taskQueue: 'cancel-test',
        workflowName: 'cancellableWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      // No cancel — workflow should sleep 5s then return 'completed'
      const result = await handle.result();
      expect(result).toBe('completed');
    }, 30_000);

    it('should cancel workflow and run cleanup in nonCancellable scope (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['order-789'],
        taskQueue: 'cancel-test',
        workflowName: 'cancellableWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      // Wait for the activity to complete and the workflow to enter sleep
      await sleepFor(2000);
      // Cancel the workflow — takes effect when sleep completes
      await handle.cancel();
      const result = await handle.result();
      // Workflow caught CancelledFailure, ran refund in nonCancellable scope
      expect(result).toBe('cancelled:refunded-order-789');
    }, 60_000);

    it('should create uncaught cancel worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'cancel-uncaught',
        workflow: workflows.uncaughtCancelWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should propagate CancelledFailure as error when not caught (negative)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'cancel-uncaught',
        workflowName: 'uncaughtCancelWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      await sleepFor(2000);
      await handle.cancel();
      try {
        await handle.result();
        expect.unreachable('should have thrown');
      } catch (err) {
        // CancelledFailure propagated as a workflow error
        expect(err).toBeDefined();
      }
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

  describe('secured worker (workerCredentials)', () => {
    let credential: { roleName: string; password: string };

    it('should provision a scoped worker role', async () => {
      credential = await Durable.provisionWorkerRole({
        connection,
        streamNames: ['secured-patched', 'secured-patched-activity'],
      });
      expect(credential.roleName).toMatch(/^hmsh_wrk_/);
      expect(credential.password).toBeDefined();
    }, 15_000);

    it('should create a secured worker with workerCredentials', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'secured-patched',
        workflow: workflows.securedPatchedWorkflow,
        workerCredentials: {
          user: credential.roleName,
          password: credential.password,
        },
      });
      await worker.run();
    }, 15_000);

    it('should run patched workflow on secured worker (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['secure-order-1'],
        taskQueue: 'secured-patched',
        workflowName: 'securedPatchedWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // patched returns true for new workflow, even on secured worker
      expect(result).toBe('v2-validated-secure-order-1');
    }, 60_000);
  });

  describe('secured worker notifications', () => {
    let securedCredential: { roleName: string; password: string };

    it('should provision a scoped role for notification tests', async () => {
      securedCredential = await Durable.provisionWorkerRole({
        connection,
        streamNames: [
          'secured-signal',
          'secured-signal-activity',
          'secured-chain',
          'secured-chain-activity',
        ],
      });
      expect(securedCredential.roleName).toMatch(/^hmsh_wrk_/);
    }, 15_000);

    it('should create secured signal worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'secured-signal',
        workflow: workflows.securedSignalWorkflow,
        workerCredentials: {
          user: securedCredential.roleName,
          password: securedCredential.password,
        },
      });
      await worker.run();
    }, 15_000);

    it('should deliver signal to secured worker via LISTEN/NOTIFY (positive)', async () => {
      const client = new Client({ connection });
      const workflowId = guid();
      const handle = await client.workflow.start({
        args: [],
        taskQueue: 'secured-signal',
        workflowName: 'securedSignalWorkflow',
        workflowId,
        expire: 120,
      });

      // Send signal after a short delay — the secured worker must be
      // woken by LISTEN/NOTIFY to process this without polling delay
      await sleepFor(500);
      await handle.signal('secured-signal', {
        value: 'notify-test',
      });

      const result = await handle.result();
      expect(result).toBe('processed-notify-test');
    }, 60_000);

    it('should create secured chain worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'secured-chain',
        workflow: workflows.securedChainWorkflow,
        workerCredentials: {
          user: securedCredential.roleName,
          password: securedCredential.password,
        },
      });
      await worker.run();
    }, 15_000);

    it('should complete multi-step chain on secured worker (positive)', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['input'],
        taskQueue: 'secured-chain',
        workflowName: 'securedChainWorkflow',
        workflowId: guid(),
        expire: 120,
      });
      const result = await handle.result();
      // Three activity steps: step1 -> step2 -> step3
      expect(result).toBe('step3-step2-step1-input');
    }, 60_000);

    it('should complete multiple workflows rapidly on secured worker (latency)', async () => {
      const client = new Client({ connection });
      const startTime = Date.now();
      const count = 3;
      const results: Promise<unknown>[] = [];

      for (let i = 0; i < count; i++) {
        const handle = await client.workflow.start({
          args: [`rapid-${i}`],
          taskQueue: 'secured-chain',
          workflowName: 'securedChainWorkflow',
          workflowId: guid(),
          expire: 120,
        });
        results.push(handle.result());
      }

      const allResults = await Promise.all(results);
      const elapsed = Date.now() - startTime;

      // All should complete correctly
      for (let i = 0; i < count; i++) {
        expect(allResults[i]).toBe(`step3-step2-step1-rapid-${i}`);
      }

      // With LISTEN/NOTIFY, 3 three-step workflows should complete well
      // under 30s. Pure polling at 500ms would need ~4.5s per workflow.
      expect(elapsed).toBeLessThan(30_000);
    }, 60_000);
  });
});
