/**
 * Readonly connection integration tests.
 *
 * This test process is the "readonly dashboard". It:
 *   1. Registers workers with Worker.create using a readonly connection
 *      and NO-OP workflow functions (same names, same task queue as
 *      the real implementations on the worker container).
 *   2. Starts workflows via the readonly client.
 *   3. Asserts the REMOTE worker container processed every job — NOT
 *      the local no-ops.
 *
 * The proof:
 *   - The no-op workflows return 'NOOP_SHOULD_NEVER_APPEAR'.
 *   - The real workflows call proxy activities and return real data.
 *   - If the result contains real data, only the remote worker could
 *     have produced it.
 *   - The execution history contains `auditLog` activity calls that
 *     only exist on the remote worker's interceptor.
 *   - The local engine's router has readonly=true and shouldConsume=false.
 *
 * Run:
 *   docker compose --profile readonly up -d --build
 *   docker compose exec hotmesh-readonly \
 *     npx vitest run tests/durable/readonly/postgres.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { postgres_options } from '../../$setup/postgres';
import { guid, sleepFor } from '../../../modules/utils';
import { Durable } from '../../../services/durable';
import { ClientService } from '../../../services/durable/client';
import { WorkerService } from '../../../services/durable/worker';
import { HotMesh } from '../../../services/hotmesh';

// NO-OP workflows — same function names as the real ones on the worker
import * as noopWorkflows from './src/workflows.noop';

const { Client, Worker } = Durable;

const TASK_QUEUE = 'readonly-test';

const readonlyConnection = {
  class: Postgres,
  options: postgres_options,
  readonly: true,
};

// ─── Helpers ────────────────────────────────────────────────────

async function resolveInstances(
  map: Map<string, HotMesh | Promise<HotMesh>>,
): Promise<{ key: string; hotMesh: HotMesh }[]> {
  const results: { key: string; hotMesh: HotMesh }[] = [];
  for (const [key, value] of map) {
    results.push({ key, hotMesh: await value });
  }
  return results;
}

// ─── Tests ──────────────────────────────────────────────────────

describe('DURABLE | readonly | `Readonly Connection` | Postgres', () => {
  beforeAll(async () => {
    // Register workers on the READONLY connection with NO-OP
    // workflow functions. Same names, same task queue as the real
    // implementations on the worker container.
    for (const wf of [
      noopWorkflows.readonlyExample,
      noopWorkflows.robustWorkflow,
      noopWorkflows.metadataWorkflow,
    ]) {
      const worker = await Worker.create({
        connection: readonlyConnection,
        taskQueue: TASK_QUEUE,
        workflow: wf,
      });
      await worker.run();
    }

    // Give the remote worker container time to finish quorum
    await sleepFor(5_000);
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1_500);
    await Durable.shutdown();
  }, 10_000);

  // ─── Structural: local engine is readonly ─────────────────────

  describe('Local engine is readonly', () => {
    it('should have router.readonly=true on every local engine and worker', async () => {
      // Force creation of the readonly client engine
      const client = new Client({ connection: readonlyConnection });
      await client.workflow.start({
        args: ['StructuralCheck'],
        taskQueue: TASK_QUEUE,
        workflowName: 'readonlyExample',
        workflowId: 'readonly-structural-' + guid(),
        expire: 120,
      });

      // Check client instances
      const clientInstances = await resolveInstances(ClientService.instances);
      expect(clientInstances.length).toBeGreaterThanOrEqual(1);
      for (const { hotMesh } of clientInstances) {
        if (hotMesh.engine?.router) {
          expect(hotMesh.engine.router.readonly).toBe(true);
        }
      }

      // Check worker instances — Worker.create with readonly connection
      const workerInstances = await resolveInstances(WorkerService.instances);
      for (const { hotMesh } of workerInstances) {
        if (hotMesh.engine?.router) {
          expect(hotMesh.engine.router.readonly).toBe(true);
        }
      }
    }, 15_000);

    it('should have shouldConsume=false — never started consuming', async () => {
      const clientInstances = await resolveInstances(ClientService.instances);
      for (const { hotMesh } of clientInstances) {
        if (hotMesh.engine?.router) {
          expect(hotMesh.engine.router.shouldConsume).toBe(false);
        }
      }
      const workerInstances = await resolveInstances(WorkerService.instances);
      for (const { hotMesh } of workerInstances) {
        if (hotMesh.engine?.router) {
          expect(hotMesh.engine.router.shouldConsume).toBe(false);
        }
      }
    }, 15_000);
  });

  // ─── Functional: remote worker processes, not local no-ops ────

  describe('Remote worker processes jobs (not local no-ops)', () => {
    it('should complete a simple workflow with real proxy activity output (not NOOP)', async () => {
      const client = new Client({ connection: readonlyConnection });
      const handle = await client.workflow.start({
        args: ['ReadonlyUser'],
        taskQueue: TASK_QUEUE,
        workflowName: 'readonlyExample',
        workflowId: 'readonly-simple-' + guid(),
        expire: 120,
      });
      const result = await handle.result();

      // If the local no-op ran, this would be 'NOOP_SHOULD_NEVER_APPEAR'
      expect(result).toBe('Hello, ReadonlyUser!');
      expect(result).not.toContain('NOOP');
    }, 30_000);

    it('should complete a robust workflow with proxy activities + sleep (not NOOP)', async () => {
      const client = new Client({ connection: readonlyConnection });
      const handle = await client.workflow.start({
        args: ['ORD-001'],
        taskQueue: TASK_QUEUE,
        workflowName: 'robustWorkflow',
        workflowId: 'readonly-robust-' + guid(),
        expire: 120,
      });
      const result = (await handle.result()) as Record<string, any>;

      // Real workflow returns entity state with activity results
      expect(result.status).toBe('completed');
      expect(result.status).not.toBe('NOOP_SHOULD_NEVER_APPEAR');
      expect(result.steps).toEqual(['processed', 'slept', 'notified']);
      expect(result.processed).toBe('order-ORD-001-processed');
      expect(result.notification).toContain('notified:admin:order-ORD-001');
    }, 45_000);

    it('should have auditLog in execution history proving the remote interceptor ran', async () => {
      const client = new Client({ connection: readonlyConnection });
      const handle = await client.workflow.start({
        args: ['ORD-PROOF'],
        taskQueue: TASK_QUEUE,
        workflowName: 'robustWorkflow',
        workflowId: 'readonly-proof-' + guid(),
        expire: 120,
      });
      const result = (await handle.result()) as Record<string, any>;
      expect(result.status).toBe('completed');

      // Export the execution history
      const execution = await handle.exportExecution();
      const scheduledActivities = execution.events
        .filter((e) => e.event_type === 'activity_task_scheduled')
        .map((e) =>
          e.attributes.kind === 'activity_task_scheduled'
            ? e.attributes.activity_type
            : null,
        );

      // auditLog only exists on the remote worker's interceptor.
      // Its presence in the execution history is irrefutable proof
      // that the REMOTE container processed this job.
      expect(scheduledActivities).toContain('auditLog');
      expect(scheduledActivities).toContain('processOrder');
      expect(scheduledActivities).toContain('sendNotification');
    }, 45_000);

    it('should propagate headers through the remote worker (not NOOP)', async () => {
      const client = new Client({ connection: readonlyConnection });
      const handle = await client.workflow.start({
        args: ['HeaderTest'],
        taskQueue: TASK_QUEUE,
        workflowName: 'metadataWorkflow',
        workflowId: 'readonly-metadata-' + guid(),
        expire: 120,
      });
      const result = (await handle.result()) as Record<string, any>;

      expect(result.status).toBe('completed');
      expect(result.status).not.toBe('NOOP_SHOULD_NEVER_APPEAR');
      expect(result.activityResult.data).toBe('HeaderTest');
      expect(result.activityResult.headers).toEqual({
        source: 'readonly-dashboard',
        env: 'test',
      });
    }, 45_000);
  });

  // ─── Post-test: readonly invariants still hold ────────────────

  describe('Readonly invariants hold after all workflows', () => {
    it('should still have readonly=true and shouldConsume=false', async () => {
      const instances = await resolveInstances(ClientService.instances);
      expect(instances.length).toBeGreaterThanOrEqual(1);
      for (const { hotMesh } of instances) {
        if (hotMesh.engine?.router) {
          expect(hotMesh.engine.router.readonly).toBe(true);
          expect(hotMesh.engine.router.shouldConsume).toBe(false);
        }
      }
    }, 15_000);
  });
});
