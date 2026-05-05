/**
 * DURABLE | Contention | Postgres
 *
 * Reproduces the collation poison loop that occurs under parallel
 * Postgres batch processing at scale.
 *
 * Each stepIterator workflow spawns 2 child workstation workflows.
 * Each workstation runs an activity then signals its parent.
 * At high concurrency (1000 workflows → 2000 children → 4000+
 * activity completions), the Postgres batch consumer processes
 * multiple messages for the same workflow in parallel. Without the
 * fix, verifySyntheticInteger sees stale GUID ledger state from
 * uncommitted sibling transactions, throws CollationError INACTIVE,
 * and the message is silently acked — stalling the workflow forever.
 *
 * With the fix: INACTIVE is rethrown into the retry path with
 * backoff. On retry, siblings have committed and the read is clean.
 * HMSH_BATCH_SIZE can be tuned as an additional safety lever.
 *
 * Scale levels:
 *   10 — smoke test (always works)
 *   100 — moderate contention
 *   1000 — triggers the poison loop without the fix
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { WorkerService } from '../../../services/durable/worker';
import { HotMesh } from '../../../services/hotmesh';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { QuorumProfile } from '../../../types/quorum';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

const TASK_QUEUE = 'contention-test';
const STATIONS = [
  { stationName: 'grinder', role: 'grinder' },
  { stationName: 'gluer', role: 'gluer' },
];

// Scale: configurable via env or defaults
const TARGET_SMOKE = 1;
const TARGET_SMALL = 10;
const TARGET_LARGE = 1000;

describe('DURABLE | Contention | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    // Clean slate
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
    await Connection.connect({ class: Postgres, options: postgres_options });

    // Create workers for both workflow types
    for (const wf of [workflows.stepIterator, workflows.workstation]) {
      const worker = await Worker.create({
        connection,
        taskQueue: TASK_QUEUE,
        workflow: wf,
      });
      await worker.run();
    }
  }, 60_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  /**
   * Smoke test: 1 workflow to validate wiring.
   */
  it.skip(`smoke: ${TARGET_SMOKE} assembly-line workflow`, async () => {
    const client = new Client({ connection });
    const handles: WorkflowHandleService[] = [];

    const startPromises = Array.from({ length: TARGET_SMOKE }, (_, i) => {
      return client.workflow.start({
        args: [`Widget-${i}`, STATIONS],
        taskQueue: TASK_QUEUE,
        workflowName: 'stepIterator',
        workflowId: `iter-sm-${guid()}`,
        expire: 120,
      });
    });

    const started = await Promise.all(startPromises);
    handles.push(...started as WorkflowHandleService[]);

    // Wait for all to complete
    const results = await Promise.all(
      handles.map((h) => h.result()),
    );

    const completed = results.filter(
      (r) => r && typeof r === 'object' && 'results' in r,
    );

    expect(completed.length).toBe(TARGET_SMOKE);
    for (const r of completed) {
      expect((r as any).results).toHaveLength(STATIONS.length);
    }
  }, 60_000);

  /**
   * Query the quorum for work distribution across the mesh.
   * Returns profiles and logs a summary table.
   */
  async function logMeshDistribution(label: string): Promise<QuorumProfile[]> {
    // Get any HotMesh instance from the worker registry
    const firstEntry = WorkerService.instances.values().next().value;
    const hotmesh: HotMesh = firstEntry instanceof Promise
      ? await firstEntry
      : firstEntry;
    const profiles = await hotmesh.rollCall(1_000);

    console.log(`\n=== MESH WORK DISTRIBUTION (${label}) ===`);
    let totalMessages = 0;
    for (const p of profiles) {
      const role = p.worker_topic ? `Worker: ${p.worker_topic}` : 'Engine';
      const countSum = p.counts
        ? Object.values(p.counts).reduce((a, b) => a + b, 0)
        : 0;
      totalMessages += countSum;
      console.log(
        `  ${role.padEnd(40)} messages=${countSum.toString().padStart(6)}  ` +
        `depth=${(p.stream_depth ?? 0).toString().padStart(4)}  ` +
        `errors=${p.error_count ?? 0}  ` +
        `counts=${JSON.stringify(p.counts || {})}`,
      );
    }
    console.log(`  ${'TOTAL'.padEnd(40)} messages=${totalMessages.toString().padStart(6)}`);
    console.log('');

    return profiles;
  }

  it(`scale-10: ${TARGET_SMALL} concurrent assembly-line workflows`, async () => {
    const client = new Client({ connection });
    const startPromises = Array.from({ length: TARGET_SMALL }, (_, i) => {
      return client.workflow.start({
        args: [`Widget-10-${i}`, STATIONS],
        taskQueue: TASK_QUEUE,
        workflowName: 'stepIterator',
        workflowId: `iter-10-${guid()}`,
        expire: 120,
      });
    });
    const handles = await Promise.all(startPromises) as WorkflowHandleService[];
    const results = await Promise.all(handles.map((h) => h.result()));
    const completed = results.filter(
      (r) => r && typeof r === 'object' && 'results' in r,
    );
    expect(completed.length).toBe(TARGET_SMALL);

    // Query the mesh for work distribution
    const profiles = await logMeshDistribution('scale-10');
    // All instances should be healthy
    for (const p of profiles) {
      expect(p.error_count).toBe(0);
    }
  }, 60_000);

  it.skip('scale-50: 50 concurrent assembly-line workflows', async () => {
    const client = new Client({ connection });
    const startPromises = Array.from({ length: 50 }, (_, i) => {
      return client.workflow.start({
        args: [`Widget-50-${i}`, STATIONS],
        taskQueue: TASK_QUEUE,
        workflowName: 'stepIterator',
        workflowId: `iter-50-${guid()}`,
        expire: 120,
      });
    });
    const handles = await Promise.all(startPromises) as WorkflowHandleService[];
    const results = await Promise.all(handles.map((h) => h.result()));
    const completed = results.filter(
      (r) => r && typeof r === 'object' && 'results' in r,
    );
    expect(completed.length).toBe(50);
  }, 60_000);

  it('scale-100-analysis: 100 concurrent with distribution analysis', async () => {
    const client = new Client({ connection });
    const startPromises = Array.from({ length: 100 }, (_, i) => {
      return client.workflow.start({
        args: [`Widget-100-${i}`, STATIONS],
        taskQueue: TASK_QUEUE,
        workflowName: 'stepIterator',
        workflowId: `iter-100-${guid()}`,
        expire: 180,
      });
    });
    const handles = await Promise.all(startPromises) as WorkflowHandleService[];
    const results = await Promise.all(handles.map((h) => h.result()));
    const completed = results.filter(
      (r) => r && typeof r === 'object' && 'results' in r,
    );
    expect(completed.length).toBe(100);

    const profiles = await logMeshDistribution('scale-100');
    for (const p of profiles) {
      expect(p.error_count).toBe(0);
    }
    // Verify engines all participated
    const engines = profiles.filter((p) => !p.worker_topic);
    const activEngines = engines.filter(
      (p) => p.counts && Object.values(p.counts).reduce((a, b) => a + b, 0) > 0,
    );
    console.log(`  Active engines: ${activEngines.length}/${engines.length}`);
  }, 120_000);

  it.skip('scale-200: 200 concurrent assembly-line workflows', async () => {
    const client = new Client({ connection });
    const startPromises = Array.from({ length: 200 }, (_, i) => {
      return client.workflow.start({
        args: [`Widget-200-${i}`, STATIONS],
        taskQueue: TASK_QUEUE,
        workflowName: 'stepIterator',
        workflowId: `iter-200-${guid()}`,
        expire: 180,
      });
    });
    const handles = await Promise.all(startPromises) as WorkflowHandleService[];
    const results = await Promise.all(handles.map((h) => h.result()));
    const completed = results.filter(
      (r) => r && typeof r === 'object' && 'results' in r,
    );
    expect(completed.length).toBe(200);
  }, 120_000);

  const runHighScale = process.env.CONTENTION_HIGH_SCALE === '1';
  it.skipIf(!runHighScale)('scale-500: 500 concurrent assembly-line workflows', async () => {
    const client = new Client({ connection });
    const BATCH = 50;
    const handles: WorkflowHandleService[] = [];
    for (let offset = 0; offset < 500; offset += BATCH) {
      const batch = Array.from({ length: Math.min(BATCH, 500 - offset) }, (_, i) => {
        return client.workflow.start({
          args: [`Widget-500-${offset + i}`, STATIONS],
          taskQueue: TASK_QUEUE,
          workflowName: 'stepIterator',
          workflowId: `iter-500-${guid()}`,
          expire: 300,
        });
      });
      const started = await Promise.all(batch);
      handles.push(...started as WorkflowHandleService[]);
    }
    const results = await Promise.all(handles.map((h) => h.result()));
    const completed = results.filter(
      (r) => r && typeof r === 'object' && 'results' in r,
    );
    expect(completed.length).toBe(500);

    const profiles = await logMeshDistribution('scale-500');
    const engines = profiles.filter((p) => !p.worker_topic);
    const activeEngines = engines.filter(
      (p) => p.counts && Object.values(p.counts).reduce((a, b) => a + b, 0) > 0,
    );
    console.log(`  Active engines: ${activeEngines.length}/${engines.length}`);
  }, 300_000);

  /**
   * Scale test: 1000 concurrent assembly-line workflows.
   *
   * This is the test that triggers the collation poison loop.
   * 1000 parents × 2 children = 2000 child workflows, each running
   * 2 activities (processStation + signalParent) = 4000+ activity
   * completions flooding through the Postgres batch consumer.
   *
   * Without the fix: workflows stall as INACTIVE collation errors
   * silently ack messages. The test times out.
   *
   * With the fix: INACTIVE errors retry with backoff, siblings
   * commit, and all workflows complete.
   */
  // Gate behind env var — takes ~4 minutes with ~9000 concurrent in-flight jobs.
  // The adaptive reservation timeout (adjustReservationTimeout) auto-corrects
  // under load so no manual HMSH_RESERVATION_TIMEOUT_S override is needed.
  // Run explicitly: CONTENTION_1K=1 npx vitest run ... -t "scale-1000"
  const run1k = process.env.CONTENTION_1K === '1';
  it.skipIf(!run1k)(`scale-1000: ${TARGET_LARGE} concurrent assembly-line workflows (collation contention)`, async () => {
    const client = new Client({ connection });
    const BATCH_SIZE = 50;
    const handles: WorkflowHandleService[] = [];
    const t0 = Date.now();

    // Enqueue in batches to avoid overwhelming connection pool
    for (let offset = 0; offset < TARGET_LARGE; offset += BATCH_SIZE) {
      const batchCount = Math.min(BATCH_SIZE, TARGET_LARGE - offset);
      const batch = Array.from({ length: batchCount }, (_, i) => {
        return client.workflow.start({
          args: [`Widget-${offset + i}`, STATIONS],
          taskQueue: TASK_QUEUE,
          workflowName: 'stepIterator',
          workflowId: `iter-lg-${guid()}`,
          expire: 600,
        });
      });
      const started = await Promise.all(batch);
      handles.push(...started as WorkflowHandleService[]);
      console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] Enqueued: ${handles.length}/${TARGET_LARGE}`);
    }

    expect(handles.length).toBe(TARGET_LARGE);
    console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] All ${TARGET_LARGE} enqueued. Waiting for results...`);

    // Track completion progress
    let doneCount = 0;
    const results = await Promise.allSettled(
      handles.map((h) =>
        h.result().then((r) => {
          doneCount++;
          if (doneCount % 100 === 0 || doneCount === TARGET_LARGE) {
            console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] Completed: ${doneCount}/${TARGET_LARGE}`);
          }
          return r;
        }),
      ),
    );

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');

    if (rejected.length > 0) {
      console.log(
        `CONTENTION: ${rejected.length}/${TARGET_LARGE} workflows failed`,
      );
      for (const r of rejected.slice(0, 5)) {
        console.log(`  reason: ${(r as PromiseRejectedResult).reason}`);
      }
    }

    const completed = fulfilled.filter((r) => {
      const val = (r as PromiseFulfilledResult<any>).value;
      return val && typeof val === 'object' && 'results' in val;
    });

    // ALL workflows must complete — zero stalls, zero poison loops
    expect(completed.length).toBe(TARGET_LARGE);
  }, 900_000); // 15-minute timeout — 1000 workflows need headroom
});
