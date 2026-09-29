import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';

//a short reservation window keeps the redelivery scenarios fast; read at module load
vi.hoisted(() => {
  process.env.HMSH_RESERVATION_TIMEOUT_S = '5';
});

import { Client as Postgres } from 'pg';

import { dropTables, postgres_options } from '../../$setup/postgres';
import {
  beginOutage,
  createCrashGuard,
  endOutage,
  terminateBackends,
  waitFor,
} from '../../$setup/postgres/chaos';
import { guid, sleepFor } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ConnectionHealth } from '../../../services/connector/health';
import { Durable } from '../../../services/durable';
import { ClientService } from '../../../services/durable/client';
import { ProviderNativeClient } from '../../../types/provider';

import * as workflows from './src/workflows';
import { executions } from './src/activities';

const { Connection, Client, Worker } = Durable;

/**
 * Durable workflows across database restarts and outages: the process
 * never exits, workflows started after a restart run, activities in
 * flight when the session dies complete, a result() awaited through an
 * outage resolves, and the retry budget is not spent on the outage.
 */
describe('DURABLE | resilience | Postgres', () => {
  const guard = createCrashGuard();
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'resilience';
  let client: ClientService;
  let postgresClient: ProviderNativeClient;

  //a terminated session reports its loss within milliseconds; let it land first
  const allUp = async () => {
    await sleepFor(150);
    await waitFor(() => ConnectionHealth.snapshot().state === 'up', 15_000, 'all connections up');
  };

  //start() fails fast while a connection is reconnecting; callers wait or park
  const start = async (workflowName: string, args: unknown[]) => {
    await allUp();
    return client.workflow.start({
      args,
      taskQueue,
      workflowName,
      workflowId: `${workflowName}-${guid()}`,
      expire: 600,
    });
  };

  beforeAll(async () => {
    guard.install();
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    await Connection.connect(connection);
    client = new Client({ connection });
    for (const workflow of [workflows.resilientEcho, workflows.resilientPair]) {
      const worker = await Worker.create({ connection, taskQueue, workflow });
      await worker.run();
    }
  }, 60_000);

  afterAll(async () => {
    await endOutage();
    await sleepFor(1_000);
    await Durable.shutdown();
    guard.uninstall();
    delete process.env.HMSH_RESERVATION_TIMEOUT_S;
  }, 30_000);

  it('runs a workflow before any fault (baseline)', async () => {
    const handle = await start('resilientEcho', ['baseline', 50]);
    expect(await handle.result()).toBe('echo:baseline');
    expect(guard.crashes).toEqual([]);
  });

  it('keeps the process alive through a restart and runs workflows started after it', async () => {
    const terminated = await terminateBackends();
    expect(terminated).toBeGreaterThan(0);
    await allUp();

    const handle = await start('resilientEcho', ['after-restart', 50]);
    expect(await handle.result()).toBe('echo:after-restart');
    expect(guard.crashes).toEqual([]);
  }, 60_000);

  it('completes a workflow whose activity is running when the session dies', async () => {
    const name = `inflight-${guid()}`;
    const handle = await start('resilientEcho', [name, 2_500]);
    await waitFor(() => (executions[name] ?? 0) >= 1, 10_000, 'activity started');
    await terminateBackends();
    expect(await handle.result()).toBe(`echo:${name}`);
    expect(guard.crashes).toEqual([]);
  }, 90_000);

  it('resolves a result() awaited through an outage that spans the activity completion', async () => {
    const name = `outage-${guid()}`;
    const handle = await start('resilientEcho', [name, 1_500]);
    const result = handle.result();
    await waitFor(() => (executions[name] ?? 0) >= 1, 10_000, 'activity started');

    //the activity finishes while the database refuses every connection:
    //its response cannot be written, so the message must survive for redelivery
    await beginOutage();
    await sleepFor(4_000);
    expect(ConnectionHealth.snapshot().state).toBe('down');
    await endOutage();
    const restoredAt = Date.now();

    expect(await result).toBe(`echo:${name}`);
    //the deferred message is released when the connection returns, not
    //after the reservation window or the fallback poller
    expect(Date.now() - restoredAt).toBeLessThan(15_000);
    //the outage consumed no retry budget: at most one redelivery beyond the first run
    expect(executions[name]).toBeLessThanOrEqual(2);
    expect(guard.crashes).toEqual([]);
  }, 120_000);

  it('continues a multi-step workflow across a restart between its steps', async () => {
    const name = `pair-${guid()}`;
    const handle = await start('resilientPair', [name, 800]);
    await waitFor(() => (executions[`${name}-a`] ?? 0) >= 1, 10_000, 'first step started');
    await sleepFor(900);
    await terminateBackends();
    expect(await handle.result()).toEqual([`echo:${name}-a`, `echo:${name}-b`]);
    expect(guard.crashes).toEqual([]);
  }, 90_000);

  it('survives several restarts under load', async () => {
    const names = Array.from({ length: 6 }, (_, i) => `load-${i}-${guid()}`);
    const handles = await Promise.all(names.map((name) => start('resilientEcho', [name, 700])));
    const results = Promise.all(handles.map((handle) => handle.result()));
    for (let i = 0; i < 3; i++) {
      await sleepFor(400);
      await terminateBackends();
    }
    expect(await results).toEqual(names.map((name) => `echo:${name}`));
    expect(guard.crashes).toEqual([]);
  }, 120_000);

  it('initializes again in the same process after Durable.shutdown()', async () => {
    await Durable.shutdown();

    await Connection.connect(connection);
    const freshClient = new Client({ connection });
    const worker = await Worker.create({ connection, taskQueue, workflow: workflows.resilientEcho });
    await worker.run();

    const handle = await freshClient.workflow.start({
      args: ['reinit', 50],
      taskQueue,
      workflowName: 'resilientEcho',
      workflowId: `reinit-${guid()}`,
      expire: 600,
    });
    expect(await handle.result()).toBe('echo:reinit');
    expect(guard.crashes).toEqual([]);
  }, 60_000);
});
