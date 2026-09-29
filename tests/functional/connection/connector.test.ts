import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import path from 'path';

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { Client, Pool } from 'pg';

import { postgres_options } from '../../$setup/postgres';
import {
  countBackends,
  createCrashGuard,
  endOutage,
  terminateBackends,
  waitFor,
} from '../../$setup/postgres/chaos';
import { guid, sleepFor } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ConnectorService } from '../../../services/connector/factory';

/**
 * The connector wiring: every connection HotMesh creates from a
 * `pg.Client` class is resilient, shared connections survive for every
 * service bound to them, pools never crash the process, and a process
 * that holds HotMesh connections outlives a database restart (the
 * failure a bare pg.Client turns into an exit).
 */
describe('FUNCTIONAL | Postgres connector resilience', () => {
  const guard = createCrashGuard();

  beforeAll(() => {
    guard.install();
  });

  afterEach(() => {
    expect(guard.crashes).toEqual([]);
  });

  afterAll(async () => {
    await endOutage();
    await PostgresConnection.disconnectAll();
    guard.uninstall();
  });

  describe('process survival (the restart that ends a bare pg.Client)', () => {
    const probe = path.join(__dirname, 'src', 'crash-probe.ts');

    const launch = (
      mode: 'plain' | 'hotmesh',
      applicationName: string,
    ): { child: ChildProcessWithoutNullStreams; output: () => string; exit: Promise<number | null> } => {
      const child = spawn('npx', ['ts-node', '--transpile-only', probe, mode, applicationName], {
        cwd: path.join(__dirname, '..', '..', '..'),
        env: { ...process.env, NODE_ENV: 'test' },
      });
      let buffer = '';
      child.stdout.on('data', (chunk) => (buffer += chunk.toString()));
      child.stderr.on('data', (chunk) => (buffer += chunk.toString()));
      const exit = new Promise<number | null>((resolve) => child.on('exit', (code) => resolve(code)));
      return { child, output: () => buffer, exit };
    };

    it('a bare pg.Client exits the process on a terminated backend (the incident)', async () => {
      const app = `probe-plain-${guid()}`;
      const { output, exit } = launch('plain', app);
      await waitFor(() => output().includes('ready'), 30_000, 'plain probe ready');
      expect(await terminateBackends(app)).toBe(1);
      const code = await Promise.race([exit, sleepFor(10_000).then(() => 'alive')]);
      //the idle client's 'error' (57P01) has no listener: Node ends the process
      expect(code).toBe(1);
      expect(output()).toContain('terminating connection due to administrator command');
    }, 60_000);

    it('a HotMesh connection keeps the process alive and serves the next query', async () => {
      const app = `probe-hotmesh-${guid()}`;
      const { child, output, exit } = launch('hotmesh', app);
      try {
        await waitFor(() => output().includes('ready'), 30_000, 'hotmesh probe ready');
        expect(await terminateBackends(app)).toBe(1);
        const code = await Promise.race([exit, sleepFor(3_000).then(() => 'alive')]);
        expect(code).toBe('alive');
        await waitFor(async () => (await countBackends(app)) === 1, 10_000, 'reconnected backend');
        child.stdin.write('query\n');
        await waitFor(() => output().includes('result:1'), 10_000, 'query after restart');
        expect(output()).not.toContain('boot-error');
      } finally {
        child.kill('SIGKILL');
      }
    }, 60_000);
  });

  describe('PostgresConnection', () => {
    it('creates a resilient client with connection defaults (caller values win)', async () => {
      const connection = await PostgresConnection.connect(guid(), Client as any, {
        ...postgres_options,
        application_name: `conn-defaults-${guid()}`,
      } as any);
      const client: any = connection.getClient();
      expect(typeof client.connectionGeneration).toBe('number');
      const { rows } = await client.query(
        `SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()`,
      );
      expect(rows[0].application_name.startsWith('conn-defaults-')).toBe(true);

      const defaulted: any = (
        await PostgresConnection.connect(guid(), Client as any, { ...postgres_options } as any)
      ).getClient();
      const named = await defaulted.query(
        `SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()`,
      );
      expect(named.rows[0].application_name).toBe('hotmesh');
    });

    it('keeps a task-queue-shared connection serving every consumer across a restart', async () => {
      const app = `conn-shared-${guid()}`;
      const options = { ...postgres_options, application_name: app } as any;
      const taskQueue = `tq-${guid()}`;
      const store = await PostgresConnection.getOrCreateTaskQueueConnection(
        guid(), taskQueue, Client as any, options, { provider: 'postgres' },
      );
      const stream = await PostgresConnection.getOrCreateTaskQueueConnection(
        guid(), taskQueue, Client as any, options, { provider: 'postgres' },
      );
      const storeClient: any = store.getClient();
      const streamClient: any = stream.getClient();
      expect(storeClient).toBe(streamClient);

      await terminateBackends(app);
      await waitFor(() => storeClient.isConnected && storeClient.connectionGeneration === 1, 5_000, 'reconnect');
      expect((await storeClient.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
      expect((await streamClient.query('SELECT 2 AS ok')).rows[0].ok).toBe(2);
    });

    it('guards a caller-supplied Pool: an idle pooled client lost to a restart never crashes', async () => {
      const app = `conn-pool-${guid()}`;
      const pool = new Pool({ ...postgres_options, application_name: app } as any);
      try {
        const target: any = {};
        await ConnectorService.initClient(
          { class: pool as any, options: {} as any, provider: 'postgres.poolclient' },
          target,
          'store',
          `tq-pool-${guid()}`,
        );
        expect(pool.listenerCount('error')).toBeGreaterThan(0);
        await pool.query('SELECT 1');
        await terminateBackends(app);
        await sleepFor(500);
        expect((await pool.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
      } finally {
        await pool.end();
      }
    });

    it('leaves a Pool that already has an error listener as the caller configured it', async () => {
      const pool = new Pool({ ...postgres_options } as any);
      const own = () => undefined;
      pool.on('error', own);
      try {
        await ConnectorService.initClient(
          { class: pool as any, options: {} as any, provider: 'postgres.poolclient' },
          {} as any,
          'store',
          `tq-pool-own-${guid()}`,
        );
        expect(pool.listeners('error')).toEqual([own]);
      } finally {
        await pool.end();
      }
    });

    it('disconnectAll ends every connection, including one lost to a restart, without hanging', async () => {
      const app = `conn-disconnect-${guid()}`;
      for (let i = 0; i < 3; i++) {
        await PostgresConnection.connect(guid(), Client as any, {
          ...postgres_options,
          application_name: app,
        } as any);
      }
      expect(await countBackends(app)).toBe(3);
      await terminateBackends(app);
      const started = Date.now();
      await PostgresConnection.disconnectAll();
      expect(Date.now() - started).toBeLessThan(6_000);
      await waitFor(async () => (await countBackends(app)) === 0, 5_000, 'no backends');
      //nothing reconnects after the end
      await sleepFor(800);
      expect(await countBackends(app)).toBe(0);
    });
  });
});
