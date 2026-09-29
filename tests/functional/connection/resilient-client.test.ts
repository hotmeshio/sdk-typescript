import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { Client } from 'pg';

import { postgres_options } from '../../$setup/postgres';
import {
  beginOutage,
  createCrashGuard,
  endOutage,
  startPostgresProxy,
  terminateBackends,
  waitFor,
} from '../../$setup/postgres/chaos';
import { guid, identifyProvider, sleepFor } from '../../../modules/utils';
import {
  HotMeshConnectionError,
  isConnectionError,
} from '../../../modules/errors';
import { LoggerService } from '../../../services/logger';
import { ConnectionHealth } from '../../../services/connector/health';
import {
  ResilientPostgresClient,
  ResilientPostgresPolicy,
} from '../../../services/connector/providers/postgres-resilient-client';

/**
 * The resilient client against a real Postgres: backends terminated the
 * way an RDS/Aurora restart terminates them, a database that refuses
 * connections for a window, a transaction caught by a reconnect, and a
 * host that goes silent while the socket stays open.
 */
describe('FUNCTIONAL | ResilientPostgresClient', () => {
  const logger = new LoggerService('hotmesh', 'resilience-test');
  const guard = createCrashGuard();
  const clients: ResilientPostgresClient[] = [];
  const FAST: Partial<ResilientPostgresPolicy> = {
    reconnectBaseMs: 50,
    reconnectMaxMs: 400,
    heartbeatMs: 0,
    txLostWindowMs: 3_000,
  };

  /** Each test owns its application_name so a fault reaches only its clients. */
  const open = async (
    applicationName: string,
    policy: Partial<ResilientPostgresPolicy> = FAST,
    options: Record<string, unknown> = {},
  ): Promise<ResilientPostgresClient> => {
    const client = new ResilientPostgresClient(
      Client as any,
      { ...postgres_options, application_name: applicationName, ...options } as any,
      guid(),
      logger,
      policy,
    );
    await client.connect();
    clients.push(client);
    return client;
  };

  beforeAll(() => {
    guard.install();
  });

  afterEach(async () => {
    await endOutage();
    while (clients.length) {
      await clients.pop()!.end();
    }
    expect(guard.crashes).toEqual([]);
  });

  afterAll(async () => {
    await endOutage();
    guard.uninstall();
  });

  it('survives a terminated idle session and reconnects in place', async () => {
    const app = `rc-idle-${guid()}`;
    const client = await open(app);
    const events: string[] = [];
    const onLost = (e: { connectionId: string }) => {
      if (e.connectionId === client.id) events.push('lost');
    };
    const onRestored = (e: { connectionId: string }) => {
      if (e.connectionId === client.id) events.push('restored');
    };
    ConnectionHealth.on('lost', onLost);
    ConnectionHealth.on('restored', onRestored);
    try {
      const before = (await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      expect(await terminateBackends(app)).toBe(1);

      await waitFor(() => events.includes('restored'), 5_000, 'restored');
      expect(events).toEqual(['lost', 'restored']);
      expect(client.connectionGeneration).toBe(1);
      expect(client.isConnected).toBe(true);

      const after = (await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      expect(after).not.toBe(before);
    } finally {
      ConnectionHealth.off('lost', onLost);
      ConnectionHealth.off('restored', onRestored);
    }
  });

  it('fails fast with a connection error during an outage and recovers when it ends', async () => {
    const app = `rc-outage-${guid()}`;
    const client = await open(app);
    await beginOutage(app);
    await waitFor(() => !client.isConnected, 2_000, 'loss observed');

    const started = Date.now();
    const error = await client.query('SELECT 1').catch((e: unknown) => e);
    expect(Date.now() - started).toBeLessThan(200);
    expect(error).toBeInstanceOf(HotMeshConnectionError);
    expect(isConnectionError(error)).toBe(true);
    expect((error as Error).message).toBe('postgres-client-reconnecting');
    expect(ConnectionHealth.snapshot().state).toBe('down');

    //the database refuses every attempt; the client keeps trying
    await sleepFor(1_500);
    expect(client.isConnected).toBe(false);

    await endOutage();
    await waitFor(() => client.isConnected, 5_000, 'reconnect after outage');
    expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });

  it('raises a connection error (with the native cause) for a query in flight when the session dies', async () => {
    const app = `rc-inflight-${guid()}`;
    const client = await open(app);
    const inflight = client.query('SELECT pg_sleep(5)').catch((e: unknown) => e);
    await sleepFor(300);
    await terminateBackends(app);
    const error = await inflight;
    expect(error).toBeInstanceOf(HotMeshConnectionError);
    expect((error as HotMeshConnectionError).message).toBe('postgres-session-lost');
    expect(isConnectionError((error as HotMeshConnectionError).cause)).toBe(true);
    await waitFor(() => client.isConnected, 5_000, 'reconnect');
  });

  it('never continues a transaction on a replacement session', async () => {
    const app = `rc-tx-${guid()}`;
    const client = await open(app);
    await client.query('CREATE TABLE IF NOT EXISTS resilience_tx (id text PRIMARY KEY)');
    await client.query('DELETE FROM resilience_tx');

    await client.query('BEGIN');
    await client.query(`INSERT INTO resilience_tx VALUES ('before-loss')`);
    await terminateBackends(app);
    await waitFor(() => client.connectionGeneration === 1 && client.isConnected, 5_000, 'reconnect');

    //the rest of the lost transaction is refused, not run in autocommit
    const error = await client
      .query(`INSERT INTO resilience_tx VALUES ('after-loss')`)
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(HotMeshConnectionError);
    expect((error as Error).message).toBe('postgres-transaction-lost');

    //the owner's ROLLBACK closes it (the server already rolled back)
    await expect(client.query('ROLLBACK')).resolves.toMatchObject({ rowCount: 0 });

    const rows = (await client.query('SELECT id FROM resilience_tx')).rows;
    expect(rows).toEqual([]);
    await client.query('DROP TABLE resilience_tx');
  });

  it('refuses a COMMIT of a lost transaction', async () => {
    const app = `rc-commit-${guid()}`;
    const client = await open(app);
    await client.query('BEGIN');
    await terminateBackends(app);
    await waitFor(() => client.connectionGeneration === 1 && client.isConnected, 5_000, 'reconnect');
    const error = await client.query('COMMIT').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(HotMeshConnectionError);
    //closed: later statements run normally
    expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });

  it('bounds the refusal window when the owner never closes the lost transaction', async () => {
    const app = `rc-txwindow-${guid()}`;
    const client = await open(app, { ...FAST, txLostWindowMs: 500 });
    await client.query('BEGIN');
    await terminateBackends(app);
    await waitFor(() => client.connectionGeneration === 1 && client.isConnected, 5_000, 'reconnect');
    await expect(client.query('SELECT 1')).rejects.toBeInstanceOf(HotMeshConnectionError);
    await sleepFor(700);
    expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });

  it('treats a single-string BEGIN ... COMMIT batch as atomic, not as an open transaction', async () => {
    const app = `rc-batch-${guid()}`;
    const client = await open(app);
    await client.query('BEGIN;\nSELECT 1;\nCOMMIT;');
    await terminateBackends(app);
    await waitFor(() => client.connectionGeneration === 1 && client.isConnected, 5_000, 'reconnect');
    expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });

  it('keeps pg semantics once ended: the pg message, no reconnect, idempotent end', async () => {
    const app = `rc-ended-${guid()}`;
    const client = await open(app);
    await client.end();
    await client.end();
    const error = await client.query('SELECT 1').catch((e: unknown) => e);
    expect(error).not.toBeInstanceOf(HotMeshConnectionError);
    expect((error as Error).message).toBe('Client was closed and is not queryable');
    expect(client._ended).toBe(true);
    expect(client._ending).toBe(true);
    await sleepFor(300);
    expect(client.isConnected).toBe(false);
  });

  it('stays compatible with pg.Client callers', async () => {
    const app = `rc-compat-${guid()}`;
    const client: any = await open(app);
    expect(client instanceof Client).toBe(true);
    //provider detection inspects own keys of the live client
    expect(Object.keys(client)).toContain('database');
    expect(identifyProvider(client)).toBe('postgres');
    expect('connection' in client).toBe(true);
    expect(typeof client.processID).toBe('number');
    expect(client.escapeIdentifier('a"b')).toBe('"a""b"');

    const viaCallback = await new Promise((resolve, reject) =>
      client.query('SELECT 7 AS n', (err: Error, res: any) =>
        err ? reject(err) : resolve(res.rows[0].n),
      ),
    );
    expect(viaCallback).toBe(7);

    const viaConfig = await client.query({ text: 'SELECT $1::int AS n', values: [9] });
    expect(viaConfig.rows[0].n).toBe(9);
  });

  it('re-emits notifications across a reconnect once the owner re-arms LISTEN', async () => {
    const app = `rc-notify-${guid()}`;
    const client = await open(app);
    const channel = `rc_${guid().replace(/[^a-z0-9]/gi, '').toLowerCase()}`.slice(0, 40);
    const received: string[] = [];
    client.on('notification', (msg: { channel: string; payload: string }) => {
      if (msg.channel === channel) received.push(msg.payload);
    });
    client.on('reconnected', () => {
      void client.query(`LISTEN "${channel}"`);
    });
    await client.query(`LISTEN "${channel}"`);
    await client.query(`NOTIFY "${channel}", 'one'`);
    await waitFor(() => received.length === 1, 2_000, 'first notification');

    await terminateBackends(app);
    await waitFor(() => client.connectionGeneration === 1 && client.isConnected, 5_000, 'reconnect');
    await sleepFor(200);
    await client.query(`NOTIFY "${channel}", 'two'`);
    await waitFor(() => received.length === 2, 2_000, 'second notification');
    expect(received).toEqual(['one', 'two']);
  });

  it('delivers the loss to an error listener the caller registered, and never throws it', async () => {
    const app = `rc-errorlistener-${guid()}`;
    const client = await open(app);
    const errors: Error[] = [];
    client.on('error', (error: Error) => errors.push(error));
    await terminateBackends(app);
    await waitFor(() => errors.length > 0, 2_000, 'error listener');
    expect(isConnectionError(errors[0])).toBe(true);
    await waitFor(() => client.isConnected, 5_000, 'reconnect');
  });

  it('isolates a faulty reconnected listener from the reconnect loop', async () => {
    const app = `rc-faulty-${guid()}`;
    const client = await open(app);
    client.on('reconnected', () => {
      throw new Error('listener fault');
    });
    await terminateBackends(app);
    await waitFor(() => client.isConnected && client.connectionGeneration === 1, 5_000, 'reconnect');
    await terminateBackends(app);
    await waitFor(() => client.connectionGeneration === 2, 5_000, 'second reconnect');
    expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });

  it('survives repeated restarts in a row', async () => {
    const app = `rc-flap-${guid()}`;
    const client = await open(app);
    for (let generation = 1; generation <= 5; generation++) {
      await terminateBackends(app);
      await waitFor(
        () => client.isConnected && client.connectionGeneration === generation,
        5_000,
        `reconnect ${generation}`,
      );
    }
    expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });

  it('detects a blackholed host with the heartbeat and reconnects once it answers', async () => {
    const proxy = await startPostgresProxy();
    try {
      const app = `rc-heartbeat-${guid()}`;
      const client = await open(
        app,
        { ...FAST, heartbeatMs: 200, heartbeatTimeoutMs: 300 },
        { host: proxy.host, port: proxy.port },
      );
      expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);

      proxy.blackhole();
      const detectedIn = await waitFor(() => !client.isConnected, 3_000, 'heartbeat detection');
      expect(detectedIn).toBeLessThan(2_000);

      proxy.restore();
      await waitFor(() => client.isConnected, 5_000, 'reconnect through the proxy');
      expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
    } finally {
      await proxy.close();
    }
  });

  it('survives a hard network reset of every socket', async () => {
    const proxy = await startPostgresProxy();
    try {
      const app = `rc-reset-${guid()}`;
      const client = await open(app, FAST, { host: proxy.host, port: proxy.port });
      proxy.resetAll();
      await waitFor(() => client.connectionGeneration === 1 && client.isConnected, 5_000, 'reconnect');
      expect((await client.query('SELECT 1 AS ok')).rows[0].ok).toBe(1);
    } finally {
      await proxy.close();
    }
  });

  it('does not heartbeat a busy session (a long query is not a lost socket)', async () => {
    const app = `rc-busy-${guid()}`;
    const client = await open(app, { ...FAST, heartbeatMs: 100, heartbeatTimeoutMs: 150 });
    const result = await client.query('SELECT pg_sleep(1), 1 AS ok');
    expect(result.rows[0].ok).toBe(1);
    expect(client.connectionGeneration).toBe(0);
  });

  it('fails the initial connect the way pg does (no background retry for a client that never connected)', async () => {
    const client = new ResilientPostgresClient(
      Client as any,
      { ...postgres_options, port: 1, connectionTimeoutMillis: 500 } as any,
      guid(),
      logger,
      FAST,
    );
    await expect(client.connect()).rejects.toBeDefined();
    expect(client.isConnected).toBe(false);
    await client.end();
  });
});
