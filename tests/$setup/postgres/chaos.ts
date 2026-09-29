import net from 'net';

import { Client } from 'pg';

import config from '../config';

/**
 * Chaos helpers for connection-resilience tests.
 *
 * An admin client connected to the `postgres` maintenance database
 * drives every fault, so it stays usable while the test database is
 * refusing connections.
 */

const TEST_DATABASE = config.POSTGRES_DB as string;

/** HotMesh-created connections report this application_name. */
export const HOTMESH_APPLICATION_NAME = 'hotmesh';

async function withAdmin<T>(fn: (admin: Client) => Promise<T>): Promise<T> {
  const admin = new Client({
    user: config.POSTGRES_USER,
    host: config.POSTGRES_HOST,
    password: config.POSTGRES_PASSWORD,
    port: Number(config.POSTGRES_PORT),
    ssl: config.POSTGRES_SSL,
    database: 'postgres',
    application_name: 'hotmesh-chaos-admin',
  });
  admin.on('error', () => undefined);
  await admin.connect();
  try {
    return await fn(admin);
  } finally {
    await admin.end().catch(() => undefined);
  }
}

/**
 * Terminate every backend of the test database opened under the given
 * application_name. Clients see SQLSTATE 57P01 and a closed socket,
 * exactly what an RDS/Aurora restart produces.
 */
export async function terminateBackends(
  applicationName = HOTMESH_APPLICATION_NAME,
  database = TEST_DATABASE,
): Promise<number> {
  return withAdmin(async (admin) => {
    const result = await admin.query(
      `SELECT pg_terminate_backend(pid) AS terminated
         FROM pg_stat_activity
        WHERE datname = $1
          AND application_name = $2
          AND pid <> pg_backend_pid()`,
      [database, applicationName],
    );
    return result.rowCount;
  });
}

/**
 * Begin a deterministic outage: the test database refuses every new
 * connection until `endOutage`, and the live HotMesh sessions are
 * terminated. Connections opened under other application names stay up.
 */
export async function beginOutage(
  applicationName = HOTMESH_APPLICATION_NAME,
  database = TEST_DATABASE,
): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.query(`ALTER DATABASE "${database}" WITH ALLOW_CONNECTIONS false`);
  });
  await terminateBackends(applicationName, database);
}

/** End the outage: the test database accepts connections again. */
export async function endOutage(database = TEST_DATABASE): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.query(`ALTER DATABASE "${database}" WITH ALLOW_CONNECTIONS true`);
  });
}

/** Live backends of the test database opened under the application_name. */
export async function countBackends(
  applicationName = HOTMESH_APPLICATION_NAME,
  database = TEST_DATABASE,
): Promise<number> {
  return withAdmin(async (admin) => {
    const result = await admin.query(
      `SELECT count(*)::int AS count
         FROM pg_stat_activity
        WHERE datname = $1
          AND application_name = $2`,
      [database, applicationName],
    );
    return result.rows[0].count;
  });
}

/** Poll until the predicate holds or the timeout elapses (then throw). */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label = 'condition',
  intervalMs = 50,
): Promise<number> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return Date.now() - startedAt;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
}

/**
 * Records every uncaught exception and unhandled rejection while
 * installed. A resilient process produces none.
 */
export function createCrashGuard(): {
  crashes: unknown[];
  install: () => void;
  uninstall: () => void;
} {
  const crashes: unknown[] = [];
  const onCrash = (error: unknown) => {
    crashes.push(error);
  };
  return {
    crashes,
    install: () => {
      process.on('uncaughtException', onCrash);
      process.on('unhandledRejection', onCrash);
    },
    uninstall: () => {
      process.off('uncaughtException', onCrash);
      process.off('unhandledRejection', onCrash);
    },
  };
}

export interface PostgresProxy {
  port: number;
  host: string;
  /** Stop forwarding bytes in both directions without closing any socket. */
  blackhole: () => void;
  /** Resume forwarding for existing and new connections. */
  restore: () => void;
  /** Destroy every proxied socket (a hard network reset). */
  resetAll: () => void;
  close: () => Promise<void>;
}

/**
 * A TCP proxy in front of Postgres. `blackhole()` reproduces a host
 * that stops answering while the socket stays open (a failover whose old
 * address goes silent), which only a heartbeat can detect.
 */
export async function startPostgresProxy(
  targetHost = config.POSTGRES_HOST as string,
  targetPort = Number(config.POSTGRES_PORT),
): Promise<PostgresProxy> {
  let silent = false;
  const sockets = new Set<net.Socket>();
  const server = net.createServer((downstream) => {
    const upstream = net.connect(targetPort, targetHost);
    sockets.add(downstream);
    sockets.add(upstream);
    const forget = () => {
      sockets.delete(downstream);
      sockets.delete(upstream);
    };
    downstream.on('data', (chunk) => {
      if (!silent) upstream.write(chunk);
    });
    upstream.on('data', (chunk) => {
      if (!silent) downstream.write(chunk);
    });
    downstream.on('error', () => upstream.destroy());
    upstream.on('error', () => downstream.destroy());
    downstream.on('close', () => {
      upstream.destroy();
      forget();
    });
    upstream.on('close', () => {
      downstream.destroy();
      forget();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;
  return {
    port,
    host: '127.0.0.1',
    blackhole: () => {
      silent = true;
    },
    restore: () => {
      silent = false;
    },
    resetAll: () => {
      for (const socket of sockets) socket.destroy();
    },
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
