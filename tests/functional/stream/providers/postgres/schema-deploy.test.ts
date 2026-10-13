/**
 * Proves stream schema deployment is safe to repeat at boot.
 *
 * - A redeploy from a new process leaves live indexes in place (no
 *   drop/rebuild, which locks the stream tables and rescans the heap).
 * - An index with a stale definition is still rebuilt.
 * - Repeat and concurrent deploys on one client run the DDL once.
 * - A failed deploy is retried, never cached.
 */
import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Client } from 'pg';

import { LoggerService } from '../../../../../services/logger';
import { deploySchema } from '../../../../../services/stream/providers/postgres/kvtables';
import { dropTables, postgres_options } from '../../../../$setup/postgres';

const APP_ID = 'schemadeploy';
const SCHEMA = 'schemadeploy';
const OTHER_APP_ID = 'schemadeploy2';

describe('FUNCTIONAL | Stream Schema Deploy | Postgres', () => {
  const logger = new LoggerService();
  const clients: Client[] = [];
  let pg: Client;

  //each new client stands in for a newly booted process
  const newClient = async (): Promise<Client> => {
    const client = new Client(postgres_options);
    await client.connect();
    clients.push(client);
    return client;
  };

  //every index in the schema (parents and partition children) by name
  const indexOids = async (): Promise<Record<string, number>> => {
    const { rows } = await pg.query(
      `SELECT c.relname, c.oid::int AS oid
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relkind IN ('i', 'I')`,
      [SCHEMA],
    );
    return Object.fromEntries(rows.map((r) => [r.relname, r.oid]));
  };

  const indexDef = async (name: string): Promise<string> => {
    const { rows } = await pg.query(
      `SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`,
      [SCHEMA, name],
    );
    return rows[0]?.indexdef;
  };

  const lockCalls = (spy: ReturnType<typeof vi.spyOn>): number =>
    spy.mock.calls.filter((args: unknown[]) =>
      String(args[0]).includes('pg_try_advisory_lock'),
    ).length;

  beforeAll(async () => {
    pg = await newClient();
    await dropTables(pg);
    await deploySchema(await newClient(), APP_ID, logger);
  });

  afterAll(async () => {
    for (const client of clients) {
      await client.end();
    }
  });

  it('keeps live indexes when a new process redeploys', async () => {
    const before = await indexOids();
    expect(before['idx_engine_streams_active_messages']).toBeDefined();
    expect(before['idx_engine_streams_message_fetch']).toBeDefined();
    expect(before['idx_worker_streams_active_messages']).toBeDefined();
    expect(before['idx_worker_streams_message_fetch']).toBeDefined();

    await deploySchema(await newClient(), APP_ID, logger);
    await deploySchema(await newClient(), APP_ID, logger);

    expect(await indexOids()).toEqual(before);
  });

  it('rebuilds an index whose definition is stale', async () => {
    //pre-0.20.1 shape: visible_at between priority and id
    await pg.query(`DROP INDEX ${SCHEMA}.idx_engine_streams_message_fetch`);
    await pg.query(`
      CREATE INDEX idx_engine_streams_message_fetch
      ON ${SCHEMA}.engine_streams (stream_name, priority DESC, visible_at, id)
      WHERE expired_at IS NULL
    `);
    const before = await indexOids();

    await deploySchema(await newClient(), APP_ID, logger);

    const after = await indexOids();
    expect(await indexDef('idx_engine_streams_message_fetch')).toContain(
      '(stream_name, priority DESC, id)',
    );
    expect(after['idx_engine_streams_message_fetch']).not.toEqual(
      before['idx_engine_streams_message_fetch'],
    );
    //the current indexes are untouched
    expect(after['idx_engine_streams_active_messages']).toEqual(
      before['idx_engine_streams_active_messages'],
    );
    expect(after['idx_worker_streams_message_fetch']).toEqual(
      before['idx_worker_streams_message_fetch'],
    );
  });

  it('runs the DDL once for repeat and concurrent deploys on one client', async () => {
    const client = await newClient();
    const spy = vi.spyOn(client, 'query');

    await Promise.all(
      Array.from({ length: 20 }, () => deploySchema(client, APP_ID, logger)),
    );
    for (let i = 0; i < 5; i++) {
      await deploySchema(client, APP_ID, logger);
    }
    expect(lockCalls(spy)).toBe(1);

    //a different app on the same client deploys its own schema
    await deploySchema(client, OTHER_APP_ID, logger);
    expect(lockCalls(spy)).toBe(2);
  });

  it('retries a failed deploy instead of caching the failure', async () => {
    const client = await newClient();
    const spy = vi.spyOn(client, 'query');
    spy.mockImplementationOnce((() =>
      Promise.reject(new Error('connection lost'))) as any);

    await expect(deploySchema(client, APP_ID, logger)).rejects.toThrow(
      'connection lost',
    );
    await deploySchema(client, APP_ID, logger);
    expect(lockCalls(spy)).toBe(1);
  });
});
