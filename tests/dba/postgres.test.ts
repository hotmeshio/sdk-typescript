import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import config from '../$setup/config';
import { guid, sleepFor } from '../../modules/utils';
import { DBA } from '../../services/dba';
import { PostgresConnection } from '../../services/connector/providers/postgres';
import { HotMesh } from '../../services/hotmesh';
import { dropTables } from '../$setup/postgres';

const appId = 'dba';

const postgres_options = {
  user: config.POSTGRES_USER,
  host: config.POSTGRES_HOST,
  database: config.POSTGRES_DB,
  password: config.POSTGRES_PASSWORD,
  port: config.POSTGRES_PORT,
};

const connection = {
  class: Postgres,
  options: postgres_options,
};

describe('DBA | Postgres', () => {
  let pgClient: any;

  beforeAll(async () => {
    pgClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(pgClient);

    // Bootstrap schema and tables via HotMesh.init
    const hotMesh = await HotMesh.init({
      appId,
      logLevel: 'error',
      engine: { connection },
    });
    await sleepFor(3000);
    await hotMesh.stop();
  }, 30_000);

  afterAll(async () => {
    await PostgresConnection.disconnectAll();
  }, 10_000);

  const schema = DBA.safeName(appId);

  describe('deploy', () => {
    it('should deploy the prune function', async () => {
      await DBA.deploy(connection, appId);

      const result = await pgClient.query(
        `SELECT proname FROM pg_proc p
         JOIN pg_namespace n ON p.pronamespace = n.oid
         WHERE n.nspname = $1 AND p.proname = 'prune'`,
        [schema],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].proname).toBe('prune');
    });
  });

  describe('prune', () => {
    it('should return zero counts on empty tables', async () => {
      const result = await DBA.prune({
        appId,
        connection,
        expire: '0 seconds',
        attributes: true,
      });

      expect(result).toEqual({
        jobs: 0,
        streams: 0,
        attributes: 0,
      });
    });

    it('should prune expired jobs older than retention window', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, expired_at)
        VALUES (gen_random_uuid(), 'prune-test-old', 0,
                NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
      });

      expect(result.jobs).toBe(1);
    });

    it('should NOT prune expired jobs within the retention window', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, expired_at)
        VALUES (gen_random_uuid(), 'prune-test-recent', 0,
                NOW() - INTERVAL '3 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
      });

      expect(result.jobs).toBe(0);

      // cleanup
      await pgClient.query(`
        DELETE FROM ${schema}.jobs WHERE key = 'prune-test-recent'
      `);
    });

    it('should prune expired stream messages', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.streams
          (stream_name, group_name, message, expired_at)
        VALUES
          ('prune-test-stream', 'ENGINE', '{}',
           NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
      });

      expect(result.streams).toBe(1);
    });

    it('should strip execution artifacts from completed jobs', async () => {
      const jobResult = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status)
        VALUES (gen_random_uuid(), 'prune-test-attrs', 0)
        RETURNING id
      `);
      const jobId = jobResult.rows[0].id;

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'return_data',   '{"result": 1}', 'jdata'),
          ($1, 'search_field',  'searchable',     'udata'),
          ($1, 'activity_data', 'temp',           'adata'),
          ($1, 'hash_mark',    'temp',           'hmark'),
          ($1, 'job_mark',     'temp',           'jmark'),
          ($1, 'status_data',  'temp',           'status'),
          ($1, 'other_data',   'temp',           'other')
      `, [jobId]);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
        attributes: true,
      });

      // 5 artifact types stripped: adata, hmark, jmark, status, other
      expect(result.attributes).toBe(5);

      // jdata and udata remain
      const remaining = await pgClient.query(
        `SELECT type::text FROM ${schema}.jobs_attributes
         WHERE job_id = $1 ORDER BY type`,
        [jobId],
      );
      expect(remaining.rows.length).toBe(2);
      expect(remaining.rows.map((r: any) => r.type).sort())
        .toEqual(['jdata', 'udata']);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE id = $1`, [jobId],
      );
    });

    it('should NOT strip attributes when attributes option is false', async () => {
      const jobResult = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status)
        VALUES (gen_random_uuid(), 'prune-test-no-strip', 0)
        RETURNING id
      `);
      const jobId = jobResult.rows[0].id;

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'return_data', '{"result": 1}', 'jdata'),
          ($1, 'some_mark',   'temp',          'hmark')
      `, [jobId]);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
        attributes: false,
      });

      expect(result.attributes).toBe(0);

      const remaining = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs_attributes
         WHERE job_id = $1`,
        [jobId],
      );
      expect(Number(remaining.rows[0].cnt)).toBe(2);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE id = $1`, [jobId],
      );
    });
  });

  describe('independent targeting', () => {
    it('should skip jobs when jobs is false', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, expired_at)
        VALUES (gen_random_uuid(), 'skip-jobs-test', 0,
                NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
        jobs: false,
        streams: false,
      });

      expect(result.jobs).toBe(0);

      // verify the row is still there
      const check = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs
         WHERE key = 'skip-jobs-test'`,
      );
      expect(Number(check.rows[0].cnt)).toBe(1);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key = 'skip-jobs-test'`,
      );
    });

    it('should skip streams when streams is false', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.streams
          (stream_name, group_name, message, expired_at)
        VALUES
          ('skip-streams-test', 'ENGINE', '{}',
           NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '7 days',
        jobs: false,
        streams: false,
      });

      expect(result.streams).toBe(0);

      // verify the row is still there
      const check = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.streams
         WHERE stream_name = 'skip-streams-test'`,
      );
      expect(Number(check.rows[0].cnt)).toBe(1);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.streams
         WHERE stream_name = 'skip-streams-test'`,
      );
    });

    it('should strip attributes without touching jobs or streams', async () => {
      // Insert a completed job with mixed attributes
      const jobResult = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status)
        VALUES (gen_random_uuid(), 'attrs-only-test', 0)
        RETURNING id
      `);
      const jobId = jobResult.rows[0].id;

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'return_data',   '{"ok": true}', 'jdata'),
          ($1, 'search_field',  'findme',        'udata'),
          ($1, 'activity_data', 'temp',          'adata'),
          ($1, 'hash_mark',    'temp',           'hmark')
      `, [jobId]);

      // Insert an expired job and stream that should NOT be touched
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, expired_at)
        VALUES (gen_random_uuid(), 'attrs-only-expired', 0,
                NOW() - INTERVAL '10 days')
      `);
      await pgClient.query(`
        INSERT INTO ${schema}.streams
          (stream_name, group_name, message, expired_at)
        VALUES ('attrs-only-stream', 'ENGINE', '{}',
                NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        jobs: false,
        streams: false,
        attributes: true,
      });

      // Only attributes stripped; jobs and streams untouched
      expect(result.jobs).toBe(0);
      expect(result.streams).toBe(0);
      expect(result.attributes).toBe(2); // adata + hmark

      // jdata and udata remain
      const remaining = await pgClient.query(
        `SELECT type::text FROM ${schema}.jobs_attributes
         WHERE job_id = $1 ORDER BY type`,
        [jobId],
      );
      expect(remaining.rows.map((r: any) => r.type).sort())
        .toEqual(['jdata', 'udata']);

      // expired job and stream still exist
      const expiredJob = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs
         WHERE key = 'attrs-only-expired'`,
      );
      expect(Number(expiredJob.rows[0].cnt)).toBe(1);

      const expiredStream = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.streams
         WHERE stream_name = 'attrs-only-stream'`,
      );
      expect(Number(expiredStream.rows[0].cnt)).toBe(1);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key IN ('attrs-only-test', 'attrs-only-expired')`,
      );
      await pgClient.query(
        `DELETE FROM ${schema}.streams WHERE stream_name = 'attrs-only-stream'`,
      );
    });

    it('should prune streams only with a short retention', async () => {
      // Insert an expired stream message (2 days old)
      await pgClient.query(`
        INSERT INTO ${schema}.streams
          (stream_name, group_name, message, expired_at)
        VALUES
          ('hourly-prune-stream', 'ENGINE', '{}',
           NOW() - INTERVAL '2 days')
      `);

      // Insert an expired job (2 days old) that should NOT be touched
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, expired_at)
        VALUES (gen_random_uuid(), 'hourly-prune-job', 0,
                NOW() - INTERVAL '2 days')
      `);

      const result = await DBA.prune({
        appId,
        connection,
        expire: '24 hours',
        jobs: false,
        streams: true,
      });

      expect(result.streams).toBe(1);
      expect(result.jobs).toBe(0);

      // expired job still exists
      const jobCheck = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs
         WHERE key = 'hourly-prune-job'`,
      );
      expect(Number(jobCheck.rows[0].cnt)).toBe(1);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key = 'hourly-prune-job'`,
      );
    });
  });
});
