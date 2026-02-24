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

    it('should add the pruned_at column', async () => {
      await DBA.deploy(connection, appId);

      const result = await pgClient.query(
        `SELECT column_name FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = 'jobs'
         AND column_name = 'pruned_at'`,
        [schema],
      );
      expect(result.rows.length).toBe(1);
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
        transient: 0,
        marked: 0,
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

    it('should strip execution artifacts but preserve jmark', async () => {
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

      // 4 artifact types stripped: adata, hmark, status, other
      // jmark is now preserved alongside jdata and udata
      expect(result.attributes).toBe(4);
      expect(result.marked).toBe(1);

      // jdata, udata, jmark remain
      const remaining = await pgClient.query(
        `SELECT type::text FROM ${schema}.jobs_attributes
         WHERE job_id = $1 ORDER BY type`,
        [jobId],
      );
      expect(remaining.rows.length).toBe(3);
      expect(remaining.rows.map((r: any) => r.type).sort())
        .toEqual(['jdata', 'jmark', 'udata']);

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

  describe('jmark preservation', () => {
    it('should preserve jmark timeline markers during stripping', async () => {
      const jobResult = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status)
        VALUES (gen_random_uuid(), 'jmark-preserve-test', 0)
        RETURNING id
      `);
      const jobId = jobResult.rows[0].id;

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'return_data', '{"ok":true}', 'jdata'),
          ($1, 'user_search', 'findme', 'udata'),
          ($1, '-proxy-1-', '/s{"data":{"msg":"hello"},"ac":"20240101","au":"20240102","job_id":"-wf-$greet-1"}', 'jmark'),
          ($1, '-child-0-', '/s{"job_id":"child-1","ac":"20240101","au":"20240102"}', 'jmark'),
          ($1, 'activity_data', 'temp', 'adata'),
          ($1, '-proxy,0,0-1-', 'state', 'hmark'),
          ($1, 'status_data', 'temp', 'status')
      `, [jobId]);

      const result = await DBA.prune({
        appId, connection,
        jobs: false, streams: false, attributes: true,
      });

      // adata, hmark, status stripped (3). jdata, udata, 2x jmark preserved.
      expect(result.attributes).toBe(3);

      const remaining = await pgClient.query(
        `SELECT type::text, field FROM ${schema}.jobs_attributes
         WHERE job_id = $1 ORDER BY type, field`, [jobId],
      );
      const types = remaining.rows.map((r: any) => r.type);
      expect(types).toContain('jdata');
      expect(types).toContain('udata');
      expect(types).toContain('jmark');
      expect(types).not.toContain('adata');
      expect(types).not.toContain('hmark');
      expect(types).not.toContain('status');
      expect(remaining.rows.length).toBe(4); // jdata + udata + 2x jmark

      // cleanup
      await pgClient.query(`DELETE FROM ${schema}.jobs WHERE id = $1`, [jobId]);
    });

    it('should preserve hmark when keepHmark is true', async () => {
      const jobResult = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status)
        VALUES (gen_random_uuid(), 'keep-hmark-test', 0)
        RETURNING id
      `);
      const jobId = jobResult.rows[0].id;

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'return_data', '{"ok":true}', 'jdata'),
          ($1, '-proxy,0,0-1-', 'state', 'hmark'),
          ($1, '-proxy-1-', '/s{"data":{}}', 'jmark'),
          ($1, 'activity_data', 'temp', 'adata')
      `, [jobId]);

      const result = await DBA.prune({
        appId, connection,
        jobs: false, streams: false, attributes: true,
        keepHmark: true,
      });

      // only adata stripped
      expect(result.attributes).toBe(1);

      const remaining = await pgClient.query(
        `SELECT type::text FROM ${schema}.jobs_attributes
         WHERE job_id = $1 ORDER BY type`, [jobId],
      );
      expect(remaining.rows.map((r: any) => r.type).sort())
        .toEqual(['hmark', 'jdata', 'jmark']);

      // cleanup
      await pgClient.query(`DELETE FROM ${schema}.jobs WHERE id = $1`, [jobId]);
    });
  });

  describe('pruned_at idempotency', () => {
    it('should mark jobs as pruned and skip them on subsequent calls', async () => {
      const jobResult = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status)
        VALUES (gen_random_uuid(), 'pruned-at-test', 0)
        RETURNING id
      `);
      const jobId = jobResult.rows[0].id;

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'return_data', '{"ok":true}', 'jdata'),
          ($1, 'activity_data', 'temp', 'adata')
      `, [jobId]);

      // First prune: strips adata, marks pruned
      const r1 = await DBA.prune({
        appId, connection,
        jobs: false, streams: false, attributes: true,
      });
      expect(r1.attributes).toBe(1); // adata stripped
      expect(r1.marked).toBe(1);

      // Verify pruned_at is set
      const check = await pgClient.query(
        `SELECT pruned_at FROM ${schema}.jobs WHERE id = $1`, [jobId],
      );
      expect(check.rows[0].pruned_at).not.toBeNull();

      // Second prune: should be a no-op (job already pruned)
      const r2 = await DBA.prune({
        appId, connection,
        jobs: false, streams: false, attributes: true,
      });
      expect(r2.attributes).toBe(0);
      expect(r2.marked).toBe(0);

      // cleanup
      await pgClient.query(`DELETE FROM ${schema}.jobs WHERE id = $1`, [jobId]);
    });
  });

  describe('entity filtering', () => {
    it('should only strip attributes for jobs in the entity allowlist', async () => {
      const j1 = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, entity)
        VALUES (gen_random_uuid(), 'entity-book', 0, 'book')
        RETURNING id
      `);
      const j2 = await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, entity)
        VALUES (gen_random_uuid(), 'entity-author', 0, 'author')
        RETURNING id
      `);

      await pgClient.query(`
        INSERT INTO ${schema}.jobs_attributes (job_id, field, value, type)
        VALUES
          ($1, 'adata1', 'temp', 'adata'),
          ($2, 'adata2', 'temp', 'adata')
      `, [j1.rows[0].id, j2.rows[0].id]);

      const result = await DBA.prune({
        appId, connection,
        jobs: false, streams: false, attributes: true,
        entities: ['book'],
      });

      // Only book's adata stripped
      expect(result.attributes).toBe(1);
      expect(result.marked).toBe(1);

      // Author's adata still present
      const remaining = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs_attributes
         WHERE job_id = $1`, [j2.rows[0].id],
      );
      expect(Number(remaining.rows[0].cnt)).toBe(1);

      // Author not marked as pruned
      const authorJob = await pgClient.query(
        `SELECT pruned_at FROM ${schema}.jobs WHERE id = $1`, [j2.rows[0].id],
      );
      expect(authorJob.rows[0].pruned_at).toBeNull();

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key IN ('entity-book', 'entity-author')`,
      );
    });

    it('should only delete expired jobs matching the entity allowlist', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, entity, expired_at)
        VALUES
          (gen_random_uuid(), 'del-book', 0, 'book', NOW() - INTERVAL '10 days'),
          (gen_random_uuid(), 'del-author', 0, 'author', NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId, connection,
        expire: '7 days',
        jobs: true, streams: false,
        entities: ['book'],
      });

      expect(result.jobs).toBe(1);

      // Author still exists
      const check = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs WHERE key = 'del-author'`,
      );
      expect(Number(check.rows[0].cnt)).toBe(1);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key = 'del-author'`,
      );
    });
  });

  describe('pruneTransient', () => {
    it('should delete transient (entity IS NULL) expired jobs', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, entity, expired_at)
        VALUES
          (gen_random_uuid(), 'transient-job', 0, NULL, NOW() - INTERVAL '10 days'),
          (gen_random_uuid(), 'entity-job', 0, 'book', NOW() - INTERVAL '10 days')
      `);

      const result = await DBA.prune({
        appId, connection,
        expire: '7 days',
        jobs: false,
        streams: false,
        pruneTransient: true,
      });

      expect(result.transient).toBe(1);
      expect(result.jobs).toBe(0); // jobs=false, so entity job untouched

      // Entity job still present
      const check = await pgClient.query(
        `SELECT COUNT(*) as cnt FROM ${schema}.jobs WHERE key = 'entity-job'`,
      );
      expect(Number(check.rows[0].cnt)).toBe(1);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key = 'entity-job'`,
      );
    });

    it('should NOT delete transient jobs within the retention window', async () => {
      await pgClient.query(`
        INSERT INTO ${schema}.jobs (id, key, status, entity, expired_at)
        VALUES (gen_random_uuid(), 'transient-recent', 0, NULL, NOW() - INTERVAL '3 days')
      `);

      const result = await DBA.prune({
        appId, connection,
        expire: '7 days',
        jobs: false, streams: false,
        pruneTransient: true,
      });

      expect(result.transient).toBe(0);

      // cleanup
      await pgClient.query(
        `DELETE FROM ${schema}.jobs WHERE key = 'transient-recent'`,
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
