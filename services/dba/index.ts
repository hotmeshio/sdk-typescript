import { PruneOptions, PruneResult } from '../../types/dba';
import { guid } from '../../modules/utils';
import { PostgresConnection } from '../connector/providers/postgres';
import {
  PostgresClassType,
  PostgresClientOptions,
  PostgresClientType,
} from '../../types/postgres';

/**
 * Database maintenance operations for HotMesh's Postgres backend.
 *
 * HotMesh uses soft-delete patterns: expired jobs and stream messages
 * retain their rows with `expired_at` set but are never physically
 * removed during normal operation. Over time, three tables accumulate
 * dead rows:
 *
 * | Table | What accumulates |
 * |---|---|
 * | `{appId}.jobs` | Completed/expired jobs with `expired_at` set |
 * | `{appId}.jobs_attributes` | Execution artifacts (`adata`, `hmark`, `jmark`, `status`, `other`) that are only needed during workflow execution |
 * | `{appId}.streams` | Processed stream messages with `expired_at` set |
 *
 * The `DBA` service addresses this with two methods:
 *
 * - {@link DBA.prune | prune()} — Targets any combination of jobs,
 *   streams, and attributes independently. Each table can be pruned on
 *   its own schedule with its own retention window.
 * - {@link DBA.deploy | deploy()} — Pre-deploys the Postgres function
 *   (e.g., during CI/CD migrations) without running a prune.
 *
 * ## Independent cron schedules (TypeScript)
 *
 * Each table can be targeted independently, allowing different retention
 * windows and schedules:
 *
 * @example
 * ```typescript
 * import { Client as Postgres } from 'pg';
 * import { DBA } from '@hotmeshio/hotmesh';
 *
 * const connection = {
 *   class: Postgres,
 *   options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 * };
 *
 * // Cron 1 — Nightly: strip execution artifacts from completed jobs
 * //          Keeps all jobs and their jdata/udata; keeps all streams.
 * await DBA.prune({
 *   appId: 'myapp', connection,
 *   jobs: false, streams: false, attributes: true,
 * });
 *
 * // Cron 2 — Hourly: remove processed stream messages older than 24h
 * await DBA.prune({
 *   appId: 'myapp', connection,
 *   expire: '24 hours',
 *   jobs: false, streams: true,
 * });
 *
 * // Cron 3 — Weekly: remove expired jobs older than 30 days
 * await DBA.prune({
 *   appId: 'myapp', connection,
 *   expire: '30 days',
 *   jobs: true, streams: false,
 * });
 * ```
 *
 * ## Direct SQL (schedulable via pg_cron)
 *
 * The underlying Postgres function can be called directly, without
 * the TypeScript SDK. Schedule it via `pg_cron`, `crontab`, or any
 * SQL client:
 *
 * ```sql
 * -- Strip attributes only (keep all jobs and streams)
 * SELECT * FROM myapp.prune('0 seconds', false, false, true);
 *
 * -- Prune streams older than 24 hours (keep jobs)
 * SELECT * FROM myapp.prune('24 hours', false, true, false);
 *
 * -- Prune expired jobs older than 30 days (keep streams)
 * SELECT * FROM myapp.prune('30 days', true, false, false);
 *
 * -- Prune everything older than 7 days and strip attributes
 * SELECT * FROM myapp.prune('7 days', true, true, true);
 * ```
 */
class DBA {
  /**
   * @private
   */
  constructor() {}

  /**
   * Sanitizes an appId for use as a Postgres schema name.
   * Mirrors the naming logic used during table deployment.
   * @private
   */
  static safeName(input: string): string {
    if (!input) return 'connections';
    let name = input.trim().toLowerCase();
    name = name.replace(/[^a-z0-9]+/g, '_');
    if (name.length > 63) {
      name = name.slice(0, 63);
    }
    name = name.replace(/_+$/g, '');
    return name || 'connections';
  }

  /**
   * Acquires a Postgres client from the connection config.
   * @private
   */
  static async getClient(
    connection: PruneOptions['connection'],
  ): Promise<{
    client: PostgresClientType;
    release: () => Promise<void>;
  }> {
    if (PostgresConnection.isPoolClient(connection.class)) {
      const poolClient = await (connection.class as any).connect();
      return {
        client: poolClient as PostgresClientType,
        release: async () => poolClient.release(),
      };
    }
    const pgConnection = await PostgresConnection.connect(
      guid(),
      connection.class as PostgresClassType,
      connection.options as PostgresClientOptions,
    );
    return {
      client: pgConnection.getClient(),
      release: async () => {},
    };
  }

  /**
   * Returns the SQL for the server-side `prune()` function.
   * @private
   */
  static getPruneFunctionSQL(schema: string): string {
    return `
      CREATE OR REPLACE FUNCTION ${schema}.prune(
        retention INTERVAL DEFAULT INTERVAL '7 days',
        prune_jobs BOOLEAN DEFAULT TRUE,
        prune_streams BOOLEAN DEFAULT TRUE,
        strip_attributes BOOLEAN DEFAULT FALSE
      )
      RETURNS TABLE(
        deleted_jobs BIGINT,
        deleted_streams BIGINT,
        stripped_attributes BIGINT
      )
      LANGUAGE plpgsql
      AS $$
      DECLARE
        v_deleted_jobs BIGINT := 0;
        v_deleted_streams BIGINT := 0;
        v_stripped_attributes BIGINT := 0;
      BEGIN
        -- 1. Hard-delete expired jobs older than the retention window.
        --    FK CASCADE on jobs_attributes handles attribute cleanup.
        IF prune_jobs THEN
          DELETE FROM ${schema}.jobs
          WHERE expired_at IS NOT NULL
            AND expired_at < NOW() - retention;
          GET DIAGNOSTICS v_deleted_jobs = ROW_COUNT;
        END IF;

        -- 2. Hard-delete expired stream messages older than the retention window.
        IF prune_streams THEN
          DELETE FROM ${schema}.streams
          WHERE expired_at IS NOT NULL
            AND expired_at < NOW() - retention;
          GET DIAGNOSTICS v_deleted_streams = ROW_COUNT;
        END IF;

        -- 3. Optionally strip execution artifacts from completed, live jobs.
        --    Retains jdata (workflow return data) and udata (searchable data).
        IF strip_attributes THEN
          DELETE FROM ${schema}.jobs_attributes
          WHERE job_id IN (
            SELECT id FROM ${schema}.jobs
            WHERE status = 0
              AND is_live = TRUE
          )
          AND type NOT IN ('jdata', 'udata');
          GET DIAGNOSTICS v_stripped_attributes = ROW_COUNT;
        END IF;

        deleted_jobs := v_deleted_jobs;
        deleted_streams := v_deleted_streams;
        stripped_attributes := v_stripped_attributes;
        RETURN NEXT;
      END;
      $$;
    `;
  }

  /**
   * Deploys the `prune()` Postgres function into the target schema.
   * Idempotent — uses `CREATE OR REPLACE` and can be called repeatedly.
   *
   * The function is automatically deployed when {@link DBA.prune} is called,
   * but this method is exposed for explicit control (e.g., CI/CD
   * migration scripts that provision database objects before the
   * application starts).
   *
   * @param connection - Postgres provider configuration
   * @param appId - Application identifier (schema name)
   *
   * @example
   * ```typescript
   * import { Client as Postgres } from 'pg';
   * import { DBA } from '@hotmeshio/hotmesh';
   *
   * // Pre-deploy during CI/CD migration
   * await DBA.deploy(
   *   {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   'myapp',
   * );
   * ```
   */
  static async deploy(
    connection: PruneOptions['connection'],
    appId: string,
  ): Promise<void> {
    const schema = DBA.safeName(appId);
    const { client, release } = await DBA.getClient(connection);
    try {
      await client.query(DBA.getPruneFunctionSQL(schema));
    } finally {
      await release();
    }
  }

  /**
   * Prunes expired data and/or strips execution artifacts from
   * completed jobs. Each operation is independently controlled,
   * so callers can target a single table per cron schedule.
   *
   * Operations (each enabled individually):
   * 1. **jobs** — Hard-deletes expired jobs older than the retention
   *    window (FK CASCADE removes their attributes automatically)
   * 2. **streams** — Hard-deletes expired stream messages older than
   *    the retention window
   * 3. **attributes** — Strips non-essential attributes (`adata`,
   *    `hmark`, `jmark`, `status`, `other`) from completed jobs,
   *    retaining only `jdata` and `udata`
   *
   * @param options - Prune configuration
   * @returns Counts of deleted/stripped rows
   *
   * @example
   * ```typescript
   * import { Client as Postgres } from 'pg';
   * import { DBA } from '@hotmeshio/hotmesh';
   *
   * // Strip attributes only — keep all jobs and streams
   * await DBA.prune({
   *   appId: 'myapp',
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   jobs: false,
   *   streams: false,
   *   attributes: true,
   * });
   * ```
   */
  static async prune(options: PruneOptions): Promise<PruneResult> {
    const schema = DBA.safeName(options.appId);
    const expire = options.expire ?? '7 days';
    const jobs = options.jobs ?? true;
    const streams = options.streams ?? true;
    const attributes = options.attributes ?? false;

    await DBA.deploy(options.connection, options.appId);

    const { client, release } = await DBA.getClient(options.connection);
    try {
      const result = await client.query(
        `SELECT * FROM ${schema}.prune($1::interval, $2::boolean, $3::boolean, $4::boolean)`,
        [expire, jobs, streams, attributes],
      );
      const row = result.rows[0];
      return {
        jobs: Number(row.deleted_jobs),
        streams: Number(row.deleted_streams),
        attributes: Number(row.stripped_attributes),
      };
    } finally {
      await release();
    }
  }
}

export { DBA };
