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
 * | `{appId}.jobs_attributes` | Execution artifacts (`adata`, `hmark`, `status`, `other`) that are only needed during workflow execution |
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
 * ## Attribute stripping preserves export history
 *
 * Stripping removes `adata`, `hmark`, `status`, and `other` attributes
 * from completed jobs while preserving:
 * - `jdata` — workflow return data
 * - `udata` — user-searchable data
 * - `jmark` — timeline markers needed for Temporal-compatible export
 *
 * Set `keepHmark: true` to also preserve `hmark` (activity state markers).
 *
 * ## Entity-scoped pruning
 *
 * Use `entities` to restrict pruning/stripping to specific entity types
 * (e.g., `['book', 'author']`). Use `pruneTransient` to delete expired
 * jobs with no entity (`entity IS NULL`).
 *
 * ## Idempotent stripping with `pruned_at`
 *
 * After stripping, jobs are marked with `pruned_at = NOW()`. Subsequent
 * prune calls skip already-pruned jobs, making the operation idempotent
 * and efficient for repeated scheduling.
 *
 * ## Independent cron schedules (TypeScript)
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
 * // Cron 3 — Weekly: remove expired 'book' jobs older than 30 days
 * await DBA.prune({
 *   appId: 'myapp', connection,
 *   expire: '30 days',
 *   jobs: true, streams: false,
 *   entities: ['book'],
 * });
 *
 * // Cron 4 — Weekly: remove transient (no entity) expired jobs
 * await DBA.prune({
 *   appId: 'myapp', connection,
 *   expire: '7 days',
 *   jobs: false, streams: false,
 *   pruneTransient: true,
 * });
 * ```
 *
 * ## Direct SQL (schedulable via pg_cron)
 *
 * The underlying Postgres function can be called directly, without
 * the TypeScript SDK. The first 4 parameters are backwards-compatible:
 *
 * ```sql
 * -- Strip attributes only (keep all jobs and streams)
 * SELECT * FROM myapp.prune('0 seconds', false, false, true);
 *
 * -- Prune only 'book' entity jobs older than 30 days
 * SELECT * FROM myapp.prune('30 days', true, false, false, ARRAY['book']);
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
   * Returns migration SQL for the `pruned_at` column.
   * Handles existing deployments that lack the column.
   * @private
   */
  static getMigrationSQL(schema: string): string {
    return `
      ALTER TABLE ${schema}.jobs
      ADD COLUMN IF NOT EXISTS pruned_at TIMESTAMP WITH TIME ZONE;

      CREATE INDEX IF NOT EXISTS idx_jobs_pruned_at
      ON ${schema}.jobs (pruned_at) WHERE pruned_at IS NULL;
    `;
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
        strip_attributes BOOLEAN DEFAULT FALSE,
        entity_list TEXT[] DEFAULT NULL,
        prune_transient BOOLEAN DEFAULT FALSE,
        keep_hmark BOOLEAN DEFAULT FALSE
      )
      RETURNS TABLE(
        deleted_jobs BIGINT,
        deleted_streams BIGINT,
        stripped_attributes BIGINT,
        deleted_transient BIGINT,
        marked_pruned BIGINT
      )
      LANGUAGE plpgsql
      AS $$
      DECLARE
        v_deleted_jobs BIGINT := 0;
        v_deleted_streams BIGINT := 0;
        v_stripped_attributes BIGINT := 0;
        v_deleted_transient BIGINT := 0;
        v_marked_pruned BIGINT := 0;
      BEGIN
        -- 1. Hard-delete expired jobs older than the retention window.
        --    FK CASCADE on jobs_attributes handles attribute cleanup.
        --    Optionally scoped to an entity allowlist.
        IF prune_jobs THEN
          DELETE FROM ${schema}.jobs
          WHERE expired_at IS NOT NULL
            AND expired_at < NOW() - retention
            AND (entity_list IS NULL OR entity = ANY(entity_list));
          GET DIAGNOSTICS v_deleted_jobs = ROW_COUNT;
        END IF;

        -- 2. Hard-delete transient (entity IS NULL) expired jobs.
        IF prune_transient THEN
          DELETE FROM ${schema}.jobs
          WHERE entity IS NULL
            AND expired_at IS NOT NULL
            AND expired_at < NOW() - retention;
          GET DIAGNOSTICS v_deleted_transient = ROW_COUNT;
        END IF;

        -- 3. Hard-delete expired stream messages older than the retention window.
        IF prune_streams THEN
          DELETE FROM ${schema}.streams
          WHERE expired_at IS NOT NULL
            AND expired_at < NOW() - retention;
          GET DIAGNOSTICS v_deleted_streams = ROW_COUNT;
        END IF;

        -- 4. Strip execution artifacts from completed, live, un-pruned jobs.
        --    Always preserves: jdata, udata, jmark (timeline/export history).
        --    Optionally preserves: hmark (when keep_hmark is true).
        IF strip_attributes THEN
          WITH target_jobs AS (
            SELECT id FROM ${schema}.jobs
            WHERE status = 0
              AND is_live = TRUE
              AND pruned_at IS NULL
              AND (entity_list IS NULL OR entity = ANY(entity_list))
          ),
          deleted AS (
            DELETE FROM ${schema}.jobs_attributes
            WHERE job_id IN (SELECT id FROM target_jobs)
              AND type NOT IN ('jdata', 'udata', 'jmark')
              AND (keep_hmark = FALSE OR type <> 'hmark')
            RETURNING job_id
          )
          SELECT COUNT(*) INTO v_stripped_attributes FROM deleted;

          -- Mark pruned jobs so they are skipped on future runs.
          WITH target_jobs AS (
            SELECT id FROM ${schema}.jobs
            WHERE status = 0
              AND is_live = TRUE
              AND pruned_at IS NULL
              AND (entity_list IS NULL OR entity = ANY(entity_list))
          )
          UPDATE ${schema}.jobs
          SET pruned_at = NOW()
          WHERE id IN (SELECT id FROM target_jobs);
          GET DIAGNOSTICS v_marked_pruned = ROW_COUNT;
        END IF;

        deleted_jobs := v_deleted_jobs;
        deleted_streams := v_deleted_streams;
        stripped_attributes := v_stripped_attributes;
        deleted_transient := v_deleted_transient;
        marked_pruned := v_marked_pruned;
        RETURN NEXT;
      END;
      $$;
    `;
  }

  /**
   * Deploys the `prune()` Postgres function into the target schema.
   * Also runs schema migrations (e.g., adding `pruned_at` column).
   * Idempotent — uses `CREATE OR REPLACE` and `IF NOT EXISTS`.
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
      await client.query(DBA.getMigrationSQL(schema));
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
   *    window (FK CASCADE removes their attributes automatically).
   *    Scoped by `entities` when set.
   * 2. **streams** — Hard-deletes expired stream messages older than
   *    the retention window
   * 3. **attributes** — Strips non-essential attributes (`adata`,
   *    `hmark`, `status`, `other`) from completed, un-pruned jobs.
   *    Preserves `jdata`, `udata`, and `jmark`. Marks stripped
   *    jobs with `pruned_at` for idempotency.
   * 4. **pruneTransient** — Deletes expired jobs with `entity IS NULL`
   *
   * @param options - Prune configuration
   * @returns Counts of deleted/stripped rows
   *
   * @example
   * ```typescript
   * import { Client as Postgres } from 'pg';
   * import { DBA } from '@hotmeshio/hotmesh';
   *
   * // Strip attributes from 'book' entities only
   * await DBA.prune({
   *   appId: 'myapp',
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   jobs: false,
   *   streams: false,
   *   attributes: true,
   *   entities: ['book'],
   * });
   * ```
   */
  static async prune(options: PruneOptions): Promise<PruneResult> {
    const schema = DBA.safeName(options.appId);
    const expire = options.expire ?? '7 days';
    const jobs = options.jobs ?? true;
    const streams = options.streams ?? true;
    const attributes = options.attributes ?? false;
    const entities = options.entities ?? null;
    const pruneTransient = options.pruneTransient ?? false;
    const keepHmark = options.keepHmark ?? false;

    await DBA.deploy(options.connection, options.appId);

    const { client, release } = await DBA.getClient(options.connection);
    try {
      const result = await client.query(
        `SELECT * FROM ${schema}.prune($1::interval, $2::boolean, $3::boolean, $4::boolean, $5::text[], $6::boolean, $7::boolean)`,
        [expire, jobs, streams, attributes, entities, pruneTransient, keepHmark],
      );
      const row = result.rows[0];
      return {
        jobs: Number(row.deleted_jobs),
        streams: Number(row.deleted_streams),
        attributes: Number(row.stripped_attributes),
        transient: Number(row.deleted_transient),
        marked: Number(row.marked_pruned),
      };
    } finally {
      await release();
    }
  }
}

export { DBA };
