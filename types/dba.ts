import { ProviderConfig } from './provider';

/**
 * Options for `DBA.prune()`.
 */
export interface PruneOptions {
  /**
   * The application identifier (Postgres schema name).
   */
  appId: string;

  /**
   * Postgres connection configuration. Uses the same format
   * as all other HotMesh services.
   *
   * @example
   * ```typescript
   * {
   *   class: Postgres,
   *   options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   * }
   * ```
   */
  connection: ProviderConfig;

  /**
   * Retention period for expired rows. Rows with `expired_at` older
   * than this interval are hard-deleted. Uses Postgres interval syntax.
   * @default '7 days'
   *
   * @example '7 days', '24 hours', '30 minutes'
   */
  expire?: string;

  /**
   * If true, hard-deletes expired jobs older than the retention window.
   * FK CASCADE on `jobs_attributes` automatically removes associated
   * attribute rows. When `entities` is set, only matching jobs are deleted.
   * @default true
   */
  jobs?: boolean;

  /**
   * If true, hard-deletes expired stream messages older than the
   * retention window from both `engine_streams` and `worker_streams`.
   * Use `engineStreams` / `workerStreams` for independent control.
   * @default true
   */
  streams?: boolean;

  /**
   * Override for `engine_streams` cleanup. When set, takes precedence
   * over `streams` for the engine table. Engine streams contain internal
   * routing messages and can be pruned aggressively.
   * @default undefined (falls back to `streams`)
   */
  engineStreams?: boolean;

  /**
   * Override for `worker_streams` cleanup. When set, takes precedence
   * over `streams` for the worker table. Worker streams contain workflow
   * input arguments and activity payloads needed by the exporter — use
   * a longer retention to preserve export fidelity.
   * @default undefined (falls back to `streams`)
   */
  workerStreams?: boolean;

  /**
   * Retention override for `engine_streams`. When set, uses this interval
   * instead of the global `expire` for engine stream cleanup.
   * @default undefined (falls back to `expire`)
   *
   * @example '24 hours'
   */
  engineStreamsExpire?: string;

  /**
   * Retention override for `worker_streams`. When set, uses this interval
   * instead of the global `expire` for worker stream cleanup.
   * @default undefined (falls back to `expire`)
   *
   * @example '90 days'
   */
  workerStreamsExpire?: string;

  /**
   * If true, strips execution-artifact attributes from completed,
   * un-pruned jobs. Preserves `jdata` (return data), `udata`
   * (searchable data), and `jmark` (timeline/event history for
   * workflow execution export). See `keepHmark` for `hmark`.
   * @default false
   */
  attributes?: boolean;

  /**
   * Entity allowlist. When provided, only jobs whose `entity` column
   * matches one of these values are eligible for pruning/stripping.
   * Jobs with `entity IS NULL` are excluded unless `pruneTransient`
   * is also true.
   * @default undefined (all entities)
   */
  entities?: string[];

  /**
   * If true, hard-deletes expired jobs where `entity IS NULL`
   * (transient workflow runs). Must also satisfy the retention
   * window (`expire`).
   * @default false
   */
  pruneTransient?: boolean;

  /**
   * If true, `hmark` attributes are preserved during stripping
   * (along with `jdata`, `udata`, and `jmark`). If false, `hmark`
   * rows are stripped.
   * @default false
   */
  keepHmark?: boolean;
}

/**
 * Result returned by `DBA.prune()`, providing deletion
 * counts for observability and logging.
 */
export interface PruneResult {
  /** Number of expired job rows hard-deleted */
  jobs: number;
  /** Number of expired stream message rows hard-deleted (engine + worker) */
  streams: number;
  /** Number of expired engine_streams rows hard-deleted */
  engineStreams: number;
  /** Number of expired worker_streams rows hard-deleted */
  workerStreams: number;
  /** Number of execution-artifact attribute rows stripped from completed jobs */
  attributes: number;
  /** Number of transient (entity IS NULL) job rows hard-deleted */
  transient: number;
  /** Number of jobs marked as pruned (pruned_at set) */
  marked: number;
}
