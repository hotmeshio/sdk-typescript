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
   * attribute rows.
   * @default true
   */
  jobs?: boolean;

  /**
   * If true, hard-deletes expired stream messages older than the
   * retention window.
   * @default true
   */
  streams?: boolean;

  /**
   * If true, strips execution-artifact attributes (`adata`, `hmark`,
   * `jmark`, `status`, `other`) from completed jobs (status = 0),
   * retaining only `jdata` (workflow return data) and `udata`
   * (user-searchable data).
   * @default false
   */
  attributes?: boolean;
}

/**
 * Result returned by `DBA.prune()`, providing deletion
 * counts for observability and logging.
 */
export interface PruneResult {
  /** Number of expired job rows hard-deleted */
  jobs: number;
  /** Number of expired stream message rows hard-deleted */
  streams: number;
  /** Number of execution-artifact attribute rows stripped from completed jobs */
  attributes: number;
}
