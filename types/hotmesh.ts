import { ILogger } from '../services/logger';
import { HotMesh as HotMeshService } from '../services/hotmesh';

import { HookRules } from './hook';
import { StreamData, StreamDataResponse } from './stream';
import { LogLevel } from './logger';
import { ProviderClient, ProviderConfig, ProvidersConfig } from './provider';

/**
 * Scout role types for distributed task processing.
 * At any given time, only a single engine will hold a scout role
 * to check for and process work items in the respective queues.
 */
type ScoutType = 'time' | 'signal' | 'activate' | 'router';

/**
 * the full set of entity types that are stored in the key/value store
 */
enum KeyType {
  APP = 'APP',
  THROTTLE_RATE = 'THROTTLE_RATE',
  HOOKS = 'HOOKS',
  JOB_DEPENDENTS = 'JOB_DEPENDENTS',
  JOB_STATE = 'JOB_STATE',
  JOB_STATS_GENERAL = 'JOB_STATS_GENERAL',
  JOB_STATS_MEDIAN = 'JOB_STATS_MEDIAN',
  JOB_STATS_INDEX = 'JOB_STATS_INDEX',
  HOTMESH = 'HOTMESH',
  QUORUM = 'QUORUM',
  SCHEMAS = 'SCHEMAS',
  SIGNALS = 'SIGNALS',
  STREAMS = 'STREAMS',
  SUBSCRIPTIONS = 'SUBSCRIPTIONS',
  SUBSCRIPTION_PATTERNS = 'SUBSCRIPTION_PATTERNS',
  SYMKEYS = 'SYMKEYS',
  SYMVALS = 'SYMVALS',
  TIME_RANGE = 'TIME_RANGE',
  WORK_ITEMS = 'WORK_ITEMS',
}

/**
 * minting keys, requires one or more of the following parameters
 */
type KeyStoreParams = {
  appId?: string; //app id is a uuid for a hotmesh app
  engineId?: string; //unique auto-generated guid for an ephemeral engine instance
  appVersion?: string; //(e.g. "1.0.0", "1", "1.0")
  jobId?: string; //a customer-defined id for job; must be unique for the entire app
  activityId?: string; //activity id is a uuid for a given hotmesh app
  jobKey?: string; //a customer-defined label for a job that serves to categorize events
  dateTime?: string; //UTC date time: YYYY-MM-DDTHH:MM (20203-04-12T00:00); serves as a time-series bucket for the job_key
  facet?: string; //data path starting at root with values separated by colons (e.g. "object/type:bar")
  topic?: string; //topic name (e.g., "foo" or "" for top-level)
  timeValue?: number; //time value (rounded to minute) (for delete range)
  scoutType?: ScoutType; //a single member of the quorum serves as the 'scout' for the group, triaging tasks for the collective
};

type HotMesh = typeof HotMeshService;

type HotMeshEngine = {
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  store?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  stream?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  sub?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * AND if the provider requires a separate channel for publishing
   * @private
   */
  pub?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  search?: ProviderClient;

  /**
   * short-form format for the connection options for the
   * store, stream, sub, and search clients
   */
  connection?: ProviderConfig | ProvidersConfig;

  /**
   * long-form format for the connection options for the
   * store, stream, sub, and search clients
   */
  // connections?: {
  //   store: ProviderConfig;
  //   stream: ProviderConfig;
  //   sub: ProviderConfig;
  //   pub?: ProviderConfig; //system injects if necessary (if store channel cannot be used for pub)
  //   search?: ProviderConfig; //inherits from store if not set
  // };

  /**
   * the number of milliseconds to wait before reclaiming a stream;
   * depending upon the provider this may be an explicit retry event,
   * consuming a message from the stream and re-queueing it (dlq, etc),
   * or it may be a configurable delay before the provider exposes the
   * message to the consumer again. It is up to the provider, but expressed
   * in milliseconds here.
   */
  reclaimDelay?: number;

  /**
   * the number of times to reclaim a stream before giving up
   * and moving the message to a dead-letter queue or other
   * error handling mechanism
   */
  reclaimCount?: number;

  /**
   * if true, the engine will not route stream messages
   * to the worker
   * @default false
   */
  readonly?: boolean;

  /**
   * Task queue identifier used for connection pooling optimization.
   * When provided, connections will be reused across providers (store, sub, stream)
   * that share the same task queue and database configuration.
   */
  taskQueue?: string;

  /**
   * Retry policy for stream messages. Configures automatic retry
   * behavior with exponential backoff for failed operations.
   * Applied to the stream connection during initialization.
   * 
   * @example
   * ```typescript
   * {
   *   retry: {
   *     maximumAttempts: 5,
   *     backoffCoefficient: 2,
   *     maximumInterval: '300s'
   *   }
   * }
   * ```
   */
  retry?: import('./stream').RetryPolicy;
};

/**
 * Configuration for a HotMesh worker that consumes messages from a
 * Postgres stream topic. Workers can run on the same process as the
 * engine or on entirely separate servers — the only coupling is the
 * shared Postgres database.
 *
 * ## Connection Modes
 *
 * **Standard**: Worker uses the same Postgres credentials as the engine.
 * Set `connection` with admin credentials. Suitable for trusted, co-located workers.
 *
 * **Secured**: Worker connects as a restricted Postgres role with
 * `workerCredentials`. The role can only dequeue/ack/respond on its
 * allowed stream topics via SECURITY DEFINER stored procedures — zero
 * direct table access. Use for untrusted workloads: K8s containers,
 * LLM agents, third-party integrations.
 */
type HotMeshWorker = {
  /**
   * the topic/task queue that the worker subscribes to (stream_name)
   */
  topic: string;

  /**
   * the workflow function name for dispatch routing (workflow_name column).
   * When set, workers sharing the same topic use a singleton consumer
   * that fetches batches and dispatches by workflowName.
   */
  workflowName?: string;

  /**
   * set by hotmesh once the connnector service instances the provider
   * AND if the provider requires a separate channel for publishing
   * @private
   */
  pub?: ProviderClient;

  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  store?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  stream?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  sub?: ProviderClient;
  /**
   * set by hotmesh once the connnector service instances the provider
   * @private
   */
  search?: ProviderClient;

  /**
   * short-form format for the connection options for the
   * store, stream, sub, and search clients
   */
  connection?: ProviderConfig | ProvidersConfig;

  /**
   * long-form format for the connection options for the
   * store, stream, sub, and search clients
   */
  // connections?: {
  //   store: ProviderConfig;
  //   stream: ProviderConfig;
  //   sub: ProviderConfig;
  //   pub?: ProviderConfig; //if store channel cannot be used for pub
  //   search?: ProviderConfig; //inherits from store if not set
  // };

  /**
   * the number of milliseconds to wait before reclaiming a stream;
   * depending upon the provider this may be an explicit retry event,
   * consuming a message from the stream and re-queueing it (dlq, etc),
   * or it may be a configurable delay before the provider exposes the
   * message to the consumer again. It is up to the provider, but expressed
   * in milliseconds here.
   */
  reclaimDelay?: number;

  /**
   * the number of times to reclaim a stream before giving up
   * and moving the message to a dead-letter queue or other
   * error handling mechanism
   */
  reclaimCount?: number;

  /**
   * The callback function to execute when a message is dequeued
   * from the target stream
   */
  callback: (payload: StreamData) => Promise<StreamDataResponse>;

  /**
   * Task queue identifier used for connection pooling optimization.
   * When provided, connections will be reused across providers (store, sub, stream)
   * that share the same task queue and database configuration.
   */
  taskQueue?: string;

  /**
   * Retry policy for stream messages. Configures automatic retry
   * behavior with exponential backoff for failed operations.
   * Applied to the stream connection during initialization.
   * 
   * @example
   * ```typescript
   * {
   *   retry: {
   *     maximumAttempts: 5,
   *     backoffCoefficient: 2,
   *     maximumInterval: '300s'
   *   }
   * }
   * ```
   */
  retry?: import('./stream').RetryPolicy;

  /**
   * Scoped Postgres credentials for database-level worker isolation.
   *
   * When provided, the `user` and `password` in the worker's connection
   * options are overridden with these values, and all stream operations
   * route through SECURITY DEFINER stored procedures that validate
   * `app.allowed_streams` before executing. The worker role has **zero
   * direct table access**.
   *
   * Use this for workers running in untrusted environments: pluggable
   * K8s containers, LLM-driven agents (e.g., MCP tool servers),
   * third-party integrations, or any workload that should be isolated
   * from the engine's database surface.
   *
   * Provision credentials via `HotMesh.provisionWorkerRole()` or by
   * creating a Postgres role with `EXECUTE` on the schema's worker
   * stored procedures and `ALTER ROLE ... SET app.allowed_streams`.
   *
   * **Limitations:** Workflows running under scoped credentials cannot
   * use `Durable.workflow.entity()`, `Durable.workflow.search()`, or
   * `Durable.workflow.enrich()` — these write directly to the `jobs`
   * and `jobs_attributes` tables, which the scoped role has no access
   * to. All other workflow primitives (`proxyActivities`, `sleep`,
   * `condition`, `signal`, `emit`, `executeChild`, etc.) work normally.
   *
   * @example
   * ```typescript
   * // Admin provisions scoped credentials (one-time)
   * const cred = await HotMesh.provisionWorkerRole({
   *   connection: { class: Postgres, options: adminOptions },
   *   streamNames: ['order.process'],
   * });
   *
   * // Worker connects with restricted role
   * workers: [{
   *   topic: 'order.process',
   *   connection: { class: Postgres, options: { host: 'pg.prod', database: 'db' } },
   *   workerCredentials: { user: cred.roleName, password: cred.password },
   *   callback: myCallback,
   * }]
   * ```
   */
  workerCredentials?: { user: string; password: string };

  /**
   * If true, the worker's router will not consume messages from the
   * stream. The worker can still publish responses but will never
   * dequeue or process messages. This is inherited from the
   * connection's `readonly` flag by the Durable layer.
   * @default false
   */
  readonly?: boolean;
};

type HotMeshConfig = {
  appId: string;
  namespace?: string;
  name?: string;
  guid?: string;
  logger?: ILogger;
  logLevel?: LogLevel;
  /**
   * Task queue identifier used for connection pooling optimization.
   * When multiple engines/workers share the same task queue and database configuration,
   * they will reuse the same connection instead of creating separate ones.
   * This is particularly useful for PostgreSQL providers to reduce connection overhead.
   */
  taskQueue?: string;
  engine?: HotMeshEngine;
  workers?: HotMeshWorker[];
};

type HotMeshGraph = {
  /**
   * the unique topic that the graph subscribes to, creating one
   * job for each idempotent message that is received
   */
  subscribes: string;
  /**
   * the unique topic that the graph publishes/emits to when the job completes
   */
  publishes?: string;
  /**
   * the number of seconds that the completed job should be
   * left in the store before it is deleted
   */
  expire?: number;
  /**
   * if the graph is reentrant and has open activities, the
   * `persistent` flag will emit the job completed  event.
   * This allows the 'main' thread/trigger that started the job to
   * signal to subscribers (or the parent) that the job
   * is 'done', while still leaving the job in a
   * state that allows for reentry (such as cyclical hooks).
   */
  persistent?: boolean;
  /**
   * the schema for the output of the graph
   */
  output?: {
    schema: Record<string, any>;
  };
  /**
   * the schema for the input of the graph
   */
  input?: {
    schema: Record<string, any>;
  };
  /**
   * the activities that define the graph
   */
  activities: Record<string, any>;
  /**
   * the transitions that define how activities are connected
   */
  transitions?: Record<string, any>;
  /**
   * the reentrant hook rules that define how to reenter a running graph
   */
  hooks?: HookRules;
};

type HotMeshSettings = {
  namespace: string;
  version: string;
};

type HotMeshManifest = {
  app: {
    id: string;
    version: string;
    settings: Record<string, any>;
    graphs: HotMeshGraph[];
  };
};

type VersionedFields = {
  [K in `versions/${string}`]: any;
};

type HotMeshApp = VersionedFields & {
  id: string; // customer's chosen app id
  version: string; // customer's chosen version scheme (semver, etc)
  settings?: string; // stringified JSON for app settings
  active?: boolean; // is the app active?
};

type HotMeshApps = {
  [appId: string]: HotMeshApp;
};

export {
  HotMesh,
  HotMeshEngine,
  HotMeshWorker,
  HotMeshSettings,
  HotMeshApp,
  HotMeshApps,
  HotMeshConfig,
  HotMeshManifest,
  HotMeshGraph,
  KeyType,
  KeyStoreParams,
  ScoutType,
  VersionedFields,
};
