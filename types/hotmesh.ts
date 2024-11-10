import { ILogger } from '../services/logger';
import { HotMesh as HotMeshService } from '../services/hotmesh';

import { HookRules } from './hook';
import { RedisClass, RedisClient, RedisOptions } from './redis';
import { StreamData, StreamDataResponse } from './stream';
import { LogLevel } from './logger';
import { StringAnyType } from './serializer';

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
  scoutType?: 'signal' | 'time' | 'activate'; //a single member of the quorum serves as the 'scout' for the group, triaging tasks for the collective
};

type HotMesh = typeof HotMeshService;

type Providers = 'redis' | 'nats' | 'postgres' | 'ioredis';

type ConnectionConfig = {
  class: any;
  options: StringAnyType;
};
interface RedisConfig extends ConnectionConfig {
  class: Partial<RedisClass>;
  options: Partial<RedisOptions>;
};

type HotMeshEngine = {
  store?: RedisClient; //set by hotmesh using instanced `redis` class
  stream?: RedisClient; //set by hotmesh using instanced `redis` class
  sub?: RedisClient; //set by hotmesh using instanced `redis` class
  redis?: RedisConfig;
  connection?: ConnectionConfig;
  connections?: {
    store: ConnectionConfig;
    stream: ConnectionConfig;
    sub: ConnectionConfig;
  };
  reclaimDelay?: number; //milliseconds
  reclaimCount?: number;
  readonly?: boolean; //if true, the engine will not route stream messages
};

type HotMeshWorker = {
  topic: string;
  store?: RedisClient; //set by hotmesh using instanced `redis` class
  stream?: RedisClient; //set by hotmesh using instanced `redis` class
  sub?: RedisClient; //set by hotmesh using instanced `redis` class
  redis?: RedisConfig;
  connection?: ConnectionConfig;
  connections?: {
    store: ConnectionConfig;
    stream: ConnectionConfig;
    sub: ConnectionConfig;
  };
  reclaimDelay?: number; //milliseconds
  reclaimCount?: number; //max number of times to reclaim a stream
  callback: (payload: StreamData) => Promise<StreamDataResponse>;
};

type HotMeshConfig = {
  appId: string;
  namespace?: string;
  name?: string;
  guid?: string;
  logger?: ILogger;
  logLevel?: LogLevel;
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

/**
 * A provider transaction is a set of operations that are executed
 * atomically by the provider. The transaction is created by calling
 * the `transact` method on the provider. The transaction object
 * contains methods specific to the provider allowing it to optionally
 * choose to execute a single command or collect all commands and
 * execute as a single transaction.
 */
interface ProviderTransaction {
  //outside callers can execute the transaction, regardless of provider by calling this method
  exec(): Promise<any>;

  // All other transaction methods are provider specific
  [key: string]: any;
}

interface ProviderClient {
  /**  The provider-specific transaction object */
  transact(): ProviderTransaction;

  /** Mint a provider-specific key */
  mintKey(type: KeyType, params: KeyStoreParams): string;

  /** The provider-specific client object */
  [key: string]: any;
}

type TransactionResultList = (string | number)[]; // e.g., [3, 2, '0']

export {
  ConnectionConfig,
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
  ProviderClient,
  Providers,
  ProviderTransaction,
  RedisConfig,
  TransactionResultList,
};
