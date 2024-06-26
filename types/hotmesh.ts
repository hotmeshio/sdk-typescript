import { ILogger } from '../services/logger';
import { HotMeshService } from '../services/hotmesh';

import { HookRules } from './hook';
import { RedisClass, RedisClient, RedisOptions } from './redis';
import { StreamData, StreamDataResponse } from './stream';
import { LogLevel } from './logger';

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

type RedisConfig = {
  class: Partial<RedisClass>;
  options: Partial<RedisOptions>;
};

type HotMeshEngine = {
  store?: RedisClient; //set by hotmesh using instanced `redis` class
  stream?: RedisClient; //set by hotmesh using instanced `redis` class
  sub?: RedisClient; //set by hotmesh using instanced `redis` class
  redis?: RedisConfig;
  reclaimDelay?: number; //milliseconds
  reclaimCount?: number;
};

type HotMeshWorker = {
  topic: string;
  store?: RedisClient; //set by hotmesh using instanced `redis` class
  stream?: RedisClient; //set by hotmesh using instanced `redis` class
  sub?: RedisClient; //set by hotmesh using instanced `redis` class
  redis?: RedisConfig;
  reclaimDelay?: number; //milliseconds
  reclaimCount?: number; //max number of times to reclaim a stream
  callback: (payload: StreamData) => Promise<StreamDataResponse | void>;
};

type HotMeshConfig = {
  appId: string;
  namespace?: string;
  name?: string;
  logger?: ILogger;
  logLevel?: LogLevel;
  engine?: HotMeshEngine;
  workers?: HotMeshWorker[];
};

type HotMeshGraph = {
  subscribes: string;
  publishes?: string;
  expire?: number;
  output?: {
    schema: Record<string, any>;
  };
  input?: {
    schema: Record<string, any>;
  };
  activities: Record<string, any>;
  transitions?: Record<string, any>;
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
  RedisConfig,
  HotMeshWorker,
  HotMeshSettings,
  HotMeshApp, //a single app in the db
  HotMeshApps, //object array of all apps in the db
  HotMeshConfig, //customer config
  HotMeshManifest,
  HotMeshGraph,
  KeyType,
  KeyStoreParams,
};
