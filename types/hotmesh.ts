import { ILogger } from "../services/logger";
import { HotMeshService } from "../services/hotmesh";
import { HookRules } from "./hook";
import { RedisClass, RedisClient, RedisOptions } from "./redis";
import { StreamData, StreamDataResponse } from "./stream";

type HotMesh = typeof HotMeshService;

type RedisConfig = {
  class: RedisClass;
  options: RedisOptions;
}

type HotMeshEngine = {
  store?: RedisClient;  //set by hotmesh using instanced `redis` class
  stream?: RedisClient; //set by hotmesh using instanced `redis` class
  sub?: RedisClient;    //set by hotmesh using instanced `redis` class
  redis?: RedisConfig;
  reclaimDelay?: number; //milliseconds
  reclaimCount?: number;
}

type HotMeshWorker = {
  topic: string;
  store?: RedisClient;  //set by hotmesh using instanced `redis` class
  stream?: RedisClient; //set by hotmesh using instanced `redis` class
  sub?: RedisClient;    //set by hotmesh using instanced `redis` class
  redis?: {
    class: RedisClass;
    options: RedisOptions;
  };
  reclaimDelay?: number; //milliseconds
  reclaimCount?: number; //max number of times to reclaim a stream
  callback: (payload: StreamData) => Promise<StreamDataResponse|void>;
}

type HotMeshConfig = {
  appId: string;
  namespace?: string;
  name?: string;
  logger?: ILogger;
  logLevel?: 'silly' | 'debug' | 'info' | 'warn' | 'error' | 'silent';
  engine?: HotMeshEngine;
  workers?: HotMeshWorker[];
}

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
  id: string;        // customer's chosen app id
  version: string;   // customer's chosen version scheme (semver, etc)
  settings?: string; // stringified JSON for app settings
  active?: boolean;  // is the app active?
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
  HotMeshApp,    //a single app in the db
  HotMeshApps,   //object array of all apps in the db
  HotMeshConfig, //customer config
  HotMeshManifest,
  HotMeshGraph
};
