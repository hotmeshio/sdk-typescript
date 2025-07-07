import { HotMesh } from './services/hotmesh';
import { HotMeshConfig } from './types/hotmesh';
import { MeshCall } from './services/meshcall';
import { MemFlow } from './services/memflow';
import { ClientService as Client } from './services/memflow/client';
import { ConnectionService as Connection } from './services/memflow/connection';
import { Search } from './services/memflow/search';
import { WorkerService as Worker } from './services/memflow/worker';
import { WorkflowService as workflow } from './services/memflow/workflow';
import { WorkflowHandleService as WorkflowHandle } from './services/memflow/handle';
import { proxyActivities } from './services/memflow/workflow/proxyActivities';
import { MeshData } from './services/meshdata';
import { MeshOS } from './services/meshos';
import * as Errors from './modules/errors';
import * as Utils from './modules/utils';
import * as Enums from './modules/enums';
import * as KeyStore from './modules/key';
import { ConnectorService as Connector } from './services/connector/factory';
import { PostgresConnection as ConnectorPostgres } from './services/connector/providers/postgres';
import { RedisConnection as ConnectorIORedis } from './services/connector/providers/ioredis';
import { RedisConnection as ConnectorRedis } from './services/connector/providers/redis';
import { NatsConnection as ConnectorNATS } from './services/connector/providers/nats';

//const { Client, Connection, Search, Worker, workflow } = MemFlow;

export {
  //Provider Connectors
  Connector, //factory
  ConnectorIORedis,
  ConnectorNATS,
  ConnectorPostgres,
  ConnectorRedis,

  //Top-level Modules
  HotMesh,
  HotMeshConfig,
  MeshCall,
  MeshData,
  MemFlow,
  MeshOS,

  //MemFlow Submodules
  Client,
  Connection,
  proxyActivities,
  Search,
  Worker,
  workflow,
  WorkflowHandle,

  //Global Modules
  Enums,
  Errors,
  Utils,
  KeyStore,
};

export * as Types from './types';
