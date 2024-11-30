import { HotMesh } from './services/hotmesh';
import { HotMeshConfig } from './types/hotmesh';
import { MeshCall } from './services/meshcall';
import { MeshFlow } from './services/meshflow';
import { WorkflowHandleService as WorkflowHandle } from './services/meshflow/handle';
import { MeshData } from './services/meshdata';
import { MeshOS } from './services/meshos';
import * as Errors from './modules/errors';
import * as Utils from './modules/utils';
import { ConnectorService as Connector } from './services/connector/factory';
import { PostgresConnection as ConnectorPostgres } from './services/connector/providers/postgres';
import { RedisConnection as ConnectorIORedis } from './services/connector/providers/ioredis';
import { RedisConnection as ConnectorRedis } from './services/connector/providers/redis';
import { NatsConnection as ConnectorNATS } from './services/connector/providers/nats';

const { Client, Connection, Search, Worker, workflow } = MeshFlow;

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
  MeshFlow,
  MeshOS,

  //MeshFlow Submodules
  Client,
  Connection,
  Search,
  Worker,
  workflow,
  WorkflowHandle,

  //Global Modules
  Errors,
  Utils,
};

export * as Types from './types';
