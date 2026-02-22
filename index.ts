import { HotMesh } from './services/hotmesh';
import { HotMeshConfig } from './types/hotmesh';
import { Virtual } from './services/virtual';
import { Durable } from './services/durable';
import { DBA } from './services/dba';
import { ClientService as Client } from './services/durable/client';
import { ConnectionService as Connection } from './services/durable/connection';
import { Search } from './services/durable/search';
import { Entity } from './services/durable/entity';
import { WorkerService as Worker } from './services/durable/worker';
import { WorkflowService as workflow } from './services/durable/workflow';
import { WorkflowHandleService as WorkflowHandle } from './services/durable/handle';
import { proxyActivities } from './services/durable/workflow/proxyActivities';
import * as Errors from './modules/errors';
import * as Utils from './modules/utils';
import * as Enums from './modules/enums';
import * as KeyStore from './modules/key';
import { ConnectorService as Connector } from './services/connector/factory';
import { PostgresConnection as ConnectorPostgres } from './services/connector/providers/postgres';
import { NatsConnection as ConnectorNATS } from './services/connector/providers/nats';

export {
  //Provider Connectors
  Connector, //factory
  ConnectorNATS,
  ConnectorPostgres,

  //Top-level Modules
  HotMesh,
  HotMeshConfig,
  Virtual,
  Durable,
  DBA,

  //Durable Submodules
  Client,
  Connection,
  proxyActivities,
  Search,
  Entity,
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
