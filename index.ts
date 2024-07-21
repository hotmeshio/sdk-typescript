import { MeshCall } from './services/meshcall';
import { MeshData } from './services/meshdata';
import { HotMesh } from './services/hotmesh';
import { ClientService } from './services/meshflow/client';
import { WorkerService } from './services/meshflow/worker';
import { WorkflowService } from './services/meshflow/workflow';
import { ConnectionService } from './services/meshflow/connection';
import { HotMeshConfig } from './types/hotmesh';

const MeshFlow = {
  Client: ClientService,
  Worker: WorkerService,
  workflow: WorkflowService,
  Connection: ConnectionService,
};

export {
  HotMesh,
  MeshCall,
  MeshData,
  MeshFlow,
  HotMeshConfig,
};

export * as Types from './types';
