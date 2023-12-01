import { ClientService } from './client';
import { ConnectionService } from './connection';
import { MeshDBService } from './meshdb';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { ContextType } from '../../types/durable';

export const Durable = {
  Client: ClientService,
  Connection: ConnectionService,
  MeshDB: MeshDBService,
  Worker: WorkerService,
  workflow: WorkflowService,
};

export type { ContextType };
