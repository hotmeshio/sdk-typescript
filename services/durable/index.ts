import { ClientService } from './client';
import { ConnectionService } from './connection';
import { MeshOSService } from './meshos';
import { Search } from './search';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { ContextType } from '../../types/durable';

export const Durable = {
  Client: ClientService,
  Connection: ConnectionService,
  Search,
  MeshOS: MeshOSService,
  Worker: WorkerService,
  workflow: WorkflowService,
};

export type { ContextType };
