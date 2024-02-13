import { ClientService } from './client';
import { ConnectionService } from './connection';
import { MeshOSService } from './meshos';
import { Search } from './search';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { HotMeshService } from '../hotmesh';
import { ContextType } from '../../types/durable';

export const Durable = {
  Client: ClientService,
  Connection: ConnectionService,
  Search,
  MeshOS: MeshOSService,
  Worker: WorkerService,
  workflow: WorkflowService,

  /**
   * Shutdown everything. All connections, workers, and clients will be closed.
   * Include in your signal handlers to ensure a clean shutdown.
   */
  async shutdown(): Promise<void> {
    await ClientService.shutdown();
    await WorkerService.shutdown();
    await HotMeshService.stop();
  }
};

export type { ContextType };
