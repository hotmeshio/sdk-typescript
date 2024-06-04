import { HotMeshService } from '../hotmesh';
import { ContextType } from '../../types/durable';

import { ClientService } from './client';
import { ConnectionService } from './connection';
import { Search } from './search';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';

export const Durable = {
  Client: ClientService,
  Connection: ConnectionService,
  Search,
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
  },
};

export type { ContextType };
