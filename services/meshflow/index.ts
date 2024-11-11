import { HotMesh } from '../hotmesh';
import { ContextType } from '../../types/meshflow';

import { ClientService } from './client';
import { ConnectionService } from './connection';
import { Search } from './search';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { WorkflowHandleService } from './handle';

/**
 * The MeshFlow service is a collection of services that
 * emulate Temporal's capabilities, but instead are
 * backed by Redis, ValKey, DragonflyDB, etc (whatever
 * your chosen backend). The following lifecycle example
 * demonstrates how to start a new workflow, subscribe
 * to the result, and shutdown the system.
 * @example
 * ```typescript
 * import { Client, Worker, MeshFlow, HotMesh } from '@hotmeshio/hotmesh';
 * import * as Redis from 'redis';
 * import * as workflows from './workflows';
 *
 * //1) Initialize the worker
 * await Worker.create({
 *   connection: {
 *     class: Redis,
 *     options: { url: 'redis://:key_admin@redis:6379' }
 *   },
 *   taskQueue: 'default',
 *   namespace: 'meshflow',
 *   workflow: workflows.example,
 *   options: {
 *     backoffCoefficient: 2,
 *     maximumAttempts: 1_000,
 *     maximumInterval: '5 seconds'
 *   }
 * });
 *
 * //2) initialize the client
 * const client = new Client({
 *   connection: {
 *     class: Redis,
 *     options: { url: 'redis://:key_admin@redis:6379' }
 *   }
 * });
 *
 * //3) start a new workflow
 * const handle = await client.workflow.start({
 *   args: ['HotMesh', 'es'],
 *   taskQueue: 'default',
 *   workflowName: 'example',
 *   workflowId: HotMesh.guid(),
 *   namespace: 'meshflow', //the app name in Redis
 * });
 *
 * //4) subscribe to the eventual result
 * console.log('\nRESPONSE', await handle.result(), '\n');
 * //logs '¡Hola, HotMesh!'
 *
 * //5) Shutdown (typically on sigint)
 * await MeshFlow.shutdown();
 * ```
 */
class MeshFlowClass {
  /**
   * @private
   */
  constructor() {}
  /**
   * The MeshFlow `Client` service is functionally
   * equivalent to the Temporal `Client` service.
   */
  static Client = ClientService;

  /**
   * The MeshFlow `Connection` service is functionally
   * equivalent to the Temporal `Connection` service.
   */
  static Connection = ConnectionService;

  /**
   * @private
   */
  static Search = Search;

  /**
   * The Handle provides methods to interact with a running
   * workflow. This includes exporting the workflow, sending signals, and
   * querying the state of the workflow. An instance of the Handle service
   * is typically accessed via the MeshFlow.Client class (workflow.getHandle).
   */
  static Handle = WorkflowHandleService;

  /**
   * The MeshFlow `Worker` service is functionally
   * equivalent to the Temporal `Worker` service.
   */
  static Worker = WorkerService;

  /**
   * The MeshFlow `workflow` service is functionally
   * equivalent to the Temporal `Workflow` service
   * with additional methods for managing workflows,
   * including: `execChild`, `waitFor`, `sleep`, etc
   */
  static workflow = WorkflowService;

  /**
   * Shutdown everything. All connections, workers, and clients will be closed.
   * Include in your signal handlers to ensure a clean shutdown.
   */
  static async shutdown(): Promise<void> {
    await MeshFlowClass.Client.shutdown();
    await MeshFlowClass.Worker.shutdown();
    await HotMesh.stop();
  }
}

export { MeshFlowClass as MeshFlow };
export type { ContextType };
