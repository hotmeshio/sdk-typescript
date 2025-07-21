import { HotMesh } from '../hotmesh';
import { ContextType, WorkflowInterceptor } from '../../types/memflow';

import { ClientService } from './client';
import { ConnectionService } from './connection';
import { Search } from './search';
import { Entity } from './entity';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { WorkflowHandleService } from './handle';
import { didInterrupt } from './workflow/interruption';
import { InterceptorService } from './interceptor';

/**
 * The MemFlow service provides a Temporal-compatible workflow framework backed by
 * Postgres. It offers durable execution, entity-based memory management,
 * and composable workflows.
 *
 * ## Core Features
 *
 * ### 1. Entity-Based Memory Model
 * Each workflow has a durable JSONB entity that serves as its memory:
 * ```typescript
 * export async function researchAgent(query: string) {
 *   const agent = await MemFlow.workflow.entity();
 *
 *   // Initialize entity state
 *   await agent.set({
 *     query,
 *     findings: [],
 *     status: 'researching'
 *   });
 *
 *   // Update state atomically
 *   await agent.merge({ status: 'analyzing' });
 *   await agent.append('findings', newFinding);
 * }
 * ```
 *
 * ### 2. Hook Functions & Workflow Coordination
 * Spawn and coordinate multiple perspectives/phases:
 * ```typescript
 * // Launch parallel research perspectives
 * await MemFlow.workflow.execHook({
 *   taskQueue: 'research',
 *   workflowName: 'optimisticView',
 *   args: [query],
 *   signalId: 'optimistic-complete'
 * });
 *
 * await MemFlow.workflow.execHook({
 *   taskQueue: 'research',
 *   workflowName: 'skepticalView',
 *   args: [query],
 *   signalId: 'skeptical-complete'
 * });
 *
 * // Wait for both perspectives
 * await Promise.all([
 *   MemFlow.workflow.waitFor('optimistic-complete'),
 *   MemFlow.workflow.waitFor('skeptical-complete')
 * ]);
 * ```
 *
 * ### 3. Durable Activities & Proxies
 * Define and execute durable activities with automatic retry:
 * ```typescript
 * const activities = MemFlow.workflow.proxyActivities<{
 *   analyzeDocument: typeof analyzeDocument;
 *   validateFindings: typeof validateFindings;
 * }>({
 *   activities: { analyzeDocument, validateFindings },
 *   retryPolicy: {
 *     maximumAttempts: 3,
 *     backoffCoefficient: 2
 *   }
 * });
 *
 * // Activities are durable and automatically retried
 * const analysis = await activities.analyzeDocument(data);
 * const validation = await activities.validateFindings(analysis);
 * ```
 *
 * ### 4. Workflow Composition
 * Build complex workflows through composition:
 * ```typescript
 * // Start a child workflow
 * const childResult = await MemFlow.workflow.execChild({
 *   taskQueue: 'analysis',
 *   workflowName: 'detailedAnalysis',
 *   args: [data],
 *   // Child workflow config
 *   config: {
 *     maximumAttempts: 5,
 *     backoffCoefficient: 2
 *   }
 * });
 *
 * // Fire-and-forget child workflow
 * await MemFlow.workflow.startChild({
 *   taskQueue: 'notifications',
 *   workflowName: 'sendUpdates',
 *   args: [updates]
 * });
 * ```
 *
 * ### 5. Workflow Interceptors
 * Add cross-cutting concerns through interceptors that run as durable functions:
 * ```typescript
 * // Add audit interceptor that uses MemFlow functions
 * MemFlow.registerInterceptor({
 *   async execute(ctx, next) {
 *     try {
 *       // Interceptors can use MemFlow functions and participate in replay
 *       const entity = await MemFlow.workflow.entity();
 *       await entity.append('auditLog', {
 *         action: 'started',
 *         timestamp: new Date().toISOString()
 *       });
 *
 *       // Rate limiting with durable sleep
 *       await MemFlow.workflow.sleepFor('100 milliseconds');
 *
 *       const result = await next();
 *
 *       await entity.append('auditLog', {
 *         action: 'completed',
 *         timestamp: new Date().toISOString()
 *       });
 *
 *       return result;
 *     } catch (err) {
 *       // CRITICAL: Always check for HotMesh interruptions
 *       if (MemFlow.didInterrupt(err)) {
 *         throw err; // Rethrow for replay system
 *       }
 *       throw err;
 *     }
 *   }
 * });
 * ```
 *
 * ## Basic Usage Example
 *
 * ```typescript
 * import { Client, Worker, MemFlow } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * // Initialize worker
 * await Worker.create({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   taskQueue: 'default',
 *   workflow: workflows.example
 * });
 *
 * // Initialize client
 * const client = new Client({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   }
 * });
 *
 * // Start workflow
 * const handle = await client.workflow.start({
 *   args: ['input data'],
 *   taskQueue: 'default',
 *   workflowName: 'example',
 *   workflowId: MemFlow.guid()
 * });
 *
 * // Get result
 * const result = await handle.result();
 *
 * // Cleanup
 * await MemFlow.shutdown();
 * ```
 */
class MemFlowClass {
  /**
   * @private
   */
  constructor() {}
  /**
   * The MemFlow `Client` service is functionally
   * equivalent to the Temporal `Client` service.
   */
  static Client: typeof ClientService = ClientService;

  /**
   * The MemFlow `Connection` service is functionally
   * equivalent to the Temporal `Connection` service.
   */
  static Connection: typeof ConnectionService = ConnectionService;

  /**
   * @private
   */
  static Search: typeof Search = Search;

  /**
   * @private
   */
  static Entity: typeof Entity = Entity;

  /**
   * The Handle provides methods to interact with a running
   * workflow. This includes exporting the workflow, sending signals, and
   * querying the state of the workflow. An instance of the Handle service
   * is typically accessed via the MemFlow.Client class (workflow.getHandle).
   */
  static Handle: typeof WorkflowHandleService = WorkflowHandleService;

  /**
   * The MemFlow `Worker` service is functionally
   * equivalent to the Temporal `Worker` service.
   */
  static Worker: typeof WorkerService = WorkerService;

  /**
   * The MemFlow `workflow` service is functionally
   * equivalent to the Temporal `Workflow` service
   * with additional methods for managing workflows,
   * including: `execChild`, `waitFor`, `sleep`, etc
   */
  static workflow: typeof WorkflowService = WorkflowService;

  /**
   * Checks if an error is a HotMesh reserved error type that indicates
   * a workflow interruption rather than a true error condition.
   *
   * @see {@link utils/interruption.didInterrupt} for detailed documentation
   */
  static didInterrupt = didInterrupt;

  private static interceptorService = new InterceptorService();

  /**
   * Register a workflow interceptor
   * @param interceptor The interceptor to register
   */
  static registerInterceptor(interceptor: WorkflowInterceptor): void {
    MemFlowClass.interceptorService.register(interceptor);
  }

  /**
   * Clear all registered workflow interceptors
   */
  static clearInterceptors(): void {
    MemFlowClass.interceptorService.clear();
  }

  /**
   * Get the interceptor service instance
   * @internal
   */
  static getInterceptorService(): InterceptorService {
    return MemFlowClass.interceptorService;
  }

  /**
   * Shutdown everything. All connections, workers, and clients will be closed.
   * Include in your signal handlers to ensure a clean shutdown.
   */
  static async shutdown(): Promise<void> {
    await MemFlowClass.Client.shutdown();
    await MemFlowClass.Worker.shutdown();
    await HotMesh.stop();
  }
}

export { MemFlowClass as MemFlow };
export type { ContextType };
