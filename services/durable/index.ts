import { HotMesh } from '../hotmesh';
import { ContextType, WorkflowInterceptor, ActivityInterceptor } from '../../types/durable';
import { guid } from '../../modules/utils';

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
 * The Durable service provides a Temporal-compatible workflow framework backed
 * by Postgres. It offers entity-based memory management and composable,
 * fault-tolerant workflows.
 *
 * ## Core Features
 *
 * ### 1. Entity-Based Memory Model
 * Each workflow has a durable JSONB entity that serves as its memory:
 * ```typescript
 * export async function researchAgent(query: string) {
 *   const agent = await Durable.workflow.entity();
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
 * await Durable.workflow.execHook({
 *   taskQueue: 'research',
 *   workflowName: 'optimisticView',
 *   args: [query],
 *   signalId: 'optimistic-complete'
 * });
 *
 * await Durable.workflow.execHook({
 *   taskQueue: 'research',
 *   workflowName: 'skepticalView',
 *   args: [query],
 *   signalId: 'skeptical-complete'
 * });
 *
 * // Wait for both perspectives
 * await Promise.all([
 *   Durable.workflow.waitFor('optimistic-complete'),
 *   Durable.workflow.waitFor('skeptical-complete')
 * ]);
 * ```
 *
 * ### 3. Durable Activities & Proxies
 * Define and execute durable activities with automatic retry:
 * ```typescript
 * // Default: activities use workflow's task queue
 * const activities = Durable.workflow.proxyActivities<{
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
 * ### 4. Explicit Activity Registration
 * Register activity workers explicitly before workflows start:
 * ```typescript
 * // Register shared activity pool for interceptors
 * await Durable.registerActivityWorker({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   taskQueue: 'shared-activities'
 * }, sharedActivities, 'shared-activities');
 *
 * // Register custom activity pool for specific use cases
 * await Durable.registerActivityWorker({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   taskQueue: 'priority-activities'
 * }, priorityActivities, 'priority-activities');
 * ```
 *
 * ### 5. Workflow Composition
 * Build complex workflows through composition:
 * ```typescript
 * // Start a child workflow
 * const childResult = await Durable.workflow.execChild({
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
 * await Durable.workflow.startChild({
 *   taskQueue: 'notifications',
 *   workflowName: 'sendUpdates',
 *   args: [updates]
 * });
 * ```
 *
 * ### 6. Workflow Interceptors
 * Add cross-cutting concerns through interceptors that run as durable functions:
 * ```typescript
 * // First register shared activity worker for interceptors
 * await Durable.registerActivityWorker({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   taskQueue: 'interceptor-activities'
 * }, { auditLog }, 'interceptor-activities');
 *
 * // Add audit interceptor that uses activities with explicit taskQueue
 * Durable.registerInterceptor({
 *   async execute(ctx, next) {
 *     try {
 *       // Interceptors use explicit taskQueue to prevent per-workflow queues
 *       const { auditLog } = Durable.workflow.proxyActivities<typeof activities>({
 *         activities: { auditLog },
 *         taskQueue: 'interceptor-activities', // Explicit shared queue
 *         retryPolicy: { maximumAttempts: 3 }
 *       });
 *
 *       await auditLog(ctx.get('workflowId'), 'started');
 *
 *       const result = await next();
 *
 *       await auditLog(ctx.get('workflowId'), 'completed');
 *
 *       return result;
 *     } catch (err) {
 *       // CRITICAL: Always check for HotMesh interruptions
 *       if (Durable.didInterrupt(err)) {
 *         throw err; // Rethrow for replay system
 *       }
 *       throw err;
 *     }
 *   }
 * });
 * ```
 *
 * ### 7. Activity Interceptors
 * Wrap individual proxied activity calls with cross-cutting logic.
 * Unlike workflow interceptors (which wrap the entire workflow), activity
 * interceptors execute around each `proxyActivities` call. They run inside
 * the workflow's execution context and have access to all Durable workflow
 * methods (`proxyActivities`, `sleepFor`, `waitFor`, `execChild`, etc.).
 * Multiple activity interceptors execute in onion order (first registered
 * is outermost).
 * ```typescript
 * // Simple logging interceptor
 * Durable.registerActivityInterceptor({
 *   async execute(activityCtx, workflowCtx, next) {
 *     console.log(`Activity ${activityCtx.activityName} starting`);
 *     try {
 *       const result = await next();
 *       console.log(`Activity ${activityCtx.activityName} completed`);
 *       return result;
 *     } catch (err) {
 *       if (Durable.didInterrupt(err)) throw err;
 *       console.error(`Activity ${activityCtx.activityName} failed`);
 *       throw err;
 *     }
 *   }
 * });
 *
 * // Interceptor that calls its own proxy activities
 * await Durable.registerActivityWorker({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   taskQueue: 'audit-activities'
 * }, { auditLog }, 'audit-activities');
 *
 * Durable.registerActivityInterceptor({
 *   async execute(activityCtx, workflowCtx, next) {
 *     try {
 *       const { auditLog } = Durable.workflow.proxyActivities<{
 *         auditLog: (id: string, action: string) => Promise<void>;
 *       }>({
 *         taskQueue: 'audit-activities',
 *         retryPolicy: { maximumAttempts: 3 }
 *       });
 *
 *       await auditLog(workflowCtx.get('workflowId'), `before:${activityCtx.activityName}`);
 *       const result = await next();
 *       await auditLog(workflowCtx.get('workflowId'), `after:${activityCtx.activityName}`);
 *       return result;
 *     } catch (err) {
 *       if (Durable.didInterrupt(err)) throw err;
 *       throw err;
 *     }
 *   }
 * });
 * ```
 *
 * ## Basic Usage Example
 *
 * ```typescript
 * import { Client, Worker, Durable } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 * import * as activities from './activities';
 *
 * // (Optional) Register shared activity workers for interceptors
 * await Durable.registerActivityWorker({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
 *   },
 *   taskQueue: 'shared-activities'
 * }, sharedActivities, 'shared-activities');
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
 *   workflowId: Durable.guid()
 * });
 *
 * // Get result
 * const result = await handle.result();
 *
 * // Cleanup
 * await Durable.shutdown();
 * ```
 */
class DurableClass {
  /**
   * @private
   */
  constructor() {}
  /**
   * The Durable `Client` service is functionally
   * equivalent to the Temporal `Client` service.
   */
  static Client: typeof ClientService = ClientService;

  /**
   * The Durable `Connection` service is functionally
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
   * is typically accessed via the Durable.Client class (workflow.getHandle).
   */
  static Handle: typeof WorkflowHandleService = WorkflowHandleService;

  /**
   * The Durable `Worker` service is functionally
   * equivalent to the Temporal `Worker` service.
   */
  static Worker: typeof WorkerService = WorkerService;

  /**
   * Register activity workers for a task queue. Activities execute via message queue
   * and can run on different servers from workflows.
   * 
   * @example
   * ```typescript
   * // Activity worker
   * const activities = {
   *   async processPayment(amount: number) { return `Processed $${amount}`; },
   *   async sendEmail(to: string, msg: string) { /* ... *\/ }
   * };
   * 
   * await Durable.registerActivityWorker({
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   taskQueue: 'payment'
   * }, activities, 'payment');
   * 
   * // Workflow worker (can be on different server)
   * async function orderWorkflow(amount: number) {
   *   const { processPayment, sendEmail } = Durable.workflow.proxyActivities<{
   *     processPayment: (amount: number) => Promise<string>;
   *     sendEmail: (to: string, msg: string) => Promise<void>;
   *   }>({
   *     taskQueue: 'payment',
   *     retryPolicy: { maximumAttempts: 3 }
   *   });
   *   
   *   const result = await processPayment(amount);
   *   await sendEmail('customer@example.com', result);
   *   return result;
   * }
   * 
   * await Durable.Worker.create({
   *   connection: { class: Postgres, options: { connectionString: '...' } },
   *   taskQueue: 'orders',
   *   workflow: orderWorkflow
   * });
   * ```
   */
  static registerActivityWorker = WorkerService.registerActivityWorker;

  /**
   * The Durable `workflow` service is functionally
   * equivalent to the Temporal `Workflow` service
   * with additional methods for managing workflows,
   * including: `execChild`, `waitFor`, `sleep`, etc
   */
  static workflow: typeof WorkflowService = WorkflowService;

  /**
   * Checks if an error is a HotMesh reserved error type that indicates
   * a workflow interruption rather than a true error condition.
   *
   * @see {@link didInterrupt} for detailed documentation
   */
  static didInterrupt = didInterrupt;

  private static interceptorService = new InterceptorService();

  /**
   * Register a workflow interceptor that wraps the entire workflow execution
   * in an onion-like pattern. Interceptors execute in registration order
   * (first registered is outermost) and can perform actions before and after
   * workflow execution, handle errors, and add cross-cutting concerns like
   * logging, metrics, or tracing.
   *
   * Workflow interceptors run inside the workflow's async local storage context,
   * so all Durable workflow methods (`proxyActivities`, `sleepFor`, `waitFor`,
   * `execChild`, etc.) are available. When using Durable functions, always check
   * for interruptions with `Durable.didInterrupt(err)` and rethrow them.
   *
   * @param interceptor The interceptor to register
   *
   * @example
   * ```typescript
   * // Logging interceptor
   * Durable.registerInterceptor({
   *   async execute(ctx, next) {
   *     console.log(`Workflow ${ctx.get('workflowName')} starting`);
   *     try {
   *       const result = await next();
   *       console.log(`Workflow ${ctx.get('workflowName')} completed`);
   *       return result;
   *     } catch (err) {
   *       if (Durable.didInterrupt(err)) throw err;
   *       console.error(`Workflow ${ctx.get('workflowName')} failed`);
   *       throw err;
   *     }
   *   }
   * });
   * ```
   */
  static registerInterceptor(interceptor: WorkflowInterceptor): void {
    DurableClass.interceptorService.register(interceptor);
  }

  /**
   * Clear all registered interceptors (both workflow and activity).
   */
  static clearInterceptors(): void {
    DurableClass.interceptorService.clear();
  }

  /**
   * Register an activity interceptor that wraps individual proxied
   * activity calls within workflows. Interceptors execute in registration
   * order (first registered is outermost) using the onion pattern.
   *
   * Activity interceptors run inside the workflow's execution context
   * and have access to all Durable workflow methods (`proxyActivities`,
   * `sleepFor`, `waitFor`, `execChild`, etc.). The `activityCtx` parameter
   * provides `activityName`, `args`, and `options` for the call being
   * intercepted. The `workflowCtx` map provides workflow metadata
   * (`workflowId`, `workflowName`, `namespace`, etc.).
   *
   * @param interceptor The activity interceptor to register
   *
   * @example
   * ```typescript
   * Durable.registerActivityInterceptor({
   *   async execute(activityCtx, workflowCtx, next) {
   *     const start = Date.now();
   *     try {
   *       const result = await next();
   *       console.log(`${activityCtx.activityName} took ${Date.now() - start}ms`);
   *       return result;
   *     } catch (err) {
   *       if (Durable.didInterrupt(err)) throw err;
   *       throw err;
   *     }
   *   }
   * });
   * ```
   */
  static registerActivityInterceptor(interceptor: ActivityInterceptor): void {
    DurableClass.interceptorService.registerActivity(interceptor);
  }

  /**
   * Clear all registered activity interceptors
   */
  static clearActivityInterceptors(): void {
    DurableClass.interceptorService.clearActivity();
  }

  /**
   * Get the interceptor service instance
   * @internal
   */
  static getInterceptorService(): InterceptorService {
    return DurableClass.interceptorService;
  }

  /**
   * Generate a unique identifier for workflow IDs
   */
  static guid = guid;

  /**
   * Shutdown everything. All connections, workers, and clients will be closed.
   * Include in your signal handlers to ensure a clean shutdown.
   */
  static async shutdown(): Promise<void> {
    await DurableClass.Client.shutdown();
    await DurableClass.Worker.shutdown();
    await HotMesh.stop();
  }
}

export { DurableClass as Durable };
export type { ContextType };
