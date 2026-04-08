import { HotMesh } from '../hotmesh';
import { ContextType, WorkflowInboundCallsInterceptor, WorkflowOutboundCallsInterceptor, ActivityInboundCallsInterceptor } from '../../types/durable';
import { guid } from '../../modules/utils';

import { ClientService } from './client';
import { ConnectionService } from './connection';
import { Search } from './search';
import { Entity } from './entity';
import { ActivityService } from './activity';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { WorkflowHandleService } from './handle';
import { didInterrupt } from './workflow/interruption';
import { InterceptorService } from './interceptor';

/**
 * The Durable service provides a workflow framework backed by Postgres.
 * It offers entity-based memory management and composable, fault-tolerant
 * workflows authored in a familiar procedural style.
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
 *   Durable.workflow.condition('optimistic-complete'),
 *   Durable.workflow.condition('skeptical-complete')
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
 * const childResult = await Durable.workflow.executeChild({
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
 * Durable.registerInboundInterceptor({
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
 * methods (`proxyActivities`, `sleep`, `condition`, `executeChild`, etc.).
 * Multiple activity interceptors execute in onion order (first registered
 * is outermost).
 * ```typescript
 * // Simple logging interceptor
 * Durable.registerOutboundInterceptor({
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
 * Durable.registerOutboundInterceptor({
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
   * provides methods for starting, signaling, and querying workflows.
   */
  static Client: typeof ClientService = ClientService;

  /**
   * The Durable `Connection` service
   * manages database connections for the durable workflow engine.
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
   * The Durable `Worker` service
   * registers workflow and activity workers and connects them to the mesh.
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
   * The Durable `activity` service provides context to
   * executing activity functions. Call `Durable.activity.getContext()`
   * inside an activity to access metadata, workflow ID, and other
   * context passed from the parent workflow.
   */
  static activity: typeof ActivityService = ActivityService;

  /**
   * The Durable `workflow` service
   * provides the workflow-internal API surface with methods for
   * managing workflows, including: `executeChild`, `condition`, `sleep`, etc
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
   * so all Durable workflow methods (`proxyActivities`, `sleep`, `condition`,
   * `executeChild`, etc.) are available. When using Durable functions, always check
   * for interruptions with `Durable.didInterrupt(err)` and rethrow them.
   *
   * @param interceptor The interceptor to register
   *
   * @example
   * ```typescript
   * // Logging interceptor
   * Durable.registerInboundInterceptor({
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
  static registerInboundInterceptor(interceptor: WorkflowInboundCallsInterceptor): void {
    DurableClass.interceptorService.registerInbound(interceptor);
  }

  /**
   * Clear all registered interceptors (both inbound and outbound).
   */
  static clearInterceptors(): void {
    DurableClass.interceptorService.clear();
  }

  /**
   * Register an outbound interceptor that wraps individual proxied
   * activity calls within workflows. Interceptors execute in registration
   * order (first registered is outermost) using the onion pattern.
   *
   * Outbound interceptors run inside the workflow's execution context
   * and have access to all Durable workflow methods (`proxyActivities`,
   * `sleep`, `condition`, `executeChild`, etc.). The `activityCtx` parameter
   * provides `activityName`, `args`, and `options` for the call being
   * intercepted. The `workflowCtx` map provides workflow metadata
   * (`workflowId`, `workflowName`, `namespace`, etc.).
   *
   * @param interceptor The outbound interceptor to register
   *
   * @example
   * ```typescript
   * Durable.registerOutboundInterceptor({
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
  static registerOutboundInterceptor(interceptor: WorkflowOutboundCallsInterceptor): void {
    DurableClass.interceptorService.registerOutbound(interceptor);
  }

  /**
   * Clear all registered outbound interceptors
   */
  static clearOutboundInterceptors(): void {
    DurableClass.interceptorService.clearOutbound();
  }

  /**
   * Register an activity inbound interceptor that wraps the actual activity
   * function execution on the activity worker side. This runs inside the
   * activity's `AsyncLocalStorage` context, NOT the workflow context.
   *
   * Use for logging, metrics, auth validation, or error enrichment at
   * the point where the activity actually executes.
   */
  static registerActivityInboundInterceptor(interceptor: ActivityInboundCallsInterceptor): void {
    DurableClass.interceptorService.registerActivityInbound(interceptor);
  }

  /**
   * Clear all registered activity inbound interceptors.
   */
  static clearActivityInboundInterceptors(): void {
    DurableClass.interceptorService.clearActivityInbound();
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
