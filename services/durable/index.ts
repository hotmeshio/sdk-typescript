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
 * Durable workflow engine backed by Postgres. Write workflows as plain
 * async functions; the engine handles persistence, replay, retry, and
 * crash recovery automatically.
 *
 * ## Architecture
 *
 * | Component | Role |
 * |-----------|------|
 * | {@link Worker} | Hosts workflow and activity functions |
 * | {@link Client} | Starts workflows, sends signals, reads results |
 * | {@link workflow} | In-workflow API (`proxyActivities`, `sleep`, `condition`, ...) |
 *
 * ## Workflow Primitives
 *
 * All methods below are available as `Durable.workflow.<method>` inside
 * a workflow function. Each call is durable — results are persisted and
 * replayed deterministically on recovery.
 *
 * | Primitive | Purpose |
 * |-----------|---------|
 * | `proxyActivities` | Execute side-effectful code with automatic retry |
 * | `sleep` | Durable timer (survives restarts) |
 * | `condition` | Pause until a named signal arrives (with optional timeout) |
 * | `signal` | Deliver data to a waiting `condition` |
 * | `executeChild` | Spawn a child workflow and await its result |
 * | `startChild` | Fire-and-forget child workflow |
 * | `continueAsNew` | Restart the workflow with new args (resets history) |
 * | `patched` / `deprecatePatch` | Safe versioning for in-flight workflow code changes |
 * | `CancellationScope.nonCancellable` | Shield cleanup code from cancellation |
 * | `isCancellation` | Detect cooperative cancellation errors |
 *
 * ## Quick Start
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 * import * as activities from './activities';
 *
 * // 1. Define a workflow
 * async function orderWorkflow(orderId: string): Promise<string> {
 *   const { charge, notify } = Durable.workflow.proxyActivities<typeof activities>({
 *     activities,
 *     retryPolicy: { maximumAttempts: 3 },
 *   });
 *
 *   const receipt = await charge(orderId);
 *   await Durable.workflow.sleep('1 hour');
 *   await notify(orderId, receipt);
 *   return receipt;
 * }
 *
 * // 2. Start a worker
 * const connection = { class: Postgres, options: { connectionString: 'postgresql://...' } };
 * await Durable.Worker.create({ connection, taskQueue: 'orders', workflow: orderWorkflow });
 *
 * // 3. Start a workflow from the client
 * const client = new Durable.Client({ connection });
 * const handle = await client.workflow.start({
 *   args: ['order-123'],
 *   taskQueue: 'orders',
 *   workflowName: 'orderWorkflow',
 *   workflowId: Durable.guid(),
 * });
 * const result = await handle.result();
 * ```
 *
 * ## Interceptors
 *
 * Cross-cutting concerns (logging, auth, metrics) attach via interceptors.
 * See {@link registerInboundInterceptor} (wraps workflows) and
 * {@link registerOutboundInterceptor} (wraps individual activity calls).
 *
 * ## Secured Workers
 *
 * For production isolation, workers can connect with scoped Postgres
 * credentials that restrict access to specific task queues via stored
 * procedures. See {@link WorkerService.create} and {@link provisionWorkerRole}.
 */
class DurableClass {
  /**
   * @private
   */
  constructor() {}
  /**
   * Starts workflows, sends signals, and reads results. Instantiate
   * with a connection, then use `client.workflow.start()` to launch
   * a workflow and obtain a {@link Handle}.
   */
  static Client: typeof ClientService = ClientService;

  /**
   * Declares connection options (class + config) for Postgres.
   * The actual connection is established lazily when a workflow
   * or worker is started.
   */
  static Connection: typeof ConnectionService = ConnectionService;

  /** @private */
  static Search: typeof Search = Search;

  /** @private */
  static Entity: typeof Entity = Entity;

  /**
   * A handle to a running or completed workflow. Provides methods to
   * read results, send signals, query state, cancel, or interrupt.
   * Obtained via `client.workflow.start()` or `client.workflow.getHandle()`.
   */
  static Handle: typeof WorkflowHandleService = WorkflowHandleService;

  /**
   * Hosts workflow and activity functions. Call `Worker.create()` with
   * a connection, task queue, and workflow function to start processing.
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
   * Activity-side context. Call `Durable.activity.getContext()` inside
   * an activity function to access the activity name, arguments,
   * parent workflow ID, and other execution metadata.
   */
  static activity: typeof ActivityService = ActivityService;

  /**
   * The workflow-internal API. Every method on this object
   * (`proxyActivities`, `sleep`, `condition`, `signal`, `executeChild`,
   * `continueAsNew`, `patched`, `CancellationScope`, etc.) is designed
   * to be called **inside** a workflow function and participates in
   * deterministic replay.
   */
  static workflow: typeof WorkflowService = WorkflowService;

  /**
   * Returns `true` if the error is an engine control-flow signal (replay
   * suspension) rather than an application error. **Always check this in
   * `catch` blocks inside workflows and interceptors** — swallowing an
   * interruption breaks the replay system.
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
   * Provision a scoped Postgres role for a worker. The role can only
   * dequeue, ack, and respond on its assigned stream names via stored
   * procedures — zero direct table access.
   *
   * @example
   * ```typescript
   * const cred = await Durable.provisionWorkerRole({
   *   connection: { class: Postgres, options: adminOptions },
   *   streamNames: ['payment-activity'],
   * });
   *
   * await Worker.create({
   *   connection: { class: Postgres, options: pgOptions },
   *   taskQueue: 'payment',
   *   workflow: paymentWorkflow,
   *   workerCredentials: cred,
   * });
   * ```
   */
  static provisionWorkerRole = HotMesh.provisionWorkerRole;

  /** Rotate a secured worker role's password. */
  static rotateWorkerPassword = HotMesh.rotateWorkerPassword;

  /** Revoke a secured worker role (disables login). */
  static revokeWorkerRole = HotMesh.revokeWorkerRole;

  /** List all provisioned secured worker roles. */
  static listWorkerRoles = HotMesh.listWorkerRoles;

  /**
   * Register a global payload codec for encrypting/decrypting workflow
   * data at rest. The codec's `encode`/`decode` methods are called
   * automatically on all persisted workflow payloads.
   */
  static registerCodec = HotMesh.registerCodec;

  /**
   * Gracefully shut down all workers, clients, and connections.
   * Call from your process signal handlers (`SIGTERM`, `SIGINT`).
   */
  static async shutdown(): Promise<void> {
    await DurableClass.Client.shutdown();
    await DurableClass.Worker.shutdown();
    await HotMesh.stop();
  }
}

export { DurableClass as Durable };
export type { ContextType };
