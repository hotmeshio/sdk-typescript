import {
  // Individual methods
  getContext,
} from './context';
import { didRun } from './didRun';
import { isSideEffectAllowed } from './isSideEffectAllowed';
import { trace } from './trace';
import { enrich } from './enrich';
import { emit } from './emit';
import { execChild, executeChild, startChild } from './execChild';
import { execHook } from './execHook';
import { execHookBatch } from './execHookBatch';
import { proxyActivities } from './proxyActivities';
import { search } from './searchMethods';
import { random } from './random';
import { signal } from './signal';
import { hook } from './hook';
import { interrupt } from './interrupt';
import { didInterrupt } from './interruption';
import { all } from './all';
import { sleepFor } from './sleepFor';
import { waitFor } from './waitFor';
import { asyncLocalStorage, WorkerService, HotMesh } from './common';
import { entity } from './entityMethods';

/**
 * The workflow-internal API surface, exposed as `Durable.workflow`. Every
 * method on this class is designed to be called **inside** a workflow
 * function — they participate in deterministic replay and durable state
 * management.
 *
 * ## Core Primitives
 *
 * | Method | Purpose |
 * |--------|---------|
 * | {@link proxyActivities} | Create durable activity proxies with retry |
 * | {@link sleepFor} | Durable, crash-safe sleep |
 * | {@link waitFor} | Pause until a signal is received |
 * | {@link signal} | Send data to a waiting workflow |
 * | {@link execChild} | Spawn and await a child workflow |
 * | {@link startChild} | Spawn a child workflow (fire-and-forget) |
 * | {@link execHook} | Spawn a hook and await its signal response |
 * | {@link execHookBatch} | Spawn multiple hooks in parallel |
 * | {@link hook} | Low-level hook spawning |
 * | {@link interrupt} | Terminate a running workflow |
 *
 * ## Data & Observability
 *
 * | Method | Purpose |
 * |--------|---------|
 * | {@link search} | Read/write flat HASH key-value data |
 * | {@link enrich} | One-shot HASH enrichment |
 * | {@link entity} | Structured JSONB document storage |
 * | {@link emit} | Publish events to the event bus |
 * | {@link trace} | Emit OpenTelemetry trace spans |
 *
 * ## Utilities
 *
 * | Method | Purpose |
 * |--------|---------|
 * | {@link getContext} | Access workflow ID, namespace, replay state |
 * | {@link random} | Deterministic pseudo-random numbers |
 * | {@link all} | Workflow-safe `Promise.all` |
 * | {@link didInterrupt} | Type guard for engine control-flow errors |
 *
 * ## Example
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import * as activities from './activities';
 *
 * export async function orderWorkflow(orderId: string): Promise<string> {
 *   // Proxy activities for durable execution
 *   const { validateOrder, processPayment, sendReceipt } =
 *     Durable.workflow.proxyActivities<typeof activities>({
 *       activities,
 *       retryPolicy: { maximumAttempts: 3 },
 *     });
 *
 *   await validateOrder(orderId);
 *
 *   // Durable sleep (survives restarts)
 *   await Durable.workflow.sleepFor('5 seconds');
 *
 *   const receipt = await processPayment(orderId);
 *
 *   // Store searchable metadata
 *   await Durable.workflow.enrich({ orderId, status: 'paid' });
 *
 *   // Wait for external approval signal
 *   const approval = await Durable.workflow.waitFor<{ ok: boolean }>('approve');
 *   if (!approval.ok) return 'cancelled';
 *
 *   await sendReceipt(orderId, receipt);
 *   return receipt;
 * }
 * ```
 */
export class WorkflowService {
  /**
   * @private
   * The constructor is private to prevent instantiation;
   * all methods are static.
   */
  private constructor() {}

  static getContext = getContext;
  static didRun = didRun;
  static isSideEffectAllowed = isSideEffectAllowed;
  static trace = trace;
  static enrich = enrich;
  static emit = emit;
  static execChild = execChild;
  static executeChild = executeChild;
  static startChild = startChild;
  static execHook = execHook;
  static execHookBatch = execHookBatch;
  static proxyActivities = proxyActivities;
  static search = search;
  static entity = entity;
  static random = random;
  static signal = signal;
  static hook = hook;
  static didInterrupt = didInterrupt;
  static interrupt = interrupt;
  static all = all;
  static sleepFor = sleepFor;
  static waitFor = waitFor;

  /**
   * Return a handle to the HotMesh client hosting the workflow execution.
   * @returns {Promise<HotMesh>} The HotMesh client instance.
   */
  static async getHotMesh(): Promise<HotMesh> {
    const store = asyncLocalStorage.getStore();
    const workflowTopic = store.get('workflowTopic');
    const connection = store.get('connection');
    const namespace = store.get('namespace');
    return await WorkerService.getHotMesh(workflowTopic, {
      connection,
      namespace,
    });
  }
}
