import {
  // Individual methods
  getContext,
} from './context';
import { didRun } from './didRun';
import { isSideEffectAllowed } from './isSideEffectAllowed';
import { trace } from './trace';
import { enrich } from './enrich';
import { emit } from './emit';
import { executeChild, startChild } from './executeChild';
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
import { sleep } from './sleep';
import { condition } from './condition';
import { continueAsNew } from './continueAsNew';
import { patched, deprecatePatch } from './patched';
import { CancellationScope, CancelledFailure, isCancellation } from './cancellationScope';
import { asyncLocalStorage, WorkerService, HotMesh } from './common';
import { entity } from './entityMethods';

/**
 * In-workflow API surface, exposed as `Durable.workflow`. Every method
 * is called **inside** a workflow function and participates in
 * deterministic replay — results are persisted and returned instantly
 * on recovery without re-executing side effects.
 *
 * ## Primitives
 *
 * | Method | Purpose |
 * |--------|---------|
 * | {@link proxyActivities} | Execute activities with automatic retry |
 * | {@link sleep} | Durable timer (survives restarts) |
 * | {@link condition} | Pause until a named signal arrives (optional timeout) |
 * | {@link signal} | Deliver data to a waiting `condition` |
 * | {@link executeChild} | Spawn a child workflow and await its result |
 * | {@link startChild} | Fire-and-forget child workflow |
 * | {@link continueAsNew} | Restart with new args (resets history) |
 * | {@link patched} / {@link deprecatePatch} | Safe versioning for in-flight code changes |
 * | {@link CancellationScope} | Shield cleanup code from cancellation |
 * | {@link isCancellation} | Detect `CancelledFailure` errors |
 *
 * ## Utilities
 *
 * | Method | Purpose |
 * |--------|---------|
 * | {@link getContext} | Access workflow ID, namespace, replay state |
 * | {@link random} | Deterministic pseudo-random numbers |
 * | {@link all} | Workflow-safe `Promise.all` |
 * | {@link didInterrupt} | Detect engine control-flow errors |
 *
 * ## Example
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import * as activities from './activities';
 *
 * export async function orderWorkflow(orderId: string): Promise<string> {
 *   const { validateOrder, processPayment, sendReceipt } =
 *     Durable.workflow.proxyActivities<typeof activities>({
 *       activities,
 *       retryPolicy: { maximumAttempts: 3 },
 *     });
 *
 *   await validateOrder(orderId);
 *   await Durable.workflow.sleep('5 seconds');
 *   const receipt = await processPayment(orderId);
 *
 *   // Wait for external approval signal (with 1-hour timeout)
 *   const approval = await Durable.workflow.condition<{ ok: boolean }>('approve', '1 hour');
 *   if (!approval) return 'timed-out';
 *   if (!approval.ok) return 'rejected';
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
  static sleep = sleep;
  static condition = condition;
  static continueAsNew = continueAsNew;
  static patched = patched;
  static deprecatePatch = deprecatePatch;
  static CancellationScope = CancellationScope;
  static CancelledFailure = CancelledFailure;
  static isCancellation = isCancellation;

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
