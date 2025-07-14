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
 * The WorkflowService class provides a set of static methods to be used within a workflow function.
 * These methods ensure deterministic replay, persistence of state, and error handling across
 * re-entrant workflow executions.
 *
 * @example
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * export async function waitForExample(): Promise<[boolean, number]> {
 *   const [s1, s2] = await Promise.all([
 *     MemFlow.workflow.waitFor<boolean>('my-sig-nal-1'),
 *     MemFlow.workflow.waitFor<number>('my-sig-nal-2')
 *   ]);
 *   return [s1, s2];
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
