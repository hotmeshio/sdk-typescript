import {
  WorkflowInterceptor,
  InterceptorRegistry,
  ActivityInterceptor,
  ActivityInterceptorContext,
} from '../../types/durable';

/**
 * Service for managing workflow interceptors that wrap workflow execution
 * in an onion-like pattern. Each interceptor can perform actions before
 * and after workflow execution, add cross-cutting concerns, and handle errors.
 *
 * ## Basic Interceptor Pattern
 *
 * @example
 * ```typescript
 * // Create and configure interceptors
 * const service = new InterceptorService();
 *
 * // Add logging interceptor (outermost)
 * service.register({
 *   async execute(ctx, next) {
 *     console.log('Starting workflow');
 *     const result = await next();
 *     console.log('Workflow completed');
 *     return result;
 *   }
 * });
 *
 * // Add metrics interceptor (middle)
 * service.register({
 *   async execute(ctx, next) {
 *     const timer = startTimer();
 *     const result = await next();
 *     recordDuration(timer.end());
 *     return result;
 *   }
 * });
 *
 * // Add error handling interceptor (innermost)
 * service.register({
 *   async execute(ctx, next) {
 *     try {
 *       return await next();
 *     } catch (err) {
 *       reportError(err);
 *       throw err;
 *     }
 *   }
 * });
 *
 * // Execute workflow through interceptor chain
 * const result = await service.executeChain(context, async () => {
 *   return await workflowFn();
 * });
 * ```
 *
 * ## Durable Interceptors with Durable Functions
 *
 * Interceptors run within the workflow's async local storage context, which means
 * they can use Durable functions like `sleepFor`, `entity`, `proxyActivities`, etc.
 * These interceptors participate in the HotMesh interruption/replay pattern.
 *
 * @example
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Rate limiting interceptor that sleeps before execution
 * const rateLimitInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     try {
 *       // This sleep will cause an interruption on first execution
 *       await Durable.workflow.sleepFor('1 second');
 *
 *       const result = await next();
 *
 *       // Another sleep after workflow completes
 *       await Durable.workflow.sleepFor('500 milliseconds');
 *
 *       return result;
 *     } catch (err) {
 *       // CRITICAL: Always check for HotMesh interruptions
 *       if (Durable.didInterrupt(err)) {
 *         throw err; // Rethrow interruptions for replay system
 *       }
 *       // Handle actual errors
 *       console.error('Interceptor error:', err);
 *       throw err;
 *     }
 *   }
 * };
 *
 * // Entity-based audit interceptor
 * const auditInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     try {
 *       const entity = await Durable.workflow.entity();
 *       await entity.append('auditLog', {
 *         action: 'workflow_started',
 *         timestamp: new Date().toISOString(),
 *         workflowId: ctx.get('workflowId')
 *       });
 *
 *       const startTime = Date.now();
 *       const result = await next();
 *       const duration = Date.now() - startTime;
 *
 *       await entity.append('auditLog', {
 *         action: 'workflow_completed',
 *         timestamp: new Date().toISOString(),
 *         duration,
 *         success: true
 *       });
 *
 *       return result;
 *     } catch (err) {
 *       if (Durable.didInterrupt(err)) {
 *         throw err;
 *       }
 *
 *       // Log failure to entity
 *       const entity = await Durable.workflow.entity();
 *       await entity.append('auditLog', {
 *         action: 'workflow_failed',
 *         timestamp: new Date().toISOString(),
 *         error: err.message
 *       });
 *
 *       throw err;
 *     }
 *   }
 * };
 *
 * // Register interceptors
 * Durable.registerInterceptor(rateLimitInterceptor);
 * Durable.registerInterceptor(auditInterceptor);
 * ```
 *
 * ## Execution Pattern with Interruptions
 *
 * When interceptors use Durable functions, the workflow will execute multiple times
 * due to the interruption/replay pattern:
 *
 * 1. **First execution**: Interceptor calls `sleepFor` → throws `DurableSleepError` → workflow pauses
 * 2. **Second execution**: Interceptor sleep replays (skipped), workflow runs → proxy activity throws `DurableProxyError` → workflow pauses
 * 3. **Third execution**: All previous operations replay, interceptor sleep after workflow → throws `DurableSleepError` → workflow pauses
 * 4. **Fourth execution**: Everything replays successfully, workflow completes
 *
 * This pattern ensures deterministic, durable execution across all interceptors and workflow code.
 *
 * @example
 * ```typescript
 * // Interceptor with complex Durable operations
 * const complexInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     try {
 *       // Get persistent state
 *       const entity = await Durable.workflow.entity();
 *       const state = await entity.get() as any;
 *
 *       // Conditional durable operations
 *       if (!state.preProcessed) {
 *         await Durable.workflow.sleepFor('100 milliseconds');
 *         await entity.merge({ preProcessed: true });
 *       }
 *
 *       // Execute the workflow
 *       const result = await next();
 *
 *       // Post-processing with child workflow
 *       if (!state.postProcessed) {
 *         await Durable.workflow.execChild({
 *           taskQueue: 'cleanup',
 *           workflowName: 'cleanupWorkflow',
 *           args: [result]
 *         });
 *         await entity.merge({ postProcessed: true });
 *       }
 *
 *       return result;
 *     } catch (err) {
 *       if (Durable.didInterrupt(err)) {
 *         throw err;
 *       }
 *       throw err;
 *     }
 *   }
 * };
 * ```
 *
 * ## Activity Interceptors
 *
 * Activity interceptors wrap individual proxied activity calls, supporting
 * both **before** and **after** phases. The before phase receives the activity
 * input (and can modify `activityCtx.args`). The after phase receives the
 * activity output as the return value of `next()`.
 *
 * This enables patterns like publishing activity results to an external
 * system (e.g., SNS, audit log) without modifying the workflow itself.
 *
 * **Important:** The after-phase proxy activity calls go through the same
 * interceptor chain. Guard against recursion by checking `activityCtx.activityName`
 * to skip the interceptor's own calls.
 *
 * @example
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import type { ActivityInterceptor } from '@hotmeshio/hotmesh/types/durable';
 * import * as activities from './activities';
 *
 * // Activity interceptor that publishes results via a proxy activity
 * const publishResultInterceptor: ActivityInterceptor = {
 *   async execute(activityCtx, workflowCtx, next) {
 *     try {
 *       // BEFORE: inspect or modify the activity input
 *       console.log(`Calling ${activityCtx.activityName}`, activityCtx.args);
 *
 *       // Execute the activity (returns stored result on replay)
 *       const result = await next();
 *
 *       // AFTER: use the activity output (only runs on replay,
 *       // once the result is available)
 *
 *       // Guard: skip for the interceptor's own proxy calls
 *       if (activityCtx.activityName !== 'publishToSNS') {
 *         const { publishToSNS } = Durable.workflow.proxyActivities<{
 *           publishToSNS: (topic: string, payload: any) => Promise<void>;
 *         }>({
 *           taskQueue: 'shared-notifications',
 *           retryPolicy: { maximumAttempts: 3, throwOnError: true },
 *         });
 *
 *         await publishToSNS('activity-results', {
 *           workflowId: workflowCtx.get('workflowId'),
 *           activityName: activityCtx.activityName,
 *           input: activityCtx.args,
 *           output: result,
 *         });
 *       }
 *
 *       return result;
 *     } catch (err) {
 *       if (Durable.didInterrupt(err)) throw err;
 *       throw err;
 *     }
 *   },
 * };
 *
 * Durable.registerActivityInterceptor(publishResultInterceptor);
 * ```
 *
 * ## Activity Interceptor Replay Pattern
 *
 * Activity interceptors participate in the interruption/replay cycle:
 *
 * 1. **First execution**: Before-phase runs → `next()` registers the activity
 *    interruption and throws `DurableProxyError` → workflow pauses
 * 2. **Second execution**: Before-phase replays → `next()` returns the stored
 *    activity result → after-phase runs → after-phase proxy call (e.g.,
 *    `publishToSNS`) registers its own interruption → workflow pauses
 * 3. **Third execution**: Everything replays → after-phase proxy call returns
 *    its stored result → interceptor returns → workflow continues
 */
export class InterceptorService implements InterceptorRegistry {
  interceptors: WorkflowInterceptor[] = [];
  activityInterceptors: ActivityInterceptor[] = [];

  /**
   * Register a new workflow interceptor that will wrap workflow execution.
   * Interceptors are executed in the order they are registered, with the
   * first registered interceptor being the outermost wrapper.
   *
   * @param interceptor The interceptor to register
   *
   * @example
   * ```typescript
   * service.register({
   *   async execute(ctx, next) {
   *     console.time('workflow');
   *     try {
   *       return await next();
   *     } finally {
   *       console.timeEnd('workflow');
   *     }
   *   }
   * });
   * ```
   */
  register(interceptor: WorkflowInterceptor): void {
    this.interceptors.push(interceptor);
  }

  /**
   * Execute the interceptor chain around the workflow function.
   * The chain is built in an onion pattern where each interceptor
   * wraps the next one, with the workflow function at the center.
   *
   * @param ctx The workflow context map
   * @param fn The workflow function to execute
   * @returns The result of the workflow execution
   *
   * @example
   * ```typescript
   * // Execute workflow with timing
   * const result = await service.executeChain(context, async () => {
   *   const start = Date.now();
   *   try {
   *     return await workflowFn();
   *   } finally {
   *     console.log('Duration:', Date.now() - start);
   *   }
   * });
   * ```
   */
  async executeChain(
    ctx: Map<string, any>,
    fn: () => Promise<any>,
  ): Promise<any> {
    // Create the onion-like chain of interceptors
    const chain = this.interceptors.reduceRight((next, interceptor) => {
      return () => interceptor.execute(ctx, next);
    }, fn);

    return chain();
  }

  /**
   * Register a new activity interceptor that will wrap individual
   * proxied activity calls. Interceptors are executed in the order
   * they are registered, with the first registered being the outermost wrapper.
   *
   * @param interceptor The activity interceptor to register
   */
  registerActivity(interceptor: ActivityInterceptor): void {
    this.activityInterceptors.push(interceptor);
  }

  /**
   * Execute the activity interceptor chain around an activity invocation.
   * Uses the same onion/reduceRight pattern as executeChain.
   *
   * @param activityCtx - Metadata about the activity (name, args, options)
   * @param workflowCtx - The workflow context Map
   * @param fn - The core activity function (registers with interruptionRegistry, sleeps, throws)
   * @returns The result of the activity (from replay or after future execution)
   */
  async executeActivityChain(
    activityCtx: ActivityInterceptorContext,
    workflowCtx: Map<string, any>,
    fn: () => Promise<any>,
  ): Promise<any> {
    const chain = this.activityInterceptors.reduceRight(
      (next, interceptor) => {
        return () => interceptor.execute(activityCtx, workflowCtx, next);
      },
      fn,
    );
    return chain();
  }

  /**
   * Clear all registered activity interceptors.
   */
  clearActivity(): void {
    this.activityInterceptors = [];
  }

  /**
   * Clear all registered interceptors (both workflow and activity).
   *
   * @example
   * ```typescript
   * service.clear();
   * ```
   */
  clear(): void {
    this.interceptors = [];
    this.activityInterceptors = [];
  }
}
