import { WorkflowInterceptor, InterceptorRegistry } from '../../types/memflow';

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
 * ## Durable Interceptors with MemFlow Functions
 *
 * Interceptors run within the workflow's async local storage context, which means
 * they can use MemFlow functions like `sleepFor`, `entity`, `proxyActivities`, etc.
 * These interceptors participate in the HotMesh interruption/replay pattern.
 *
 * @example
 * ```typescript
 * import { MemFlow } from '@hotmeshio/hotmesh';
 *
 * // Rate limiting interceptor that sleeps before execution
 * const rateLimitInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     try {
 *       // This sleep will cause an interruption on first execution
 *       await MemFlow.workflow.sleepFor('1 second');
 *
 *       const result = await next();
 *
 *       // Another sleep after workflow completes
 *       await MemFlow.workflow.sleepFor('500 milliseconds');
 *
 *       return result;
 *     } catch (err) {
 *       // CRITICAL: Always check for HotMesh interruptions
 *       if (MemFlow.didInterrupt(err)) {
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
 *       const entity = await MemFlow.workflow.entity();
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
 *       if (MemFlow.didInterrupt(err)) {
 *         throw err;
 *       }
 *
 *       // Log failure to entity
 *       const entity = await MemFlow.workflow.entity();
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
 * MemFlow.registerInterceptor(rateLimitInterceptor);
 * MemFlow.registerInterceptor(auditInterceptor);
 * ```
 *
 * ## Execution Pattern with Interruptions
 *
 * When interceptors use MemFlow functions, the workflow will execute multiple times
 * due to the interruption/replay pattern:
 *
 * 1. **First execution**: Interceptor calls `sleepFor` → throws `MemFlowSleepError` → workflow pauses
 * 2. **Second execution**: Interceptor sleep replays (skipped), workflow runs → proxy activity throws `MemFlowProxyError` → workflow pauses
 * 3. **Third execution**: All previous operations replay, interceptor sleep after workflow → throws `MemFlowSleepError` → workflow pauses
 * 4. **Fourth execution**: Everything replays successfully, workflow completes
 *
 * This pattern ensures deterministic, durable execution across all interceptors and workflow code.
 *
 * @example
 * ```typescript
 * // Interceptor with complex MemFlow operations
 * const complexInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     try {
 *       // Get persistent state
 *       const entity = await MemFlow.workflow.entity();
 *       const state = await entity.get() as any;
 *
 *       // Conditional durable operations
 *       if (!state.preProcessed) {
 *         await MemFlow.workflow.sleepFor('100 milliseconds');
 *         await entity.merge({ preProcessed: true });
 *       }
 *
 *       // Execute the workflow
 *       const result = await next();
 *
 *       // Post-processing with child workflow
 *       if (!state.postProcessed) {
 *         await MemFlow.workflow.execChild({
 *           taskQueue: 'cleanup',
 *           workflowName: 'cleanupWorkflow',
 *           args: [result]
 *         });
 *         await entity.merge({ postProcessed: true });
 *       }
 *
 *       return result;
 *     } catch (err) {
 *       if (MemFlow.didInterrupt(err)) {
 *         throw err;
 *       }
 *       throw err;
 *     }
 *   }
 * };
 * ```
 */
export class InterceptorService implements InterceptorRegistry {
  interceptors: WorkflowInterceptor[] = [];

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
   * Clear all registered interceptors.
   *
   * @example
   * ```typescript
   * service.clear();
   * ```
   */
  clear(): void {
    this.interceptors = [];
  }
}
