import {
  asyncLocalStorage,
  TelemetryService,
  WorkerService,
  StringScalarType,
} from './common';
import { getContext } from './context';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Emits a distributed trace span to the configured telemetry sink
 * (e.g., OpenTelemetry). The span is linked to the workflow's trace
 * context (`traceId`, `spanId`) for end-to-end observability across
 * workflow executions, activities, and child workflows.
 *
 * By default (`config.once = true`), the trace is emitted exactly once
 * per workflow execution — it will not re-fire on replay.
 *
 * ## Examples
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * // Emit trace spans at key workflow milestones
 * export async function orderWorkflow(orderId: string): Promise<void> {
 *   await Durable.workflow.trace({
 *     'order.id': orderId,
 *     'order.stage': 'started',
 *   });
 *
 *   const { processPayment } = Durable.workflow.proxyActivities<typeof activities>();
 *   await processPayment(orderId);
 *
 *   await Durable.workflow.trace({
 *     'order.id': orderId,
 *     'order.stage': 'payment_completed',
 *   });
 * }
 * ```
 *
 * ```typescript
 * // Trace on every re-execution (for debugging replay behavior)
 * await Durable.workflow.trace(
 *   { 'debug.counter': counter, 'debug.phase': 'retry' },
 *   { once: false },
 * );
 * ```
 *
 * @param {StringScalarType} attributes - Key-value attributes to attach to the trace span.
 * @param {{ once: boolean }} [config={ once: true }] - If `true`, trace fires only once (idempotent).
 * @returns {Promise<boolean>} `true` if tracing succeeded.
 */
export async function trace(
  attributes: StringScalarType,
  config: { once: boolean } = { once: true },
): Promise<boolean> {
  const store = asyncLocalStorage.getStore();
  const workflowTopic = store.get('workflowTopic');
  const connection = store.get('connection');
  const namespace = store.get('namespace');
  const hotMeshClient = await WorkerService.getHotMesh(workflowTopic, {
    connection,
    namespace,
  });
  const { raw, COUNTER } = getContext();
  const { trc: traceId, spn: spanId, aid: activityId } = raw.metadata;

  if (!config.once || await isSideEffectAllowed(hotMeshClient, 'trace')) {
    return await TelemetryService.traceActivity(
      namespace,
      attributes,
      activityId,
      traceId,
      spanId,
      COUNTER.counter,
    );
  }
  return true;
}
