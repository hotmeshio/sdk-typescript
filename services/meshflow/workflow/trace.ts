import {
  asyncLocalStorage,
  TelemetryService,
  WorkerService,
  StringScalarType,
} from './common';
import { getContext } from './context';
import { isSideEffectAllowed } from './isSideEffectAllowed';


/**
 * Executes a distributed trace, outputting the provided attributes
 * to the telemetry sink (e.g. OpenTelemetry).
 *
 * This trace will only run once per workflow execution by default.
 *
 * @param {StringScalarType} attributes - Key-value attributes to attach to the trace.
 * @param {{ once: boolean }} [config={ once: true }] - If `once` is true, trace only runs once.
 * @returns {Promise<boolean>} True if tracing succeeded, otherwise false.
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
