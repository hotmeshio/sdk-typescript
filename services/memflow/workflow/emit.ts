import {
  asyncLocalStorage,
  WorkerService,
  StringAnyType,
  UserMessage,
} from './common';
import { isSideEffectAllowed } from './isSideEffectAllowed';

/**
 * Emits events to the event bus provider. Topics are prefixed with the quorum namespace.
 *
 * @param {StringAnyType} events - A mapping of topic => message to publish.
 * @param {{ once: boolean }} [config={ once: true }] - If `once` is true, events are emitted only once.
 * @returns {Promise<boolean>} True after emission completes.
 */
export async function emit(
  events: StringAnyType,
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

  if (!config.once || await isSideEffectAllowed(hotMeshClient, 'emit')) {
    for (const [topic, message] of Object.entries(events)) {
      await hotMeshClient.quorum.pub({ topic, message } as UserMessage);
    }
  }
  return true;
}
