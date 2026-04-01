import { activityAsyncLocalStorage } from '../../modules/storage';
import { DurableActivityContext } from '../../types/durable';

/**
 * The activity-internal API surface, exposed as `Durable.activity`. Methods
 * on this class are designed to be called **inside** an activity function —
 * they read from the activity's `AsyncLocalStorage` context that is populated
 * by the activity worker before invoking the user's function.
 *
 * ## Usage
 *
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 *
 * export async function processData(data: string): Promise<string> {
 *   const ctx = Durable.activity.getContext();
 *   console.log(`Activity ${ctx.activityName} for workflow ${ctx.workflowId}`);
 *   console.log(`Metadata:`, ctx.argumentMetadata);
 *   return `Processed: ${data}`;
 * }
 * ```
 */
export class ActivityService {
  /**
   * Returns the current activity's execution context. Only available
   * inside an activity function invoked by the durable activity worker.
   *
   * @returns The activity context including name, args, metadata, and parent workflow info.
   * @throws If called outside of an activity execution context.
   */
  static getContext(): DurableActivityContext {
    const store = activityAsyncLocalStorage.getStore();
    if (!store) {
      throw new Error(
        'Durable.activity.getContext() called outside of an activity execution context',
      );
    }
    return {
      activityName: store.get('activityName'),
      arguments: store.get('arguments'),
      argumentMetadata: store.get('argumentMetadata') ?? {},
      workflowId: store.get('workflowId'),
      workflowTopic: store.get('workflowTopic'),
    };
  }
}
