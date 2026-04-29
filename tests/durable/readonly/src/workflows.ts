import { Durable } from '../../../../services/durable';

import type greetFunctionType from './activities';
type ActivitiesType = { greet: typeof greetFunctionType };

import * as activities from './activities';

const { greet } = Durable.workflow.proxyActivities<ActivitiesType>({
  activities,
});

/**
 * Minimal workflow — just calls a proxy activity.
 */
export async function readonlyExample(name: string): Promise<string> {
  const greeting = await greet(name);
  return greeting.complex;
}

/**
 * Robust workflow that exercises proxy activities, sleep, and entity
 * state. When started from a readonly client, the remote non-readonly
 * worker handles all routing, activity execution, and timer management.
 */
export async function robustWorkflow(
  orderId: string,
): Promise<Record<string, any>> {
  const entity = await Durable.workflow.entity();

  await entity.set({
    orderId,
    status: 'started',
    steps: [],
  });

  try {
    // 1. Call a proxy activity
    const { processOrder, sendNotification } =
      Durable.workflow.proxyActivities<typeof activities>({
        activities,
        retry: { maximumAttempts: 1, throwOnError: true },
      });

    const processed = await processOrder(orderId);
    await entity.append('steps', 'processed');

    // 2. Durable sleep
    await Durable.workflow.sleep('1 second');
    await entity.append('steps', 'slept');

    // 3. Another proxy activity
    const notification = await sendNotification('admin', processed);
    await entity.append('steps', 'notified');

    await entity.merge({
      status: 'completed',
      processed,
      notification,
    });

    return await entity.get();
  } catch (err) {
    if (Durable.didInterrupt(err)) throw err;
    await entity.merge({ status: 'failed', error: err.message });
    throw err;
  }
}

/**
 * Workflow that calls proxy activities with headers/metadata.
 * The interceptor injects additional headers before each call.
 */
export async function metadataWorkflow(
  name: string,
): Promise<Record<string, any>> {
  const entity = await Durable.workflow.entity();

  await entity.set({
    name,
    status: 'started',
    steps: [],
  });

  try {
    const { metadataAwareActivity } =
      Durable.workflow.proxyActivities<typeof activities>({
        activities,
        headers: { source: 'readonly-dashboard', env: 'test' },
        retry: { maximumAttempts: 1, throwOnError: true },
      });

    const result = await metadataAwareActivity(name);
    await entity.append('steps', 'metadata-called');
    await entity.merge({ status: 'completed', activityResult: result });

    return await entity.get();
  } catch (err) {
    if (Durable.didInterrupt(err)) throw err;
    await entity.merge({ status: 'failed', error: err.message });
    throw err;
  }
}
