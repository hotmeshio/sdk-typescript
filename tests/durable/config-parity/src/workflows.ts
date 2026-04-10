import { Durable } from '../../../../services/durable';

import * as activities from './activities';

// Test initialInterval: 2s with backoffCoefficient: 2
// Retry 1: 2 * 2^0 = 2s, Retry 2: 2 * 2^1 = 4s
export async function initialIntervalWorkflow(): Promise<string> {
  const { failThenSucceed } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: {
      maximumAttempts: 5,
      backoffCoefficient: 2,
      initialInterval: '5s',
      maximumInterval: '30s',
    },
  });
  return await failThenSucceed();
}

// Test startToCloseTimeout with a fast activity (positive: completes in time)
export async function startToCloseWorkflow(): Promise<string> {
  const { fastActivity } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    startToCloseTimeout: '30s',
    retryPolicy: { maximumAttempts: 1 },
  });
  return await fastActivity();
}

// Test startToCloseTimeout with a slow activity (negative: exceeds timeout)
export async function startToCloseTimeoutWorkflow(): Promise<string> {
  const { slowActivity } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    startToCloseTimeout: '5s',
    retryPolicy: { maximumAttempts: 1, throwOnError: false },
  });
  const result = await slowActivity();
  // If timeout kicked in, result will be an error object, not a string
  if (typeof result === 'object' && result !== null) {
    return 'timeout_error';
  }
  return result;
}
