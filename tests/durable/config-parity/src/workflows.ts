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

// Positive test: patched returns true for new workflows, taking the new code path
export async function patchedWorkflow(orderId: string): Promise<string> {
  const { validateOrderV2, validateOrder } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: { maximumAttempts: 3 },
  });

  if (await Durable.workflow.patched('v2-validation')) {
    return await validateOrderV2(orderId);
  } else {
    return await validateOrder(orderId);
  }
}

// Negative test: multiple patches in the same workflow all return true on first execution
export async function multiPatchWorkflow(): Promise<string> {
  const { validateOrderV2, validateOrder } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: { maximumAttempts: 3 },
  });

  const patchA = await Durable.workflow.patched('change-a');
  const patchB = await Durable.workflow.patched('change-b');

  if (patchA && patchB) {
    return 'both-patched';
  }
  return 'not-all-patched';
}

// Test: deprecatePatch is a no-op that doesn't break execution
export async function deprecatePatchWorkflow(orderId: string): Promise<string> {
  const { validateOrderV2 } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: { maximumAttempts: 3 },
  });

  Durable.workflow.deprecatePatch('v2-validation');
  return await validateOrderV2(orderId);
}

// Hook function that uses patched inside dimensional isolation
export async function patchedHookFunction(
  signalInfo?: { signal: string; $durable: boolean },
): Promise<string> {
  const { hookTaskV2, hookTaskV1 } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: { maximumAttempts: 3 },
  });

  let result: string;
  if (await Durable.workflow.patched('hook-v2-task')) {
    result = await hookTaskV2();
  } else {
    result = await hookTaskV1();
  }

  // Signal the parent workflow with the result
  if (signalInfo?.signal) {
    await Durable.workflow.signal(signalInfo.signal, { data: result });
  }
  return result;
}

// Parent workflow that spawns a hook with patched inside it
export async function patchedInHookWorkflow(): Promise<string> {
  const result = await Durable.workflow.execHook({
    taskQueue: 'patched-hook',
    workflowName: 'patchedHookFunction',
    args: [],
    signalId: 'hook-done',
  });
  return result as string;
}

// Positive test: continueAsNew restarts with new args until cursor exhausted
export async function continueAsNewWorkflow(cursor = 1, totalProcessed = 0): Promise<number> {
  const { processBatch } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: { maximumAttempts: 3 },
  });

  const result = await processBatch(cursor);
  const newTotal = totalProcessed + result.processed;

  if (result.nextCursor !== null) {
    // Restart with fresh history, carrying forward state via args
    await Durable.workflow.continueAsNew(result.nextCursor, newTotal);
  }
  // Final iteration — return accumulated total
  return newTotal;
}

// Negative test: continueAsNew is terminal — code after it is unreachable
export async function continueAsNewTerminalWorkflow(iteration = 0): Promise<string> {
  const { processBatch } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retryPolicy: { maximumAttempts: 3 },
  });

  await processBatch(iteration);

  if (iteration < 1) {
    await Durable.workflow.continueAsNew(iteration + 1);
    // This code should NEVER execute
    return 'unreachable';
  }
  return `completed-at-${iteration}`;
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
