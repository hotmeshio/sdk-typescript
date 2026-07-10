import { Durable } from '../../../../services/durable';

import * as activities from './activities';

// The long-batch shape: one activity that outlives the base reservation
// window, declared with a startToCloseTimeout sized to the work. The
// reservation heartbeat holds the lease for the full run, independent of
// the declared timeout.
const { outliveTheLease } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
  startToCloseTimeout: '120 seconds',
  retry: { maximumAttempts: 1 },
});

export async function heartbeatExample(runId: string): Promise<string> {
  return await outliveTheLease(runId);
}
