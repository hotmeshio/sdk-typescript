import { Durable } from '../../../../services/durable';

import * as activities from './activities';

const { outliveTheLease } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
});

export async function heartbeatExample(runId: string): Promise<string> {
  return await outliveTheLease(runId);
}
