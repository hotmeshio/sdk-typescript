import { Durable } from '../../../../services/durable';

import * as activities from './activities';

const { slowSideEffect } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
});

export async function zombieExample(runId: string): Promise<string> {
  await slowSideEffect(runId);
  return 'done';
}
