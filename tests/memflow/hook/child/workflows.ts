import { MemFlow } from '../../../../services/memflow';

import * as activities from './activities';

const { childActivity } = MemFlow.workflow.proxyActivities<typeof activities>({
  activities,
});

export async function childExample(name: string): Promise<string> {
  return await childActivity(name);
}
