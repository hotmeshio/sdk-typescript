import { Durable } from '../../../../services/durable';

import * as activities from './activities';

const { slowEcho } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
  retry: {
    maximumAttempts: 3,
    maximumInterval: '1s',
    backoffCoefficient: 1,
  },
});

/** One activity whose duration the test controls, so a fault can land inside it. */
export async function resilientEcho(name: string, delayMs: number): Promise<string> {
  return await slowEcho(name, delayMs);
}

/** Two sequential activities: the second runs after the engine resumes. */
export async function resilientPair(name: string, delayMs: number): Promise<string[]> {
  const first = await slowEcho(`${name}-a`, delayMs);
  const second = await slowEcho(`${name}-b`, delayMs);
  return [first, second];
}
