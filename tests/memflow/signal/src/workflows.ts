import { MemFlow } from '../../../../services/memflow';

import * as activities from './activities';
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = MemFlow.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(
  name: string,
): Promise<[string, Record<any, any>, Record<any, any>, string]> {
  const strangerGreeting = await greet('stranger');

  const [signal1, signal2] = await Promise.all([
    MemFlow.workflow.waitFor<Record<any, any>>('abcdefg'),
    MemFlow.workflow.waitFor<Record<any, any>>('hijklmnop'),
  ]);

  return [strangerGreeting, signal1, signal2, await greet(name)];
}
