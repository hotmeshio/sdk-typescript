import { MemFlow } from '../../../../services/memflow';

import * as activities from './activities';
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = MemFlow.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<string> {
  //run a proxy activity
  const yo = await greet(name);

  //ALWAYS use MemFlow.workflow.sleepFor as its deterministic
  await MemFlow.workflow.sleepFor('1 seconds');

  //sleep for 2 more
  await MemFlow.workflow.sleepFor('2 seconds');

  return yo;
}
