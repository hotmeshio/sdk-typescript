import { Durable } from '../../../../services/durable';

import * as activities from './activities';
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = Durable.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<string> {
  //run a proxy activity
  const yo = await greet(name);

  //ALWAYS use Durable.workflow.sleepFor as its deterministic
  await Durable.workflow.sleepFor('1 seconds');

  //sleep for 2 more
  await Durable.workflow.sleepFor('2 seconds');

  return yo;
}
