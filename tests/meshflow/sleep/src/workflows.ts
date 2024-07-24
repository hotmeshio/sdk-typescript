import { MeshFlow } from '../../../../services/meshflow';

import * as activities from './activities';
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = MeshFlow.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<string> {
  //run a proxy activity
  const yo = await greet(name);

  //ALWAYS use MeshFlow.workflow.sleepFor as its deterministic
  await MeshFlow.workflow.sleepFor('1 seconds');

  //sleep for 2 more
  await MeshFlow.workflow.sleepFor('2 seconds');

  return yo;
}
