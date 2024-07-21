import { MeshFlow } from '../../../../services/meshflow';

import * as activities from './activities';

//NOTE: when `./activities` exports a `default` function,
//      it is imported as shown here (using the type)
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = MeshFlow.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<string> {
  const random1 = MeshFlow.workflow.random();
  const proxyGreeting = await greet(name);
  const random2 = MeshFlow.workflow.random();
  const oneTimeGreeting = await MeshFlow.workflow.once<string>(
    activities.default,
    name,
  );
  return `${random1} ${proxyGreeting} ${random2} ${oneTimeGreeting}`;
}
