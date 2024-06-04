import { Durable } from '../../../../services/durable';

import * as activities from './activities';

//NOTE: when `./activities` exports a `default` function,
//      it is imported as shown here (using the type)
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = Durable.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<string> {
  const random1 = Durable.workflow.random();
  const proxyGreeting = await greet(name);
  const random2 = Durable.workflow.random();
  const oneTimeGreeting = await Durable.workflow.once<string>(
    activities.default,
    name,
  );
  return `${random1} ${proxyGreeting} ${random2} ${oneTimeGreeting}`;
}
