//NOTE: testing the default index file that users access when importing npm package
import { workflow, proxyActivities } from '../../../../index';

import * as activities from './activities';

//NOTE: when `./activities` exports a `default` function,
//      it is imported as shown here (using the type)
import type greetFunctionType from './activities';
type ActivitiesType = { greet: typeof greetFunctionType };

const { greet } = proxyActivities<ActivitiesType>({ activities });

export async function example(name: string): Promise<string> {
  const random1 = workflow.random();
  const proxyGreeting = await greet(name);
  const random2 = workflow.random();
  return `${random1} ${proxyGreeting} ${random2}`;
}
