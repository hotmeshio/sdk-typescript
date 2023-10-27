import { Durable } from '../../../../services/durable';
import * as activities from './activities';

import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = Durable.workflow
  .proxyActivities<ActivitiesType>({ activities });

export async function example(name: string): Promise<string> {
  //ALWAYS use Durable.workflow.sleep as its deterministic
  await Durable.workflow.sleep('1 seconds');

  //sleep for 2 more
  await Durable.workflow.sleep('2 seconds');

  //run a proxy activity and return
  return await greet(name);
}
