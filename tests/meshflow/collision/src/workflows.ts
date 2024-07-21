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
  const greet1 = await greet(name);
  await MeshFlow.workflow.execChild<string>({
    args: ['Howdy!'],
    taskQueue: 'collision-world',
    workflowName: 'childExample',
    workflowId: 'collision-child', //the parent is already named this
  });
  //should never return as error will be thrown
  return greet1;
}

export async function childExample(name: string): Promise<string> {
  return await greet(name);
}

const STATE = {
  count: 0,
};

export async function fixableExample(badCount: number): Promise<string> {
  //add unique suffix to workflowId after `badCount` failures
  const uniqueSuffix = STATE.count++ > badCount ? '-fixed' : '';

  const childOutput = await MeshFlow.workflow.execChild<string>({
    args: ['FIXED'],
    taskQueue: 'collision-world',
    workflowName: 'childExample',
    workflowId: `fixable-collision-child${uniqueSuffix}`,
  });
  return childOutput;
}
