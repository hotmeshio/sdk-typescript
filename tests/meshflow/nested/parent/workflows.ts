import { MeshFlow } from '../../../../services/meshflow';

import * as activities from './activities';
import type * as activityTypes from './activities';

const { parentActivity } = MeshFlow.workflow.proxyActivities<
  typeof activityTypes
>({
  activities,
});

export async function parentExample(
  name: string,
  signalIn: boolean,
): Promise<Record<string, string>> {
  const activityOutput = await parentActivity(name);
  //tests signal suppression within collated sets
  const [childWorkflowOutput] = await Promise.all([
    MeshFlow.workflow.execChild<string>({
      args: [`${name} to CHILD`],
      taskQueue: 'child-world',
      workflowName: 'childExample',
      signalIn,
    }),
    MeshFlow.workflow.execChild<string>({
      args: [`${name} to CHILD 2`],
      taskQueue: 'child-world',
      workflowName: 'childExample',
      signalIn,
    }),
  ]);
  return { activityOutput, childWorkflowOutput };
}
