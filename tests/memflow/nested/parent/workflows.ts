import { MemFlow } from '../../../../services/memflow';

import * as activities from './activities';
import type * as activityTypes from './activities';

const { parentActivity } = MemFlow.workflow.proxyActivities<
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
    MemFlow.workflow.execChild<string>({
      args: [`${name} to CHILD`],
      taskQueue: 'child-world',
      workflowName: 'childExample',
      signalIn,
    }),
    MemFlow.workflow.execChild<string>({
      args: [`${name} to CHILD 2`],
      taskQueue: 'child-world',
      workflowName: 'childExample',
      signalIn,
    }),
  ]);
  return { activityOutput, childWorkflowOutput };
}
