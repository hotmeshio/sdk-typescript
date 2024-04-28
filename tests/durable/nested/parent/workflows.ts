import { Durable } from '../../../../services/durable';
import * as activities from './activities';
import type * as activityTypes from './activities';

const { parentActivity } = Durable.workflow
  .proxyActivities<typeof activityTypes>({
    activities
  });

export async function parentExample(name: string): Promise<Record<string, string>> {
  const activityOutput = await parentActivity(name);
  const childWorkflowOutput = await Durable.workflow.execChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
  });
  return { activityOutput, childWorkflowOutput };
}
