import { Durable } from '../../../../services/durable';
import type * as activities from './activities';

const { parentActivity } = Durable.workflow.proxyActivities<typeof activities>();

export async function parentExample(name: string): Promise<Record<string, string>> {
  const activityOutput = await parentActivity(name);
  const childWorkflowOutput = await Durable.workflow.executeChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
    workflowId: '-'
  });
  return { activityOutput, childWorkflowOutput };
}
