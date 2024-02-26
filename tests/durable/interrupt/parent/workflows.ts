import { Durable } from '../../../../services/durable';

export async function parentExample(name: string): Promise<Record<string, string>> {
  const workflowId1 = 'jimbo1';
  const workflowId2 = 'jimbo2';

  const childWorkflowOutput1 = await Durable.workflow.executeChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
    workflowId: workflowId1,
  });

  const childWorkflowOutput2 = await Durable.workflow.startChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
    workflowId: workflowId2,
  });

  //the test will check if this workflow is cancelled
  await Durable.workflow.interrupt(workflowId2, { throw: false }) as string;
  
  return { childWorkflowOutput: childWorkflowOutput1, cancelledWorkflowId: childWorkflowOutput2 };
}
