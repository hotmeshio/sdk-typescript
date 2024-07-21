import { MeshFlow } from '../../../../services/meshflow';

export async function parentExample(
  name: string,
): Promise<Record<string, string>> {
  const workflowId1 = 'jimbo1';
  const workflowId2 = 'jimbo2';
  const childWorkflowOutput1 = await MeshFlow.workflow.execChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
    workflowId: workflowId1,
  });
  const childWorkflowOutput2 = await MeshFlow.workflow.startChild({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
    workflowId: workflowId2,
    expire: 600, //don't expire immediately once complete
  });
  //interrupted flows are stopped immediately, while an async cascade is triggered
  // the workflow will also be expired/scrubbed from Redis in 600 seconds (default 1s)
  (await MeshFlow.workflow.interrupt(workflowId2, {
    throw: false,
    expire: 600,
  })) as string;
  return {
    childWorkflowOutput: childWorkflowOutput1,
    cancelledWorkflowId: childWorkflowOutput2,
  };
}
