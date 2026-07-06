import { Durable } from '../../../../services/durable';

// Parks on condition(), writing one hmsh_escalations row at suspension.
// Resolving that row must wake this workflow even if the resolver process
// dies immediately after the resolve transaction commits.
export async function crashWindowWorkflow(
  runId: string,
): Promise<{ approved: boolean; via: string }> {
  const signalId = `wake-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean; via: string }>(
    signalId,
    {
      role: 'crash-approver',
      type: 'wake-atomicity',
      priority: 1,
      description: `crash-window wake for ${runId}`,
      metadata: { runId },
    },
  );
  return result as { approved: boolean; via: string };
}
