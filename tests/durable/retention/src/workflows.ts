import { Durable } from '../../../../services/durable';

// Parks on condition() with metadata. The pending row must survive pruning
// even when its updated_at is older than the retention horizon — pending is
// live state, terminal is audit history.
export async function retentionWaiter(caseId: string): Promise<{ resolved: boolean; resolvedBy: string }> {
  const signalId = `retention-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ resolved: boolean; resolvedBy: string }>(
    signalId,
    {
      role: 'retention-waiter',
      type: 'retention-gate',
      priority: 3,
      description: `Retention live waiter for ${caseId}`,
      metadata: { caseId },
    },
  );
  return result as { resolved: boolean; resolvedBy: string };
}
