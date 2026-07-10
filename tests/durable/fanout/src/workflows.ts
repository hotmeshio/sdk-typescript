import { Durable } from '../../../../services/durable';

/**
 * Harvest fan-out: opens `count` condition() waits at once and harvests
 * them with Promise.all — the market-maker shape. Signal IDs are
 * deterministic so a signaler can fire before any wait registers.
 */
export async function fanout(caseId: string, count: number): Promise<string[]> {
  const ids = Array.from({ length: count }, (_, i) => `fanout-${caseId}-${i}`);
  const results = await Promise.all(
    ids.map((id) => Durable.workflow.condition<{ v: string }>(id)),
  );
  return results.map((r) => (r === false ? 'timed_out' : (r as { v: string }).v));
}
