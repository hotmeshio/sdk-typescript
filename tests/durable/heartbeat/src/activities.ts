let executionCount = 0;

export function getExecutionCount(): number {
  return executionCount;
}

export function resetExecutionCount(): void {
  executionCount = 0;
}

/**
 * An activity that outlives the base reservation window (30s default).
 * The reservation heartbeat keeps the message leased while it runs, so
 * it executes exactly once and its result is delivered.
 */
export async function outliveTheLease(runId: string): Promise<string> {
  executionCount++;
  await new Promise((resolve) => setTimeout(resolve, 45_000));
  return `leased:${runId}`;
}
