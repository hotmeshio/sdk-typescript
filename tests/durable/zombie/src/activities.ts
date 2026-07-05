let sideEffectCount = 0;

export function getSideEffectCount(): number {
  return sideEffectCount;
}

export function resetSideEffectCount(): void {
  sideEffectCount = 0;
}

/**
 * A slow activity with an observable side effect. The side effect
 * (counter increment) happens FIRST, so every execution — including
 * a zombie re-execution after the workflow was terminated — is counted.
 */
export async function slowSideEffect(runId: string): Promise<string> {
  sideEffectCount++;
  await new Promise((resolve) => setTimeout(resolve, 5_000));
  return runId;
}
