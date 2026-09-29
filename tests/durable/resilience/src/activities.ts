import { sleepFor } from '../../../../modules/utils';

/** Executions per name, so a test can see an activity redelivered. */
export const executions: Record<string, number> = {};

export async function slowEcho(name: string, delayMs: number): Promise<string> {
  executions[name] = (executions[name] ?? 0) + 1;
  await sleepFor(delayMs);
  return `echo:${name}`;
}
