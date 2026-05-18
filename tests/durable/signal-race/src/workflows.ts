import { Durable } from '../../../../services/durable';

/**
 * Parent workflow: registers a condition (hook) and waits for a signal.
 * Under concurrent load, the signal may arrive before the hook registers.
 */
export async function parent(id: string): Promise<string> {
  const signalId = `race-signal-${id}`;
  const result = await Durable.workflow.condition<{ value: string }>(
    signalId,
  );
  if (result === false) return 'timed_out';
  return result.value;
}

/**
 * Child workflow: immediately signals the parent.
 * The speed of this signal is what creates the race — under load
 * this signal can be processed before the parent's hook registers.
 */
export async function child(
  signalId: string,
  payload: string,
): Promise<void> {
  await Durable.workflow.signal(signalId, { value: payload });
}
