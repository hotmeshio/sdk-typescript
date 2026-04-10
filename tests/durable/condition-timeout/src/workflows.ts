import { Durable } from '../../../../services/durable';

// Condition with timeout — signal arrives in time
export async function signalWins(): Promise<string> {
  const result = await Durable.workflow.condition<{ msg: string }>(
    'test-signal',
    '30s',
  );
  if (result === false) return 'timed_out';
  return result.msg;
}

// Condition with timeout — no signal, timeout fires
export async function timeoutWins(): Promise<string> {
  const result = await Durable.workflow.condition<{ msg: string }>(
    'never-signal',
    '5s',
  );
  if (result === false) return 'timed_out';
  return result.msg;
}

// Condition without timeout — backward compatibility
export async function noTimeout(): Promise<string> {
  const result = await Durable.workflow.condition<{ msg: string }>(
    'compat-signal',
  );
  return result.msg;
}
