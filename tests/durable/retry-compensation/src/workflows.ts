import { Durable } from '../../../../services/durable';
import {
  DurableFatalError,
  DurableMaxedError,
  DurableTimeoutError,
} from '../../../../modules/errors';

import * as activities from './activities';
import { state } from './state';

// Static retry policy. The author writes nothing about retrying — the
// framework re-invokes the activity automatically up to the configured
// limit, then surfaces a terminal error once the retries are exhausted.
const { unreliableActivity } = Durable.workflow.proxyActivities<
  typeof activities
>({
  activities,
  retry: {
    maximumAttempts: 2, //retried automatically before giving up
    maximumInterval: '1s', //keep short for testing
    backoffCoefficient: 1, //keep short for testing
  },
});

// The main workflow lets the framework exhaust the retries, CATCHES the
// terminal failure, communicates the error details in its return envelope,
// and completes SUCCESSFULLY (no rethrow) so `handle.result()` resolves.
//
// Re-entry signals (proxy dispatch, sleep, waitFor, child) must bubble so
// the engine can suspend and replay — only the terminal activity failure
// is handled here.
async function example({
  orderId,
}: Record<'orderId', string>): Promise<Record<string, any>> {
  try {
    const processed = await unreliableActivity(orderId);
    return { ok: true, orderId, processed, error: null, attempts: state.attempts };
  } catch (err) {
    const isTerminalActivityFailure =
      err instanceof DurableMaxedError ||
      err instanceof DurableFatalError ||
      err instanceof DurableTimeoutError;
    if (!isTerminalActivityFailure) throw err;
    return {
      ok: false,
      orderId,
      error: {
        name: (err as Error).name,
        message: (err as Error).message,
        code: (err as any).code,
      },
      attempts: state.attempts,
    };
  }
}

export default { example };
