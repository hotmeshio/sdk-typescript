import { SerializerService } from './common';
import { getContext } from './context';

/**
 * The core of the deterministic replay mechanism. Every durable operation
 * (`proxyActivities`, `execChild`, `sleepFor`, `waitFor`, etc.) calls
 * `didRun` before executing. It increments a shared counter to produce
 * a unique `sessionId` of the form `-{prefix}{dimension}-{index}-`.
 *
 * If that `sessionId` already exists in the `replay` hash (loaded from
 * the job's stored state), the previously persisted result is
 * deserialized and returned — skipping re-execution entirely. This is
 * analogous to Temporal's event history replay.
 *
 * ## Session ID Format
 *
 * ```
 * -{prefix}{workflowDimension}-{execIndex}-
 * ```
 *
 * Examples: `-proxy-1-`, `-child-2-`, `-sleep-0-3-` (dimension `0`).
 *
 * @private
 * @param {string} prefix - Operation type identifier (`'proxy'`, `'child'`, `'start'`, `'wait'`, `'sleep'`).
 * @returns {Promise<[boolean, number, any]>} A tuple:
 *   - `[0]` — `true` if the operation was already executed (replay hit).
 *   - `[1]` — The execution index for this operation.
 *   - `[2]` — The deserialized result (or `null` if not replayed).
 */
export async function didRun(prefix: string): Promise<[boolean, number, any]> {
  const { COUNTER, replay, workflowDimension } = getContext();
  const execIndex = COUNTER.counter = COUNTER.counter + 1;
  const sessionId = `-${prefix}${workflowDimension}-${execIndex}-`;
  if (sessionId in replay) {
    const restored = SerializerService.fromString(replay[sessionId]);
    return [true, execIndex, restored];
  }
  return [false, execIndex, null];
}
