import { parseExpression } from 'cron-parser';

import { HMSH_FIDELITY_SECONDS } from '../../../modules/enums';
import { isValidCron } from '../../../modules/utils';

/**
 * Provides cron-related utility functions based on the
 * [cron](https://en.wikipedia.org/wiki/Cron) format for use
 * in HotMesh mapping rules.
 *
 * @remarks
 * Invoked in mapping rules using `{@cron.<method>}` syntax.
 */
class CronHandler {

  /**
   * Calculates the delay in seconds until the next execution
   * of a cron job. Throws on invalid expressions rather than
   * degrading silently.
   *
   * @param {string} cronExpression - The cron expression to parse (e.g. `'0 0 * * *'`)
   * @returns {number} The delay in seconds until the next cron job execution (minimum `HMSH_FIDELITY_SECONDS`)
   * @throws {Error} If the cron expression is invalid
   * @example
   * ```yaml
   * cron_next_result:
   *   "@pipe":
   *     - ["{a.expressions.cron}"]
   *     - ["{@cron.nextDelay}"]
   * ```
   */
  nextDelay(cronExpression: string): number {
    if (cronExpression == null || typeof cronExpression !== 'string') {
      return -1;
    }
    if (!isValidCron(cronExpression)) {
      throw new Error(`Invalid cron expression: ${cronExpression}`);
    }
    const interval = parseExpression(cronExpression, { utc: true });
    const nextDate = interval.next().toDate();
    const now = new Date();
    const delay = (nextDate.getTime() - now.getTime()) / 1000;
    if (delay <= 0) {
      return HMSH_FIDELITY_SECONDS;
    }
    return Math.max(Math.round(delay), HMSH_FIDELITY_SECONDS);
  }
}

export { CronHandler };
