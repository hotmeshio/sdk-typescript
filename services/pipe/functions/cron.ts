import { parseExpression } from 'cron-parser';

import { HMSH_FIDELITY_SECONDS } from '../../../modules/enums';
import { isValidCron } from '../../../modules/utils';
import { ILogger } from '../../../types/logger';
import { LoggerService } from '../../logger';

/**
 * Provides cron-related utility functions based on the
 * [cron](https://en.wikipedia.org/wiki/Cron) format for use
 * in HotMesh mapping rules.
 *
 * @remarks
 * Invoked in mapping rules using `{@cron.<method>}` syntax.
 */
class CronHandler {
  static logger: ILogger = new LoggerService('hotmesh', 'cron');

  /**
   * Safely calculates the delay in seconds until the next execution
   * of a cron job. Calculates the next date and time (per the system
   * clock) that satisfies the cron expression, then returns the value
   * in seconds from now. Fails silently and returns `-1` if the cron
   * expression is invalid or the next execution time is in the past.
   *
   * @param {string} cronExpression - The cron expression to parse (e.g. `'0 0 * * *'`)
   * @returns {number} The delay in seconds until the next cron job execution (minimum `HMSH_FIDELITY_SECONDS`), or `-1` on error
   * @example
   * ```yaml
   * cron_next_result:
   *   "@pipe":
   *     - ["{a.expressions.cron}"]
   *     - ["{@cron.nextDelay}"]
   * ```
   */
  nextDelay(cronExpression: string): number {
    try {
      if (!isValidCron(cronExpression)) {
        return -1;
      }
      const interval = parseExpression(cronExpression, { utc: true });
      const nextDate = interval.next().toDate();
      const now = new Date();
      const delay = (nextDate.getTime() - now.getTime()) / 1000;
      if (delay <= 0) {
        return -1;
      }
      if (delay < HMSH_FIDELITY_SECONDS) {
        return HMSH_FIDELITY_SECONDS;
      }
      const iDelay = Math.round(delay);
      return iDelay;
    } catch (error) {
      CronHandler.logger.error(
        'Error calculating next cron job execution  delay:',
        { error },
      );
      return -1;
    }
  }
}

export { CronHandler };
