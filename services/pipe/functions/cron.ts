import { parseExpression } from 'cron-parser';

import { HMSH_FIDELITY_SECONDS } from '../../../modules/enums';
import { isValidCron } from '../../../modules/utils';

/**
 * Safely calculates the delay in seconds until the next execution of a cron job.
 * Fails silently and returns -1 if the cron expression is invalid.
 * @param cronExpression The cron expression to parse (e.g. '0 0 * * *').
 * @returns The delay in seconds until the next cron job execution (minimum 5 seconds).
 */
class CronHandler {
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
      console.error('Error calculating next cron job execution  delay:', error);
      return -1;
    }
  }
}

export { CronHandler };
