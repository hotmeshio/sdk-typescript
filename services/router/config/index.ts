import {
  HMSH_BLOCK_TIME_MS,
  HMSH_MAX_RETRIES,
  HMSH_MAX_TIMEOUT_MS,
  HMSH_GRADUATED_INTERVAL_MS,
  HMSH_CODE_UNACKED,
  HMSH_CODE_UNKNOWN,
  HMSH_STATUS_UNKNOWN,
  HMSH_XCLAIM_COUNT,
  HMSH_XCLAIM_DELAY_MS,
  HMSH_XPENDING_COUNT,
  MAX_DELAY,
  MAX_STREAM_BACKOFF,
  INITIAL_STREAM_BACKOFF,
  MAX_STREAM_RETRIES,
} from '../../../modules/enums';
import { RouterConfig } from '../../../types/stream';

export class RouterConfigManager {
  static validateThrottle(delayInMillis: number): void {
    if (
      !Number.isInteger(delayInMillis) ||
      delayInMillis < 0 ||
      delayInMillis > MAX_DELAY
    ) {
      throw new Error(
        `Throttle must be a non-negative integer and not exceed ${MAX_DELAY} ms; send -1 to throttle indefinitely`,
      );
    }
  }

  static setDefaults(config: RouterConfig): RouterConfig & {
    reclaimDelay: number;
    reclaimCount: number;
    readonly: boolean;
  } {
    return {
      ...config,
      reclaimDelay: config.reclaimDelay || HMSH_XCLAIM_DELAY_MS,
      reclaimCount: config.reclaimCount || HMSH_XCLAIM_COUNT,
      readonly: config.readonly || false,
    };
  }
}

export {
  HMSH_BLOCK_TIME_MS,
  HMSH_MAX_RETRIES,
  HMSH_MAX_TIMEOUT_MS,
  HMSH_GRADUATED_INTERVAL_MS,
  HMSH_CODE_UNACKED,
  HMSH_CODE_UNKNOWN,
  HMSH_STATUS_UNKNOWN,
  HMSH_XCLAIM_COUNT,
  HMSH_XCLAIM_DELAY_MS,
  HMSH_XPENDING_COUNT,
  MAX_DELAY,
  MAX_STREAM_BACKOFF,
  INITIAL_STREAM_BACKOFF,
  MAX_STREAM_RETRIES,
};
