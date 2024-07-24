import { RedisConfig } from "./hotmesh";
import { LogLevel } from "./logger";

interface MeshCallExecOptions {
  /**
   * Cache id when caching, flushing and retrieving function results.
   */
  id: string;
  /**
   * Time to live for the cache key. For example, `1 day`, `1 hour`. Refer to the syntax for the `ms` NPM package.
   */
  ttl?: string; 
  /**
   * If true, the cache will first be flushed and the function will be executed.
   */
  flush?: boolean;
}
interface MeshCallConnectParams {
  logLevel?: LogLevel;
  guid?: string; // Assign a custom engine id
  namespace?: string;
  topic: string;
  redis: RedisConfig;
  callback: <T extends any[], U>(...args: T) => Promise<U>;
}

interface MeshCallExecParams {
  namespace?: string;
  topic: string;
  args: any[];
  redis: RedisConfig;
  options?: MeshCallExecOptions;
}

interface MeshCallFlushParams {
  namespace?: string;
  id: string;
  topic: string;
  redis: RedisConfig;
}

interface MeshCallCronOptions {
  /**
   * Idempotent GUID for the function
   * */
  id: string;
  /** 
   * For example, `1 day`, `1 hour`. Fidelity is generally
   * within 5 seconds. Refer to the syntax for the `ms` NPM package.
   */
  interval: string;
  /**
   * Maximum number of cycles to run before stopping.
   */
  maxCycles?: number;
  /**
   * Maximum duration to run before stopping the cron.
   * Refer to the syntax for the `ms` NPM package.
   */
  maxDuration?: string;
}

interface MeshCallInterruptOptions {
  /**
   * Idempotent GUID for the cron function
   */
  id: string;
}

interface MeshCallCronParams {
  /**
   * Log level for the cron
   */
  logLevel?: LogLevel;
  /**
   * Idempotent GUID for the worker and engine used for the cron
   */
  guid?: string;
  /**
   * Namespace for grouping common cron functions. The cron job keys in Redis
   * will be prefixed with this namespace. (e.g. `hmsh:[namespace]:j:*`)
   */
  namespace?: string;
  /**
   * Unique topic for the cron function to identify the worker
   */
  topic: string;
  /**
   * Redis configuration for the cron job
   */
  redis: RedisConfig;
  /**
   * Arguments to pass to the cron job; arguments will be passed to the callback
   * each time it runs
   */
  args: any[];
  /**
   * Callback function to invoke each time the cron job runs; `args` will be passed
   */
  callback: <T extends any[], U>(...args: T) => Promise<U>;
  /**
   * Options for the cron job
   */
  options: MeshCallCronOptions;
}

interface MeshCallInterruptParams {
  namespace?: string;
  topic: string;
  redis: RedisConfig;
  options: MeshCallInterruptOptions;
}

export {
  MeshCallConnectParams,
  MeshCallExecParams,
  MeshCallCronParams,
  MeshCallExecOptions,
  MeshCallCronOptions,
  MeshCallInterruptOptions,
  MeshCallInterruptParams,
  MeshCallFlushParams,
};
