import { ProviderConfig, ProvidersConfig } from './provider';
import { LogLevel } from './logger';

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
  /**
   * Log level for the worker
   */
  logLevel?: LogLevel;
  /**
   * Idempotent GUID for the worker and engine
   */
  guid?: string;
  /**
   * Namespace for grouping common functions
   */
  namespace?: string;
  /**
   * Unique topic for the worker function
   */
  topic: string;
  /**
   * Redis configuration; use 'connection' instead of 'redis'
   * @deprecated
   */
  redis?: ProviderConfig;
  /**
   * Provider configuration
   */
  connection?: ProviderConfig | ProvidersConfig;
  /**
   * The linked worker function that will be called; optional if read only
   */
  callback?: (...args: any[]) => any;
}

interface MeshCallExecParams {
  /**
   * namespace for grouping common functions
   */
  namespace?: string;
  /**
   * topic assigned to the worker when it was connected
   */
  topic: string;
  /**
   * Arguments to pass to the worker function
   */
  args: any[];
  /**
   * Redis configuration; use 'connection' instead of 'redis'
   * @deprecated
   */
  redis?: ProviderConfig;
  /**
   * Provider configuration
   */
  connection?: ProviderConfig | ProvidersConfig;
  /**
   * Execution options like caching ttl
   */
  options?: MeshCallExecOptions;
}

interface MeshCallFlushOptions {
  /**
   * Cache id when caching/flushing/retrieving function results.
   */
  id: string;
}

interface MeshCallFlushParams {
  /**
   * namespace for grouping common functions
   */
  namespace?: string;
  /**
   * id for cached response to flush
   */
  id?: string;
  /**
   * topic assigned to the worker when it was connected
   */
  topic: string;
  /**
   * Redis configuration; use 'connection' instead of 'redis'
   * @deprecated
   */
  redis?: ProviderConfig;
  /**
   * Provider configuration
   */
  connection?: ProviderConfig | ProvidersConfig;
  /**
   * Options for the flush
   */
  options?: MeshCallFlushOptions;
}
interface MeshCallCronOptions {
  /**
   * Idempotent GUID for the function
   * */
  id: string;
  /**
   * For example, `1 day`, `1 hour`. Fidelity is generally
   * within 5 seconds. Refer to the syntax for the `ms` NPM package.
   * Standard cron syntax is also supported. (e.g. `0 0 * * *`)
   */
  interval: string;
  /**
   * Maximum number of cycles to run before exiting the cron.
   */
  maxCycles?: number;
  /**
   * Time in seconds to sleep before invoking the first cycle.
   * For example, `1 day`, `1 hour`. Fidelity is generally
   * within 5 seconds. Refer to the syntax for the `ms` NPM package.
   * If the interval field uses standard cron syntax, this field is ignored.
   */
  delay?: string;
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
   * Redis configuration; use 'connection' instead of 'redis'
   * @deprecated
   */
  redis?: ProviderConfig;
  /**
   * Provider configuration
   */
  connection?: ProviderConfig | ProvidersConfig;
  /**
   * Arguments to pass to the cron job; arguments will be passed to the callback
   * each time it runs
   */
  args: any[];
  /**
   * linked worker function to run; if not provided, the system will
   * attempt to start the cron job using the topic, but a new
   * worker will not be created. This is useful for spawning a cron job
   * from an ephemeral node process.
   */
  callback?: (...args: any[]) => any;
  /**
   * Options for the cron job
   */
  options: MeshCallCronOptions;
}

interface MeshCallInstanceOptions {
  /**
   * if true, the connection to HotMesh will be in readonly mode
   * and the instantiated client will not route messages
   * @default false
   */
  readonly?: boolean;
  /**
   * Idempotent GUID for the worker and engine
   */
  guid?: string;
}

interface MeshCallInterruptParams {
  /**
   * namespace for grouping common functions
   */
  namespace?: string;
  /**
   * topic assigned to the cron worker when it was connected
   */
  topic: string;
  /**
   * Redis configuration; use 'connection' instead of 'redis'
   * @deprecated
   */
  redis?: ProviderConfig;
  /**
   * Provider configuration
   */
  connection?: ProviderConfig | ProvidersConfig;
  /**
   * Options for interrupting the cron
   */
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
  MeshCallFlushOptions,
  MeshCallFlushParams,
  MeshCallInstanceOptions,
};
