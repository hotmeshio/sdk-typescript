import { RedisConfig } from "./hotmesh";
import { LogLevel } from "./logger";
import { StreamData } from "./stream";

interface MeshCallExecOptions {
  id: string;
  ttl?: string; 
  flush?: boolean;
}
interface MeshCallConnectParams {
  logLevel?: LogLevel;
  guid?: string; // Assign a custom engine id
  namespace: string;
  topic: string;
  redis: RedisConfig;
  callback: <T extends any[], U>(...args: T) => Promise<U>;
}

interface MeshCallExecParams {
  namespace: string;
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
   * Initial arguments for the cron; these will be provided each run
   * of the cron to the linked callback.
   */
  args?: string;
  /**
   * Idempotent GUID for the function
   * */
  id: string;
  /** 
   * For example, `1 day`, `1 hour`. Fidelity is generally
   * within 5 seconds.
   */
  interval: string;
  /**
   * Maximum number of cycles to run before stopping.
   */
  maxCycles?: number;
  /**
   * Maximum duration to run before stopping.
   */
  maxDuration?: string;
}

interface MeshCallInterruptOptions {
  id: string;
}

interface MeshCallCronParams {
  logLevel?: LogLevel;
  guid?: string; // Assign a custom engine id
  namespace: string;
  topic: string;
  redis: RedisConfig;
  args: any[];
  callback: <T extends any[], U>(...args: T) => Promise<U>;
  options: MeshCallCronOptions;
}

interface MeshCallInterruptParams {
  namespace: string;
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
