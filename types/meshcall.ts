import { RedisConfig } from "./hotmesh";
import { StreamData } from "./stream";

// Define the interface for the execution options, including caching
interface MCExecOptions {
  ttl?: string;  // time to live for caching the response
}

// Define the interface for the connection options
interface MCConnectOptions {
  namespace: string;
  topic: string;
  redis: RedisConfig;
  callback: (data: StreamData) => Promise<{ metadata: Record<string, any>; data: Record<string, any> }>;
}

// Define the interface for the execution parameters
interface MCExecParams {
  namespace: string;
  topic: string;
  payload: Record<string, any>;
  redis: RedisConfig;
  options?: MCExecOptions;
}

// Define the interface for cron job options
interface MCCronOptions {
  interval: string;
  maxCycles?: number;
  maxDuration?: string;
}

// Define the interface for the cron job parameters
interface MCCronParams {
  namespace: string;
  topic: string;
  redis: RedisConfig;
  callback: () => Promise<void>;
  options: MCCronOptions;
}

export { MCConnectOptions, MCExecParams, MCCronParams, MCExecOptions, MCCronOptions };
