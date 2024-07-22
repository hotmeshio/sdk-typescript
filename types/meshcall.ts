import { RedisConfig } from "./hotmesh";
import { StreamData } from "./stream";

interface MeshCallExecOptions {
  ttl?: string; 
}

interface MeshCallConnectParams {
  namespace: string;
  topic: string;
  redis: RedisConfig;
  callback: (data: StreamData) => Promise<{ metadata: Record<string, any>; data: Record<string, any> }>;
}

interface MeshCallExecParams {
  namespace: string;
  topic: string;
  args: any[];
  redis: RedisConfig;
  options?: MeshCallExecOptions;
}

interface MeshCallFlushParams {
  namespace: string;
  topic: string;
  redis: RedisConfig;
}

interface MeshCallCronOptions {
  id: string;
  interval: string;
  maxCycles?: number;
  maxDuration?: string;
}

interface MeshCallInterruptOptions {
  id: string;
}

interface MeshCallCronParams {
  namespace: string;
  topic: string;
  redis: RedisConfig;
  callback: () => Promise<void>;
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
