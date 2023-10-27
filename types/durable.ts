import { RedisClass, RedisOptions } from './redis';

type WorkflowConfig = {
  backoffCoefficient?: number; //default 10
  maximumAttempts?: number; //default 2
  maximumInterval?: string; //default 30s
  initialInterval?: string; //default 1s
}

type WorkflowOptions = {
  taskQueue: string;
  args: any[];          //input arguments to pass in
  workflowId: string;   //execution id (the job id)
  workflowName?: string; //the name of the user's workflow function
  workflowTrace?: string;
  workflowSpan?: string;
  config?: WorkflowConfig;
}

type SignalOptions = {
  taskQueue: string;
  data: Record<string, any>; //input data (any serializable object)
  workflowId: string;   //execution id (the job id)
  workflowName?: string; //the name of the user's workflow function
}

type ActivityWorkflowDataType = {
  activityName: string;
  arguments: any[];
  workflowId: string;
  workflowTopic: string;
}

type WorkflowDataType = {
  arguments: any[];
  workflowId: string;
  workflowTopic: string;
}

type ConnectionConfig = {
  class: RedisClass;
  options: RedisOptions;
}
type Connection =  ConnectionConfig;
type NativeConnection =  ConnectionConfig;

type ClientConfig = {
  connection: Connection;
}

type Registry  = {
  [key: string]: Function
};

type WorkerConfig = {
  connection: Connection;
  namespace?: string; //`appid` in the YAML (e.g, 'default')
  taskQueue: string; //`subscribes` in the YAML (e.g, 'hello-world')
  workflow: Function; //target function to run
  options?: WorkerOptions;
}

type WorkerOptions = {
  maxSystemRetries?: number; //1-3 (10ms, 100ms, 1_000ms)
  backoffCoefficient?: number; //2-10ish
}

type ContextType = {
  workflowId: string
  workflowTopic: string
};

type FunctionSignature<T> = T extends (...args: infer A) => infer R ? (...args: A) => R : never;
type ProxyType<ACT> = {
  [K in keyof ACT]: FunctionSignature<ACT[K]>;
};

type ActivityConfig = {
  startToCloseTimeout?: string;
  activities?: any;
  retryPolicy?: {
    initialInterval: string;
    maximumAttempts: number;
    backoffCoefficient: number;
    maximumInterval: string;
  };
};

export {
  ActivityConfig,
  ActivityWorkflowDataType,
  ClientConfig,
  ContextType,
  ConnectionConfig,
  Connection,
  NativeConnection,
  ProxyType,
  Registry,
  SignalOptions,
  WorkerConfig,
  WorkerOptions,
  WorkflowDataType,
  WorkflowOptions,
};
