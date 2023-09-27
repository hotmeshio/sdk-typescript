import { RedisClass, RedisOptions } from './redis';

type WorkflowOptions = {
  taskQueue: string;
  args: any[];          //input arguments to pass in
  workflowId: string;   //execution id (the job id)
  workflowName?: string; //the name of the user's workflow function
  workflowTrace?: string;
  workflowSpan?: string;
}

type ActivityDataType = {
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
  namespace: string; //`appid` in the YAML (e.g, 'default')
  taskQueue: string; //`subscribes` in the YAML (e.g, 'hello-world')
  workflowsPath: string; //resolved abs path to dyn import()
  activities: { [key: string]: Function }; //vanilla activity exports
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
  startToCloseTimeout: string;
  retryPolicy: {
    initialInterval: string;
    maximumAttempts: number;
    backoffCoefficient: number;
    maximumInterval: string;
  };
};

export {
  ActivityConfig,
  ActivityDataType,
  ClientConfig,
  ContextType,
  ConnectionConfig,
  Connection,
  NativeConnection,
  ProxyType,
  Registry,
  WorkerConfig,
  WorkflowDataType,
  WorkflowOptions,
};