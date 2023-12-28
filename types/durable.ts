import { RedisClass, RedisOptions } from './redis';

type WorkflowConfig = {
  backoffCoefficient?: number; //default 10
  maximumAttempts?: number; //default 2
  maximumInterval?: string; //default 30s
  initialInterval?: string; //default 1s
}

type WorkflowSearchOptions = {
  index?: string;         //FT index name (myapp:myindex)
  prefix?: string[];      //FT prefixes (['myapp:myindex:prefix1', 'myapp:myindex:prefix2'])
  schema?: Record<string, {type: 'TEXT' | 'NUMERIC' | 'TAG', sortable?: boolean}>;
  data?: Record<string, string>;
}

type WorkflowOptions = {
  namespace?: string;   //'durable' is the default namespace if not provided; similar to setting `appid` in the YAML
  taskQueue: string;
  args: any[];          //input arguments to pass in
  workflowId?: string;   //execution id (the job id)
  workflowName?: string; //the name of the user's workflow function
  parentWorkflowId?: string;  //system reserved; the id of the parent; if present the flow will not self-clean until the parent that spawned it self-cleans
  workflowTrace?: string;
  workflowSpan?: string;
  search?: WorkflowSearchOptions
  config?: WorkflowConfig;
}

type HookOptions = {
  namespace?: string;   //'durable' is the default namespace if not provided; similar to setting `appid` in the YAML
  taskQueue?: string;
  args: any[];          //input arguments to pass into the hook
  workflowId?: string;   //execution id (the job id to hook into)
  workflowName?: string; //the name of the user's hook function
  search?: WorkflowSearchOptions //bind additional search terms immediately before hook reentry
  config?: WorkflowConfig; //hook function constraints (backoffCoefficient, maximumAttempts, maximumInterval, initialInterval)
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

type MeshOSClassConfig = {
  namespace: string;
  taskQueue: string;
  redisOptions: RedisOptions;
  redisClass: RedisClass;
}

type MeshOSConfig = {
  id?: string; //guid for the workflow when instancing
  await?: boolean; //default is false; must explicitly send true to await the final result
  taskQueue?: string; //optional target queue isolate for the function
}

type ConnectionConfig = {
  class: RedisClass;
  options: RedisOptions;
}
type Connection =  ConnectionConfig;

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
  workflow: Function | Record<string | symbol, Function>; //target function to run
  options?: WorkerOptions;
  search?: WorkflowSearchOptions;
}

type FindWhereQuery = {
  field: string;
  is: string;
  value: string | boolean | number | [number, number];
  type?: string; //default is TEXT
}

type FindOptions = {
  workflowName?: string; //also the function name
  taskQueue?: string;
  namespace?: string;
  index?: string;        //the FT search index name
}

type FindWhereOptions = {
  options?: FindOptions;
  count?: boolean;
  query: FindWhereQuery[];
  return?: string[];
  limit?: {
    start: number,
    size: number
  }
}

type MeshOSOptions = {
  name: string;
  options: WorkerOptions;
}

type MeshOSActivityOptions = {
  name: string;
  options: ActivityConfig;
}

type MeshOSWorkerOptions = {
  taskQueue?: string; //change the default task queue
  allowList?: Array<MeshOSOptions | string>; //limit which `hook` and `workflow` workers start
  logLevel?: string; //debug, info, warn, error
  maxSystemRetries?: number; //1-3 (10ms, 100ms, 1_000ms)
  backoffCoefficient?: number; //2-10ish
}

type WorkerOptions = {
  logLevel?: string; //debug, info, warn, error
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
  ProxyType,
  Registry,
  SignalOptions,
  FindOptions,
  FindWhereOptions,
  FindWhereQuery,
  HookOptions,
  MeshOSActivityOptions,
  MeshOSWorkerOptions,
  MeshOSClassConfig,
  MeshOSConfig,
  MeshOSOptions,
  WorkerConfig,
  WorkflowConfig,
  WorkerOptions,
  WorkflowSearchOptions,
  WorkflowDataType,
  WorkflowOptions,
};
