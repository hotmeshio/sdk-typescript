import { LogLevel } from './logger';
import { RedisClass, RedisOptions } from './redis';
import { StringStringType } from './serializer';
import { StreamData } from './stream';

//todo: not sure if this is the right place for these types
//      makes better sense to apply to activities
type WorkflowConfig = {
  backoffCoefficient?: number; //default 10
  maximumAttempts?: number;    //default 2
  maximumInterval?: string;    //default 30s
}

type WorkflowContext = {

  /**
   * the reentrant semaphore, incremented in real-time as idempotent statements are re-traversed upon reentry. Indicates the current semaphore count.
   */
  counter: number;

  /**
   * number as string for the replay cursor
   */
  cursor: string;

  /**
   * the replay hash of name/value pairs representing prior executions
   */
  replay: StringStringType;

  /**
   * the HotMesh App namespace. `durable` is the default.
   */
  namespace: string;

  /**
   * the workflow/job ID
   */
  workflowId: string;

  /** 
   * the dimensional isolation for the reentrant hook, expressed in the format `0,0`, `0,1`, etc
   */
  workflowDimension: string;

  /**
   * a concatenation of the task queue and workflow name (e.g., `${taskQueueName}-${workflowName}`)
   */
  workflowTopic: string;

  /**
   * the open telemetry trace context for the workflow, used for logging and tracing. If a sink is enabled, this will be sent to the sink.
   */
  workflowTrace: string;

  /**
   * the open telemetry span context for the workflow, used for logging and tracing. If a sink is enabled, this will be sent to the sink.
   */
  workflowSpan: string;

  /**
   * the native HotMesh message that encapsulates the arguments, metadata, and raw data for the workflow
   */
  raw: StreamData;
}

type WorkflowSearchOptions = {
  index?: string;         //FT index name (myapp:myindex)
  prefix?: string[];      //FT prefixes (['myapp:myindex:prefix1', 'myapp:myindex:prefix2'])
  schema?: Record<string, {type: 'TEXT' | 'NUMERIC' | 'TAG', sortable?: boolean}>;
  data?: Record<string, string>;
}

type WorkflowOptions = {
  /**
   * the namespace for the workflow; `durable` is the default namespace if not provided
   */
  namespace?: string;
  /**
   * the task queue for the workflow; optional if entity is provided
   */
  taskQueue?: string;
  /**
   * input arguments to pass in
   */
  args: any[];
  /**
   * the job id
   */
  workflowId?: string;
  /**
   * if invoking a workflow, passing 'entity' will apply the value as the workflowName, taskQueue, and prefix, ensuring the FT.SEARCH index is properly scoped. This is a convenience method but limits options.
   */
  entity?: string;
  /**
   * the name of the user's workflow function; optional if 'entity' is provided
   */
  workflowName?: string;
  /**
   * the parent workflow id; adjacent ancestor ID
   */
  parentWorkflowId?: string;
  /**
   * the entry point workflow id
   */
  originJobId?: string;
  /**
   * OpenTelemetry trace context for the workflow
   */
  workflowTrace?: string;
  /**
   * OpenTelemetry span context for the workflow
   */
  workflowSpan?: string;
  /**
   * the search options for the workflow
   */
  search?: WorkflowSearchOptions
  /**
   * the workflow configuration
   */
  config?: WorkflowConfig;
  /**
   * expire in seconds
   */
  expire?: number;
  /**
   * default is true; if false, will not await the execution
   */
  await?: boolean;
}

type HookOptions = {
  namespace?: string;   //'durable' is the default namespace if not provided; similar to setting `appid` in the YAML
  taskQueue?: string;   //optional if 'entity' is provided
  args: any[];          //input arguments to pass into the hook
  entity?: string;      //If invoking a hook, passing 'entity' will apply the value as the workflowName, taskQueue, and prefix, ensuring the FT.SEARCH index is properly scoped. This is a convenience method but limits options.
  workflowId?: string;   //execution id (the job id to hook into)
  workflowName?: string; //the name of the user's hook function
  search?: WorkflowSearchOptions //bind additional search terms immediately before hook reentry
  config?: WorkflowConfig; //hook function constraints (backoffCoefficient, maximumAttempts, maximumInterval)
}

type SignalOptions = {
  taskQueue: string;
  data: Record<string, any>; //input data (any serializable object)
  workflowId: string;        //execution id (the job id)
  workflowName?: string;     //the name of the user's workflow function
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
  workflowDimension?: string; //is present if hook (not main workflow)
  originJobId?: string;       //is present if there is an originating ancestor job (should rename to originJobId)
}

type MeshOSClassConfig = {
  namespace: string;
  taskQueue: string;
  redisOptions: RedisOptions;
  redisClass: RedisClass;
}

type MeshOSConfig = {
  id?: string;        //guid for the workflow when instancing
  await?: boolean;    //default is false; must explicitly send true to await the final result
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
  is: '=' | '==' | '>=' | '<=' | '[]';
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
  taskQueue?: string;          //change the default task queue
  allowList?: Array<MeshOSOptions | string>; //limit which `hook` and `workflow` workers start
  logLevel?: LogLevel;         //debug, info, warn, error
  maximumAttempts?: number;   //1-3 (10ms, 100ms, 1_000ms)
  backoffCoefficient?: number; //2-10ish
}

type WorkerOptions = {
  logLevel?: LogLevel;         //debug, info, warn, error
  maximumAttempts?: number;   //1-3 (10ms, 100ms, 1_000ms)
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
  WorkflowContext,
};
