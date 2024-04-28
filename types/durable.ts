import { LogLevel } from './logger';
import { RedisClass, RedisOptions } from './redis';
import { StringStringType } from './serializer';
import { StreamData, StreamError } from './stream';

/**
 * Type definition for workflow configuration.
 */
type WorkflowConfig = {
  /** 
   * Backoff coefficient for retry mechanism.
   * @default 10 (HMSH_DURABLE_EXP_BACKOFF)
   */
  backoffCoefficient?: number;

  /** 
   * Maximum number of attempts for retries.
   * @default 5 (HMSH_DURABLE_MAX_ATTEMPTS)
   */
  maximumAttempts?: number;

  /** 
   * Maximum interval between retries.
   * @default 120s (HMSH_DURABLE_MAX_INTERVAL)
   */
  maximumInterval?: string;

  /**
   * Whether to throw an error on final failure after retries are exhausted.
   * @default true
   */
  throwOnError?: boolean;
}

type WorkflowContext = {

  /**
   * can the workflow be retried if an error occurs
   */
  canRetry: boolean;

  COUNTER: {
    /**
     * the reentrant semaphore parent counter object for object reference during increment
     */
    counter: number;
  }

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
   * the full-text-search (RediSearch) options for the workflow
   */
  search?: WorkflowSearchOptions

  /**
   * the workflow configuration object
   */
  config?: WorkflowConfig;

  /**
   * sets the number of seconds a workflow may exist after completion. As the process engine is an in-memory cache, the default policy is to expire and scrub the job hash as soon as it completes.
   */
  expire?: number;

  /**
   * default is true; if false, will not await the execution
   */
  await?: boolean;
}

/** 
 * Options for setting up a hook.
 * 'durable' is the default namespace if not provided; similar to setting `appid` in the YAML
 */
type HookOptions = {
  /** Optional namespace under which the hook function will be grouped */
  namespace?: string;

  /** Optional task queue, needed unless 'entity' is provided */
  taskQueue?: string;

  /** Input arguments to pass into the hook */
  args: any[];
     
  /**
   * Optional entity name. If provided, applies as the workflowName,
   * taskQueue, and prefix. This scopes the FT.SEARCH index appropriately.
   * This is a convenience method but limits options.
   */
  entity?: string;
  
  /** Execution ID, also known as the job ID to hook into */
  workflowId?: string;
  
  /** The name of the user's hook function */
  workflowName?: string;
  
  /** Bind additional search terms immediately before hook reentry */
  search?: WorkflowSearchOptions
  
  /** Hook function constraints (backoffCoefficient, maximumAttempts, maximumInterval) */
  config?: WorkflowConfig; 
}

/** 
 * Options for sending signals in a workflow.
 */
type SignalOptions = {
  /** Task queue associated with the workflow */
  taskQueue: string;
  /** Input data for the signal (any serializable object) */
  data: Record<string, any>; 
  /** Execution ID, also known as the job ID */
  workflowId: string;        
  /** Optional name of the user's workflow function */
  workflowName?: string;     
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
  canRetry?: boolean;
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

type WorkerOptions = {
  logLevel?: LogLevel;         //debug, info, warn, error
  maximumAttempts?: number;    //default 5 (HMSH_DURABLE_MAX_ATTEMPTS)
  backoffCoefficient?: number; //default 10 (HMSH_DURABLE_EXP_BACKOFF)
  maximumInterval?: string;    //default 120s (HMSH_DURABLE_MAX_INTERVAL)
}

type ContextType = {
  workflowId: string
  workflowTopic: string
};

type FunctionSignature<T> = T extends (...args: infer A) => infer R ? (...args: A) => R : never;
type ProxyType<ACT> = {
  [K in keyof ACT]: FunctionSignature<ACT[K]>;
};

/**
 * Configuration settings for activities within a workflow.
 */
type ActivityConfig = {
  /** Start to close timeout for the activity; not yet implemented */
  startToCloseTimeout?: string;

  /** Configuration for specific activities, type not yet specified */
  activities?: any;

  /** Retry policy configuration for activities */
  retryPolicy?: {
    /** Maximum number of retry attempts, default is 5 (HMSH_DURABLE_MAX_ATTEMPTS) */
    maximumAttempts?: number;
    /** Factor by which the retry timeout increases, default is 10 (HMSH_DURABLE_MAX_INTERVAL) */
    backoffCoefficient?: number;
    /** Maximum interval between retries, default is '120s' (HMSH_DURABLE_EXP_BACKOFF) */
    maximumInterval?: string;
    /** Whether to throw an error on failure, default is true */
    throwOnError?: boolean;
  };
};

/**
 * The proxy response object returned from the activity proxy flow
 */
type ProxyResponseType<T> = {
  data?: T,             //expected data
  $error?: StreamError,
  done?: boolean,       //non-existent if error was thrown in transition (not during execution)
  jc: string,
  ju: string
};

/**
 * The child flow response object returned from the  main flow during recursion
 */
type ChildResponseType<T> = {
  data?: T,             //expected data
  $error?: StreamError,
  done?: boolean,       //non-existent if error was thrown in transition (not during execution)
  jc: string,
  ju: string
};

export {
  ActivityConfig,
  ActivityWorkflowDataType,
  ChildResponseType,
  ClientConfig,
  ContextType,
  ConnectionConfig,
  Connection,
  ProxyResponseType,  
  ProxyType,
  Registry,
  SignalOptions,
  FindOptions,
  FindWhereOptions,
  FindWhereQuery,
  HookOptions,
  WorkerConfig,
  WorkflowConfig,
  WorkerOptions,
  WorkflowSearchOptions,
  WorkflowDataType,
  WorkflowOptions,
  WorkflowContext,
};
