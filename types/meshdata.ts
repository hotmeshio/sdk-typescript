import { HookOptions, WorkflowConfig, WorkflowSearchOptions } from "./meshflow";
import { StringStringType } from "./serializer";

export type CallOptions = {
  /**
   * if provided along with a `ttl`, the function will be cached
   */
  id?: string;
  /**
   * in format '1 minute', '5 minutes', '1 hour', 'infinity', etc
   */
  ttl?: string;
  /**
   * full GUID (including prefix)
   */
  $guid?: string;
  /**
   * exec, hook, proxy
   */
  $type?: string;
  /**
   * if set to false explicitly it will not await the result
   */
  await?: boolean;
  /**
   * taskQueue for the workflowId (defaults to entity)
   */
  taskQueue?: string;
  /**
   * defaults to `entity` input parameter; allows override of the workflowId prefix
   */
  prefix?: string;
  search?: WorkflowSearchOptions;
  /**
   * list of  state field names to return (this is NOT the final response)
   */
  fields?: string[];
  /**
   * namespace for the the execution client; how it appears in Redis (defaults to 'durable')
   */
  namespace?: string; //optional namespace for the workflowId (defaults to 'durable')

  /**
   * Custom marker data field used for adding a searchable marker to the job.
   * markers always begin with a dash (-). Any field that does not
   * begin with a dash will be removed and will not be inserted with
   * the initial data set.
   */
  marker?: StringStringType;

  /**
   * If provided, the job will initialize in an pending state, reserving
   * only the job ID (HSETNX) and persisting search and marker (if provided).
   * If a `resume` signal is sent before the specified number of seconds,
   * the job will resume as normal. If the job is not resumed within the
   * number of seconds provided, the job will be scrubbed. No dependencies
   * are set for a job in a pending state; however, dependencies will be
   * added after the job is resumed if necessary.
   */
  pending?: number;

  //flush?: boolean;
};

export type ConnectOptions = {
  /**
   * if set to infinity, callers may not override (the function will be durable)
   */
  ttl?: string;
  /**
   * the task queue for the connected function for greater specificity
   */
  taskQueue?: string;
  /**
   * prefix for the workflowId (defaults to entity value if not provided)
   */
  prefix?: string; 
  /**
   * optional namespace for the the worker; how it appears in Redis (defaults to 'durable')
   */
  namespace?: string; //optional namespace for the workflowId (defaults to 'durable')
  /**
   * extended worker options
   */
  options?: WorkerOptions;
  /**
   * optional search configuration
   */
  search?: WorkflowSearchOptions;
};

/**
 * Connect a function to the operational data layer.
 * @template T - the return type of the connected function
 */
export type ConnectionInput<T> = {
  /**
   * The connected function's entity identifier
   * 
   * @example
   * user
   */
  entity: string;
  /**
   * The target function reference
   * 
   * @example
   * function() { return "hello world" }
   */
  target: (...args: any[]) => T;
  /**
   * Extended connection options (e.g., ttl, taskQueue)
   * @example
   * { ttl: 'infinity' }
   */
  options?: ConnectOptions;
};

/**
 * Executes a remote function by its global entity identifier with specified arguments.
 * If options.ttl is infinity, the function will be cached indefinitely and can only be
 * removed by calling `flush`. During this time, the function will remain active and can
 * its state can be augmented by calling `set`, `incr`, `del`, etc OR by calling a
 * transactional 'hook' function.
 * 
 * @template T The expected return type of the remote function.
 */
export type ExecInput = {
  /**
   * the connected function's entity identifier
   * @example
   * user
   */
  entity: string;
  /**
   * the function's input arguments
   * @example
   * ['Jane', 'Doe']
   */
  args: any[];
  /**
   * Extended options for the hook function, like specifying a taskQueue or ttl
   * @example
   * { ttl: '5 minutes' }
   */
  options?: Partial<MeshDataWorkflowOptions>;
};

/**
 * Hook function inputs. Hooks augment running jobs.
 */
export type HookInput = {
  /**
   * The target function's entity identifier
   * @example 'user'
   */
  entity: string;
  /**
   * The target execution id (workflowId/jobId)
   * @example 'jsmith123'
   */
  id: string;
  /**
   * The hook function's entity identifier
   * @example 'user.notify'
   */
  hookEntity: string;
  /**
   * The hook function's input arguments
   * @example 'notify'
   */
  hookArgs: any[];
  /**
   * Extended options for the hook function, like specifying a taskQueue
   * @example { taskQueue: 'priority' }
   */
  options?: Partial<HookOptions>;
};


export type MeshDataWorkflowOptions = {
  /**
   * The app deployment namespace; how it appears in redis (e.g., 'durable')
   */
  namespace?: string;

  /**
   * Target connected functions more specifically by taskQueue
   */
  taskQueue?: string;

  /**
   * The connected function's entity identifier
   */
  prefix?: string;

  /**
   * The function execution id (shorthand for workflowId)
   */
  id?: string;

  /**
   * The function execution id
   */
  workflowId?: string;

  /**
   * The function name (`entity` is a shorthand for this)
   */
  workflowName?: string;

  /**
   * The open telemetry trace context for the workflow, used for logging and tracing. If a sink is enabled, this will be sent to the sink.
   */
  workflowTrace?: string;

  /**
   * The open telemetry span context for the workflow, used for logging and tracing. If a sink is enabled, this will be sent to the sink.
   */
  workflowSpan?: string;

  /**
   * Search fields to seed function state when it first initializes
   */
  search?: WorkflowSearchOptions;

  /**
   * Extended execution options
   */
  config?: WorkflowConfig;

  /**
   * Set to 'infinity' to make the function durable; otherwise, '1 minute', '1 hour', etc
   */
  ttl?: string;

  /**
   * If set to false explicitly it will not await the result
   * @default true
   */
  await?: boolean;

  /**
   * Custom marker data field used for adding a searchable marker to the job.
   * markers always begin with a dash (-). Any field that does not
   * begin with a dash will be removed and will not be inserted with
   * the initial data set.
   */
  marker?: StringStringType;

  /**
   * If provided, the job will initialize in a pending state, reserving
   * only the job ID (HSETNX) and persisting search and marker (if provided).
   * If a `resume` signal is sent before the specified number of seconds,
   * the job will resume as normal. If the job is not resumed within the
   * number of seconds provided, the job will be scrubbed. No dependencies
   * are set for a job in a pending state; however, dependencies will be
   * added after the job is resumed (if necessary).
   */
  pending?: number;

  /**
   * sets the number of seconds a workflow may exist after completion. As the process engine is an in-memory cache, the default policy is to expire and scrub the job hash as soon as it completes.
   * @default 1
   */
  expire?: number;

  /**
   * set to false to optimize workflows that do not require a `signal in`
   * @default true
   */
  signalIn?: boolean;
};
