import { WorkflowHandleService } from '../services/memflow/handle';

import { LogLevel } from './logger';
import { ProviderConfig, ProvidersConfig } from './provider';
import { StringAnyType, StringStringType } from './serializer';
import { StreamData, StreamError } from './stream';

/**
 * Type definition for workflow configuration.
 */
type WorkflowConfig = {
  /**
   * Backoff coefficient for retry mechanism.
   * @default 10 (HMSH_MEMFLOW_EXP_BACKOFF)
   */
  backoffCoefficient?: number;

  /**
   * Maximum number of attempts for retries.
   * @default 5 (HMSH_MEMFLOW_MAX_ATTEMPTS)
   */
  maximumAttempts?: number;

  /**
   * Maximum interval between retries.
   * @default 120s (HMSH_MEMFLOW_MAX_INTERVAL)
   */
  maximumInterval?: string;

  /**
   * Whether to throw an error on final failure after retries are exhausted
   * or return the error object as a standard response containing error-related
   * fields like `stack`, `code`, `message`.
   * @default true
   */
  throwOnError?: boolean;
};

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
  };

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
   * the HotMesh App namespace
   * @default memflow
   */
  namespace: string;

  /**
   * holds list of interruption payloads; if list is longer than 1 when the error is thrown, a `collator` subflow will be used
   */
  interruptionRegistry: any[];

  /**
   * entry point ancestor flow; might be the parent; will never be self
   */
  originJobId: string;

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

  /**
   * the HotMesh connection configuration
   */
  connection: Connection;

  /**
   * if present, the workflow will delay expiration for the specified number of seconds
   */
  expire?: number;
};

/**
 * The schema for the full-text-search
 * @deprecated
 */
export type WorkflowSearchSchema = Record<
  string,
  {
    /**
     * The FT.SEARCH field type. One of: TEXT, NUMERIC, TAG. TEXT is
     * most expensive, but also most expressive.
     */
    type: 'TEXT' | 'NUMERIC' | 'TAG';

    /**
     * FT.SEARCH SORTABLE field. If true, results may be sorted according to this field
     * @default false
     */
    sortable?: boolean;

    /**
     * FT.SEARCH NOSTEM field. applies to TEXT fields types.
     * If true, the text field index will not stem words
     * @default false
     */
    nostem?: boolean;

    /**
     * FT.SEARCH NOINDEX field. If true and if the field is sortable, the field will aid
     * in sorting results but not be directly indexed as a standalone
     * @default false
     */
    noindex?: boolean;

    /**
     * if true, the field is indexed and searchable within the FT.SEARCH index
     * This is different from `noindex` which is FT.SEARCH specific and relates
     * to sorting and indexing. This is a general flag for the field that will
     * enable or disable indexing and searching entirely. Use for fields with
     * absolutely no meaning to query or sorting but which are important
     * nonetheless as part of the data record that is saved and returned.
     * @default true
     */
    indexed?: boolean;

    /**
     * An array of possible values for the field
     */
    examples?: string[];

    /**
     * The 'nilable' setting may NOT be set to `true` for
     * NUMBER types as it causes an indexing error;
     * consider a custom (e.g., negative number) value to represent
     * `null` if desired for a NUMERIC field.
     * Set to true only if the field is a TEXT or TAG type and
     * you wish to save the string `null` as a value to search
     * on (the tag, {null}, or the string, (null)
     * @default false
     */
    nilable?: boolean;

    /**
     * possible scalar/primitive types for the field. Use when
     * serializing and restoring data to ensure the field is
     * properly typed. If not provided, the field will be
     * treated as a string.
     */
    primitive?: 'string' | 'number' | 'boolean' | 'array' | 'object';

    /**
     * if true, the field is required to be present in the data record
     * @default false
     */
    required?: boolean;

    /**
     * an enumerated list of allowed values; if field is nilable, it is implied
     * and therefore not necessary to include `null` in the list
     * @default []
     */
    enum?: string[];

    /**
     * a regular expression pattern for the field
     * @default '.*'
     * @example '^[a-zA-Z0-9_]*$'
     */
    pattern?: string;

    /**
     * literal value to use for the indexed field name (without including the standard underscore (_) prefix isolate)
     */
    fieldName?: string;
  }
>;

type WorkflowSearchOptions = {
  /** FT index name (myapp:myindex) */
  index?: string;

  /** FT prefixes (['myapp:myindex:prefix1', 'myapp:myindex:prefix2']) */
  prefix?: string[];

  /**
   * Schema mapping each field. Each field is a key-value pair where the key is the field name
   * and the value is a record of field options. If the fieldName is provided,
   * it will be used as the indexed field name. If not provided
   * key will be used as the indexed field name with an underscore prefix.
   *
   */
  schema?: WorkflowSearchSchema;

  /** Additional data as a key-value record */
  data?: StringStringType;
};

type SearchResults = {
  /**
   * the total number of results
   */
  count: number;
  /**
   * the raw FT.SEARCH query string
   */
  query: string;
  /**
   * the raw FT.SEARCH results as an array of objects
   */
  data: StringStringType[];
};

type WorkflowOptions = {
  /**
   * the namespace for the workflow
   * @default memflow
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
   * the full-text-search
   */
  search?: WorkflowSearchOptions;

  /**
   * marker data (begins with a -)
   */
  marker?: StringStringType;

  /**
   * the workflow configuration object
   */
  config?: WorkflowConfig;

  /**
   * sets the number of seconds a workflow may exist after completion. The default policy is to expire the job hash as soon as it completes.
   */
  expire?: number;

  /**
   * system flag to indicate that the flow should remain open beyond main method completion while still emitting the 'job done' event
   */
  persistent?: boolean;

  /**
   * default is true; set to false to optimize workflows that do not require a `signal in`
   */
  signalIn?: boolean;

  /**
   * default is true; if false, will not await the execution
   */
  await?: boolean;

  /**
   * If provided, the job will initialize in a pending state, reserving
   * only the job ID (HSETNX) and persisting search and marker (if provided).
   * If a `resume` signal is sent before the specified number of seconds,
   * the job will resume as normal, transitioning to the adjacent children
   * of the trigger. If the job is not resumed within the number
   * of seconds specified, the job will be scrubbed. No dependencies
   * are added for a job in a pending state; however, dependencies
   * will be added after the job is resumed if relevant.
   */
  pending?: number;

  /**
   * Provide to set the engine name. This MUST be unique, so do not
   * provide unless it is guaranteed to be a unique engine/worker guid
   * when identifying the point of presence within the mesh.
   */
  guid?: string;
};

/**
 * Options for setting up a hook.
 * 'memflow' is the default namespace if not provided;
 * similar to setting `appid` in the YAML
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
  search?: WorkflowSearchOptions;

  /** Hook function constraints (backoffCoefficient, maximumAttempts, maximumInterval) */
  config?: WorkflowConfig;
};

/**
 * Options for sending signals in a workflow.
 */
type SignalOptions = {
  /**
   * Task queue associated with the workflow
   */
  taskQueue: string;

  /**
   * Input data for the signal (any serializable object)
   */
  data: StringAnyType;

  /**
   * Execution ID, also known as the job ID
   */
  workflowId: string;

  /**
   * Optional name of the user's workflow function
   */
  workflowName?: string;
};

type ActivityWorkflowDataType = {
  activityName: string;
  arguments: any[];
  workflowId: string;
  workflowTopic: string;
};

type WorkflowDataType = {
  arguments: any[];
  workflowId: string;
  workflowTopic: string;
  workflowDimension?: string; //is present if hook (not main workflow)
  originJobId?: string; //is present if there is an originating ancestor job
  canRetry?: boolean;
  expire?: number;
};

type Connection = ProviderConfig | ProvidersConfig;

type ClientConfig = {
  connection: Connection;
};

type Registry = {
  [key: string]: Function;
};
type WorkerConfig = {
  /** Connection configuration for the worker */
  connection: Connection;

  /**
   * Namespace used in the app configuration, denoted as `appid` in the YAML
   * @default memflow
   */
  namespace?: string;

  /** Task queue name, denoted as `subscribes` in the YAML (e.g., 'hello-world') */
  taskQueue: string;

  /** Target function or a record type with a name (string) and reference function */
  workflow: Function | Record<string | symbol, Function>;

  /** Additional options for configuring the worker */
  options?: WorkerOptions;

  /** Search options for workflow execution details */
  search?: WorkflowSearchOptions;

  /**
   * Provide to set the engine name. This MUST be unique, so do not
   * provide unless it is guaranteed to be a unique engine/worker guid
   * when identifying the point of presence within the mesh.
   */
  guid?: string;
};

type FindWhereQuery = {
  field: string;
  is: '=' | '==' | '>=' | '<=' | '[]';
  value: string | boolean | number | [number, number];
  type?: string; //default is TEXT
};

type FindOptions = {
  workflowName?: string; //also the function name
  taskQueue?: string;
  namespace?: string;
  index?: string;
  search?: WorkflowSearchOptions;
};

type FindWhereOptions = {
  options?: FindOptions;
  count?: boolean;
  query: FindWhereQuery[];
  return?: string[];
  limit?: {
    start: number;
    size: number;
  };
};

type FindJobsOptions = {
  /** The workflow name; include an asterisk for wilcard search */
  match?: string;

  /**
   * application namespace
   * @default memflow
   */
  namespace?: string;

  /** The suggested response limit. Reduce batch size to reduce the likelihood of large overages. */
  limit?: number;

  /** How many records to scan at a time */
  batch?: number;

  /** The start cursor; defaults to 0 */
  cursor?: string;
};

type WorkerOptions = {
  /** Log level: debug, info, warn, error */
  logLevel?: LogLevel;

  /** Maximum number of attempts, default 5 (HMSH_MEMFLOW_MAX_ATTEMPTS) */
  maximumAttempts?: number;

  /** Backoff coefficient for retry logic, default 10 (HMSH_MEMFLOW_EXP_BACKOFF) */
  backoffCoefficient?: number;

  /** Maximum interval between retries, default 120s (HMSH_MEMFLOW_MAX_INTERVAL) */
  maximumInterval?: string;
};

type ContextType = {
  workflowId: string;
  workflowTopic: string;
};

type FunctionSignature<T> = T extends (...args: infer A) => infer R
  ? (...args: A) => R
  : never;
type ProxyType<ACT> = {
  [K in keyof ACT]: FunctionSignature<ACT[K]>;
};

/**
 * Configuration settings for activities within a workflow.
 */
type ActivityConfig = {
  /** place holder setting; unused at this time (re: activity workflow expire configuration) */
  expire?: number;

  /** Start to close timeout for the activity; not yet implemented */
  startToCloseTimeout?: string;

  /** Configuration for specific activities, type not yet specified */
  activities?: any;

  /** Retry policy configuration for activities */
  retryPolicy?: {
    /** Maximum number of retry attempts, default is 5 (HMSH_MEMFLOW_MAX_ATTEMPTS) */
    maximumAttempts?: number;
    /** Factor by which the retry timeout increases, default is 10 (HMSH_MEMFLOW_MAX_INTERVAL) */
    backoffCoefficient?: number;
    /** Maximum interval between retries, default is '120s' (HMSH_MEMFLOW_EXP_BACKOFF) */
    maximumInterval?: string;
    /** Whether to throw an error on failure, default is true */
    throwOnError?: boolean;
  };
};

/**
 * The proxy response object returned from the activity proxy flow
 */
type ProxyResponseType<T> = {
  data?: T; //expected data
  $error?: StreamError;
  done?: boolean; //non-existent if error was thrown in transition (not during execution)
  jc: string;
  ju: string;
};

/**
 * The child flow response object returned from the  main flow during recursion
 */
type ChildResponseType<T> = {
  data?: T; //expected data
  $error?: StreamError;
  done?: boolean; //non-existent if error was thrown in transition (not during execution)
  jc: string;
  ju: string;
};

interface ClientWorkflow {
  start(options: WorkflowOptions): Promise<WorkflowHandleService>;
  signal(
    signalId: string,
    data: StringAnyType,
    namespace?: string,
  ): Promise<string>;
  hook(options: HookOptions): Promise<string>;
  getHandle(
    taskQueue: string,
    workflowName: string,
    workflowId: string,
    namespace?: string,
  ): Promise<WorkflowHandleService>;
  search(
    taskQueue: string,
    workflowName: string,
    namespace: string | null,
    index: string,
    ...query: string[]
  ): Promise<string[]>;
}

/**
 * Workflow interceptor that can wrap workflow execution in an onion-like pattern.
 * Each interceptor wraps the next one, with the actual workflow execution at the center.
 *
 * Interceptors are executed in the order they are registered. Each interceptor can:
 * - Perform actions before workflow execution
 * - Modify or enhance the workflow context
 * - Handle or transform workflow results
 * - Catch and handle errors
 * - Add cross-cutting concerns like logging, metrics, or tracing
 *
 * @example
 * ```typescript
 * // Simple logging interceptor
 * const loggingInterceptor: WorkflowInterceptor = {
 *   async execute(ctx, next) {
 *     console.log('Before workflow');
 *     try {
 *       const result = await next();
 *       console.log('After workflow');
 *       return result;
 *     } catch (err) {
 *       console.error('Workflow error:', err);
 *       throw err;
 *     }
 *   }
 * };
 *
 * // Register the interceptor
 * MemFlow.registerInterceptor(loggingInterceptor);
 * ```
 */
export interface WorkflowInterceptor {
  /**
   * Called before workflow execution to wrap the workflow in custom logic
   *
   * @param ctx - The workflow context map containing workflow metadata and state
   * @param next - Function to call the next interceptor or the workflow itself
   * @returns The result of the workflow execution
   *
   * @example
   * ```typescript
   * // Metrics interceptor implementation
   * {
   *   async execute(ctx, next) {
   *     const workflowName = ctx.get('workflowName');
   *     const metrics = getMetricsClient();
   *
   *     metrics.increment(`workflow.start.${workflowName}`);
   *     const timer = metrics.startTimer();
   *
   *     try {
   *       const result = await next();
   *       metrics.increment(`workflow.success.${workflowName}`);
   *       return result;
   *     } catch (err) {
   *       metrics.increment(`workflow.error.${workflowName}`);
   *       throw err;
   *     } finally {
   *       timer.end();
   *     }
   *   }
   * }
   * ```
   */
  execute(ctx: Map<string, any>, next: () => Promise<any>): Promise<any>;
}

/**
 * Registry for workflow interceptors that are executed in order
 * for each workflow execution
 */
export interface InterceptorRegistry {
  /**
   * Array of registered interceptors that will wrap workflow execution
   * in the order they were registered (first registered = outermost wrapper)
   */
  interceptors: WorkflowInterceptor[];
}

export {
  ActivityConfig,
  ActivityWorkflowDataType,
  ChildResponseType,
  ClientConfig,
  ClientWorkflow,
  ContextType,
  Connection,
  ProxyResponseType,
  ProxyType,
  Registry,
  SignalOptions,
  FindJobsOptions,
  FindOptions,
  FindWhereOptions,
  FindWhereQuery,
  HookOptions,
  SearchResults,
  WorkerConfig,
  WorkflowConfig,
  WorkerOptions,
  WorkflowSearchOptions,
  WorkflowDataType,
  WorkflowOptions,
  WorkflowContext,
};
