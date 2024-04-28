/** Represents a policy for retrying stream operations based on error codes */
export interface StreamRetryPolicy {
  /** 
   * Key is error code, value is the retry profile.
   * Tuple contains: [max retry count, retry type].
   * 'x' denotes exponential backoff (default). Only 10, 100, 1000, 10000 are allowed retry intervals.
   */
  [key: string]: [number, 'x'?];
}

/** A 3-digit status code representing the outcome of a stream operation */
export type StreamCode = number;

/** Describes the structure of a stream error */
export type StreamError = {
  /** Descriptive message of the error */
  message: string;
  /** Numeric code corresponding to the type of error */
  code: number;
  /** Optional job identifier, used when communicating errors externally */
  job_id?: string;
  /** Stack trace of the error if unhandled */
  stack?: string;
  /** Name of the error if unhandled */
  name?: string;
  /** Custom user-defined error details */
  error?: Record<string, unknown>;
  /** True if originating via a standard transition message with an `error` status */
  is_stream_error?: boolean;
}

/** Enumerated status values for stream operations */
export enum StreamStatus {
  /** Indicates successful completion of the stream operation */
  SUCCESS = 'success',
  /** Indicates an error occurred during the stream operation */
  ERROR = 'error',
  /** Indicates the stream operation is still pending */
  PENDING = 'pending',
}

export enum StreamDataType {
  TIMEHOOK = 'timehook',
  WEBHOOK = 'webhook',
  AWAIT = 'await',
  RESULT = 'result', //await result
  WORKER = 'worker',
  RESPONSE = 'response', //worker response
  TRANSITION = 'transition',
  SIGNAL = 'signal',
  INTERRUPT = 'interrupt',
}

/** Defines the structure of stream data used when passing stream messages (transitions) */
export interface StreamData {
  /** Metadata associated with the stream data */
  metadata: {
    /** Globally unique identifier for the StreamData message to distinguish `retries` from new 'reentry/cycles' */
    guid: string;
    /** Workflow/job topic */
    topic?: string;
    /** Workflow/job ID */
    jid?: string;
    /** Workflow Generational ID (internal GUID) */
    gid?: string;
    /** Dimensional address indicating the message routing specifics */
    dad?: string;
    /** Activity ID */
    aid: string;
    /** OpenTelemetry Trace identifier */
    trc?: string;
    /** OpenTelemetry Span identifier */
    spn?: string;
    /** Current try count, used for retry logic */
    try?: number;
    /** 
     * Indicates if the message should wait for a response.
     * If explicitly false, the connection is severed immediately
     * upon verifying (and returning) the Job ID.
     */
    await?: boolean;
  };
  /** Type of the data being streamed, optional */
  type?: StreamDataType;
  /** Actual data being transmitted as a record of key-value pairs */
  data: Record<string, unknown>;
  /** Policies related to retry logic, optional */
  policies?: {
    retry?: StreamRetryPolicy;
  };
  /** Status of the stream, default assumed as 'success' */
  status?: StreamStatus;
  /** HTTP-like status code for the stream, default assumed as 200 */
  code?: number;
  /** Error stack trace */
  stack?: string;
}

/** Extends StreamData for responses, allowing for inheritance of the base properties */
export interface StreamDataResponse extends StreamData {}

export enum StreamRole {
  WORKER = 'worker',
  ENGINE = 'engine',
  SYSTEM = 'system', //reserved for system use (i.e, if worker or engine fails)
}
/** 
 * Represents a type for messages that have been reclaimed from a stream. 
 * Each item is a tuple containing a messageId and its details.
 */
export type ReclaimedMessageType = [
  /** The stream ID, typically formatted as `<timestamp>-<count>` */
  messageId: string,
  /** Details of the message, consisting of a key and its value */
  details: [
    /** Key is always 'message' */
    key: string,
    /** Value is a stringified representation of StreamData */
    value: string
  ]
][];

/** Configuration parameters for a stream */
export type StreamConfig = {
  /** Namespace under which the stream operates */
  namespace: string;
  /** Application identifier */
  appId: string;
  /** Globally unique identifier for the stream */
  guid: string;
  /** Role associated with the stream */
  role: StreamRole;
  /** Optional topic for the stream */
  topic?: string;
  /** Delay before a message can be reclaimed, defaults to 60,000 milliseconds */
  reclaimDelay?: number;
  /** Maximum number of reclaims allowed, defaults to 3. Values greater throw an error */
  reclaimCount?: number;
}
