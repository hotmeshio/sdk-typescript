import { ProviderTransaction } from './provider';

/**
 * Structured retry policy configuration for stream messages.
 * Controls retry behavior with exponential backoff.
 * 
 * @example
 * ```typescript
 * const retryPolicy: RetryPolicy = {
 *   maximumAttempts: 5,
 *   backoffCoefficient: 2,
 *   maximumInterval: '300s',
 * };
 * // Results in delays: 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 300s (capped)
 * ```
 */
export interface RetryPolicy {
  /**
   * Maximum number of retry attempts before the message fails.
   * @default 3
   * @example 5
   */
  maximumAttempts?: number;

  /**
   * Base coefficient for exponential backoff calculation.
   * Retry delay = min(backoffCoefficient ^ attemptNumber, maximumInterval)
   * @default 10
   * @example 2
   */
  backoffCoefficient?: number;

  /**
   * Maximum interval between retries in seconds or as a duration string.
   * Caps the exponential backoff to prevent excessive delays.
   * @default "120s"
   * @example "60s" or 60
   */
  maximumInterval?: string | number;
}

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
};

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
  /**
   * Internal retry configuration propagated from PostgreSQL columns.
   * Used to maintain retry policy across message recreation cycles.
   * @internal
   */
  _streamRetryConfig?: {
    max_retry_attempts: number;
    backoff_coefficient: number;
    maximum_interval_seconds: number;
  };
  /**
   * Visibility delay in milliseconds for retry with visibility timeout.
   * When set, the message will be invisible until the delay expires.
   * @internal
   */
  _visibilityDelayMs?: number;
  /**
   * Current retry attempt count from database.
   * Tracks how many times this message has been retried.
   * @internal
   */
  _retryAttempt?: number;
}

/** Extends StreamData for responses, allowing for inheritance of the base properties */
export type StreamDataResponse = StreamData;

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
    value: string,
  ],
][];

/** Configuration parameters for a stream */
export type RouterConfig = {
  /** Namespace under which the stream operates */
  namespace: string;
  /** Application identifier */
  appId: string;
  /** Globally unique identifier for the stream */
  guid: string;
  /** Role associated with the stream */
  role: StreamRole;
  /** Default throttle (read from KeyType.THROTTLE_RATE) */
  throttle: number;
  /** Optional topic for the stream */
  topic?: string;
  /** Delay before a message can be reclaimed, defaults to 60,000 milliseconds */
  reclaimDelay?: number;
  /** Maximum number of reclaims allowed, defaults to 3. Values greater throw an error */
  reclaimCount?: number;
  /** if true, will not process stream messages; default true */
  readonly?: boolean;
  /** Retry policy for worker messages. Applied when worker callback throws an error */
  retryPolicy?: RetryPolicy;
};

export type StreamProviderType =
  | 'postgres'
  | 'nats'
  | 'sqs';

export interface StreamConfig {
  // Common configuration
  provider?: StreamProviderType;
  namespace?: string;
  appId?: string;
  maxRetries?: number;
  batchSize?: number;
  timeout?: number;

  /**
   * Default retry policy for all streams in this app.
   * Can be overridden per message using PublishMessageConfig.
   * 
   * @example
   * ```typescript
   * {
   *   maximumAttempts: 5,
   *   backoffCoefficient: 2,
   *   maximumInterval: '300s',
   * }
   * ```
   */
  retryPolicy?: RetryPolicy;

  // Provider-specific configurations
  postgres?: {
    pollInterval?: number;
    vacuumInterval?: number;
    partitionInterval?: 'daily' | 'weekly' | 'monthly';
    cleanupInterval?: number;
    // Notification-related configuration
    enableNotifications?: boolean; // Default: true
    notificationFallbackInterval?: number; // Default: 30000ms (30 seconds)
    notificationTimeout?: number; // Default: 5000ms (5 seconds)
  };
  nats?: {
    jetstream?: boolean;
    durableName?: string;
    deliverPolicy?: 'all' | 'last' | 'new' | 'byStartSequence' | 'byStartTime';
    ackWait?: number;
  };
  sqs?: {
    deadLetterQueue?: string;
    visibilityTimeout?: number;
    waitTimeSeconds?: number;
    messageRetentionPeriod?: number;
    dlqArn?: string;
  };
}

export interface StreamMessage {
  id: string;
  data: StreamData;
  metadata?: StreamMessageMetadata;
  /**
   * Retry policy configuration for this message.
   * Populated from database columns when available.
   */
  retryPolicy?: RetryPolicy;
}

export interface StreamMessageMetadata {
  timestamp?: number;
  stream?: string;
  groupName?: string;
  consumerName?: string;
  retryCount?: number;
  deliveryTime?: number;
  originalMessageId?: string;
  [key: string]: any; // For provider-specific metadata
}

export interface StreamStats {
  messageCount: number;
  consumerCount?: number;
  bytesInMemory?: number;
  oldestMessageTimestamp?: number;
  pendingMessages?: number;
  deadLetterQueueMessageCount?: number;
  averageProcessingTime?: number;
  lastErrorTimestamp?: number;
  lastErrorMessage?: string;
  [key: string]: any; // For provider-specific stats
}

/**
 * When publishing a message to the stream, the configuration
 * can include a transaction object to execute the operation
 * atomically and/or a retry policy to control retry behavior.
 */
export interface PublishMessageConfig {
  transaction?: ProviderTransaction;
  
  /**
   * Retry policy for this specific message.
   * Overrides stream-level and app-level defaults.
   * 
   * @example
   * ```typescript
   * await streamService.publishMessages('my-topic', [msg], {
   *   retryPolicy: {
   *     maximumAttempts: 10,
   *     backoffCoefficient: 2,
   *     maximumInterval: '600s',
   *   }
   * });
   * ```
   */
  retryPolicy?: RetryPolicy;
}

/**
 * Notification consumer configuration for PostgreSQL stream provider.
 * Manages notification-based message consumption with fallback polling.
 */
export interface NotificationConsumer {
  /** Name of the stream being consumed */
  streamName: string;
  /** Name of the consumer group */
  groupName: string;
  /** Unique name identifying this consumer */
  consumerName: string;
  /** Callback function invoked when messages are available */
  callback: (messages: StreamMessage[]) => void;
  /** Whether this consumer is actively listening for notifications */
  isListening: boolean;
  /** Timestamp of the last fallback check (used for periodic polling) */
  lastFallbackCheck: number;
}
