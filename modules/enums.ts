import { LogLevel } from '../types/logger';

/**
 * Determines the log level for the application. The default is 'info'.
 */
export const HMSH_LOGLEVEL = (process.env.HMSH_LOGLEVEL as LogLevel) || 'info';
/**
 * Determines the log level for telemetry. The default is 'info' which emits worker and trigger spans. 'debug' emits all spans.
 */
export const HMSH_TELEMETRY =
  (process.env.HMSH_TELEMETRY as 'info' | 'debug') || 'info';
/**
 * If Redis, explicitly sets whether the application is running in a cluster. The default is false.
 * @deprecated
 */
export const HMSH_IS_CLUSTER = process.env.HMSH_IS_CLUSTER === 'true';
/**
 * Default cleanup time for signal in the db when its associated job is completed.
 */
export const HMSH_SIGNAL_EXPIRE = 3_600; //seconds

// HOTMESH STATUS CODES
export const HMSH_CODE_SUCCESS = 200;
export const HMSH_CODE_PENDING = 202;
export const HMSH_CODE_NOTFOUND = 404;
export const HMSH_CODE_INTERRUPT = 410;
export const HMSH_CODE_UNKNOWN = 500;
export const HMSH_CODE_TIMEOUT = 504;
export const HMSH_CODE_UNACKED = 999;
// MEMFLOW STATUS CODES
/**
 * This is thrown when a MemFlow has been interrupted by a sleepFor call.
 */
export const HMSH_CODE_MEMFLOW_SLEEP = 588;
/**
 * This is thrown when a MemFlow has been interrupted by a Promise.all call.
 */
export const HMSH_CODE_MEMFLOW_ALL = 589;
/**
 * This is thrown when a MemFlow has been interrupted by an execChild or startChild call.
 */
export const HMSH_CODE_MEMFLOW_CHILD = 590;
/**
 * This is thrown when a MemFlow has been interrupted by a proxyActivity call.
 */
export const HMSH_CODE_MEMFLOW_PROXY = 591;
/**
 * This is thrown when a MemFlow has been interrupted by a waitForSignal call.
 */
export const HMSH_CODE_MEMFLOW_WAIT = 595;
/**
 * The timeout status code for MemFlow. This status code is thrown when MemFlow has encountered a timeout error and needs to aler the caller why the call failed.
 */
export const HMSH_CODE_MEMFLOW_TIMEOUT = 596;
/**
 * The maxed status code for MemFlow. This status code is used to indicate that the MemFlow has reached the maximum
 * number of attempts and should be halted. Thrown from a proxied activity or a flow to halt standard execution
 * and prevent further attempts.
 */
export const HMSH_CODE_MEMFLOW_MAXED = 597;
/**
 * The fatal status code for MemFlow. This status code is used to indicate that the MemFlow has encountered a fatal error. Throw from a proxied activity or a flow to halt standard execution.
 */
export const HMSH_CODE_MEMFLOW_FATAL = 598;
/**
 * The retryable status code for MemFlow. This status code is used to indicate that the MemFlow has encountered a retryable error (essentially unknown and covered by the standard retry policy).
 */
export const HMSH_CODE_MEMFLOW_RETRYABLE = 599;

// HOTMESH MESSAGES
export const HMSH_STATUS_UNKNOWN = 'unknown';

// QUORUM
/**
 * The number of cycles to re/try for a quorum to be established.
 */
export const HMSH_QUORUM_ROLLCALL_CYCLES = 12;
/**
 * The delay in milliseconds between quorum rollcall cycles.
 */
export const HMSH_QUORUM_DELAY_MS = 250;
/**
 * The number of times the call-response exchange must succeed in succession to establish a quorum.
 */
export const HMSH_ACTIVATION_MAX_RETRY = 3;
//backend provisioning
export const HMSH_DEPLOYMENT_DELAY =
  parseInt(process.env.HMSH_DEPLOYMENT_DELAY, 10) || 10_000; //in ms
export const HMSH_DEPLOYMENT_PAUSE =
  parseInt(process.env.HMSH_DEPLOYMENT_PAUSE, 10) || 250; //in ms

// ENGINE
export const HMSH_OTT_WAIT_TIME =
  parseInt(process.env.HMSH_OTT_WAIT_TIME, 10) || 1000;
export const HMSH_EXPIRE_JOB_SECONDS =
  parseInt(process.env.HMSH_EXPIRE_JOB_SECONDS, 10) || 1;

// STREAM ROUTER
export const MAX_STREAM_BACKOFF = 
  parseInt(process.env.MAX_STREAM_BACKOFF, 10) || 500;
export const INITIAL_STREAM_BACKOFF =
  parseInt(process.env.INITIAL_STREAM_BACKOFF, 10) || 250;
export const MAX_STREAM_RETRIES =
  parseInt(process.env.MAX_STREAM_RETRIES, 10) || 2;
export const MAX_DELAY = 2147483647; // Maximum allowed delay in milliseconds for setTimeout
export const HMSH_MAX_RETRIES = parseInt(process.env.HMSH_MAX_RETRIES, 10) || 3;
export const HMSH_MAX_TIMEOUT_MS =
  parseInt(process.env.HMSH_MAX_TIMEOUT_MS, 10) || 60000;
export const HMSH_GRADUATED_INTERVAL_MS =
  parseInt(process.env.HMSH_GRADUATED_INTERVAL_MS, 10) || 5000;

// MEMFLOW
/**
 * The maximum number of attempts to retry a MemFlow job before it is considered failed.
 * @default 3
 */
export const HMSH_MEMFLOW_MAX_ATTEMPTS = 3;
/**
 * The maximum interval to wait before retrying a MemFlow job.
 * @default 120s
 */
export const HMSH_MEMFLOW_MAX_INTERVAL = '120s';
/**
 * The exponential backoff factor to apply to the interval between retries.
 * @default 10
 */
export const HMSH_MEMFLOW_EXP_BACKOFF = 10;

const BASE_BLOCK_DURATION = 10000;
const TEST_BLOCK_DURATION = 1000;
export const HMSH_BLOCK_TIME_MS = process.env.HMSH_BLOCK_TIME_MS
  ? parseInt(process.env.HMSH_BLOCK_TIME_MS, 10)
  : process.env.NODE_ENV === 'test'
    ? TEST_BLOCK_DURATION
    : BASE_BLOCK_DURATION;

export const HMSH_XCLAIM_DELAY_MS =
  parseInt(process.env.HMSH_XCLAIM_DELAY_MS, 10) || 1000 * 60;
export const HMSH_XCLAIM_COUNT =
  parseInt(process.env.HMSH_XCLAIM_COUNT, 10) || 3;
export const HMSH_XPENDING_COUNT =
  parseInt(process.env.HMSH_XPENDING_COUNT, 10) || 10;

// TASK WORKER
export const HMSH_EXPIRE_DURATION =
  parseInt(process.env.HMSH_EXPIRE_DURATION, 10) || 1;

const BASE_FIDELITY_SECONDS = 5;
const TEST_FIDELITY_SECONDS = 1;
export const HMSH_FIDELITY_SECONDS = process.env.HMSH_FIDELITY_SECONDS
  ? parseInt(process.env.HMSH_FIDELITY_SECONDS, 10)
  : process.env.NODE_ENV === 'test'
    ? TEST_FIDELITY_SECONDS
    : BASE_FIDELITY_SECONDS;

export const HMSH_SCOUT_INTERVAL_SECONDS =
  parseInt(process.env.HMSH_SCOUT_INTERVAL_SECONDS, 10) || 60;

// UTILS
export const HMSH_GUID_SIZE = Math.min(
  parseInt(process.env.HMSH_GUID_SIZE, 10) || 22,
  32,
);

/**
 * Default task queue name used when no task queue is specified
 */
export const DEFAULT_TASK_QUEUE = 'default';
