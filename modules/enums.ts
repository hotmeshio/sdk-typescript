import { LogLevel } from '../types/logger';

/**
 * Determines the log level for the application. The default is 'info'.
 */
export const HMSH_LOGLEVEL = (process.env.HMSH_LOGLEVEL as LogLevel) || 'info';
/**
 * Controls which OpenTelemetry spans are emitted. Spans are only
 * captured when an OTel SDK (TracerProvider + exporter) is registered
 * before workers start. Without an SDK, all span calls are no-ops.
 *
 * ## Modes
 *
 * | Value | Durable workflows | YAML flows |
 * |-------|-------------------|------------|
 * | `'info'` (default) | `WORKFLOW/START`, `WORKFLOW/COMPLETE`, `WORKFLOW/ERROR`, `ACTIVITY/{name}` | trigger + worker spans |
 * | `'debug'` | All `info` spans + `DISPATCH/{type}/{name}/{idx}`, `RETURN/{type}/{name}/{idx}` + engine internals | All activity types + all stream hops |
 *
 * ## Setup
 *
 * Register an OpenTelemetry SDK with a trace exporter **before**
 * calling `Durable.Worker.create()`. Any OTLP-compatible backend
 * works (Honeycomb, Jaeger, Grafana Tempo, etc.):
 *
 * ```typescript
 * import { NodeSDK } from '@opentelemetry/sdk-node';
 * import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
 * import { resourceFromAttributes } from '@opentelemetry/resources';
 * import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
 *
 * const sdk = new NodeSDK({
 *   resource: resourceFromAttributes({
 *     [ATTR_SERVICE_NAME]: 'my-service',
 *   }),
 *   traceExporter: new OTLPTraceExporter({
 *     url: 'https://api.honeycomb.io/v1/traces',
 *     headers: { 'x-honeycomb-team': process.env.HONEYCOMB_API_KEY },
 *   }),
 * });
 * sdk.start();
 *
 * // Then start workers — spans flow automatically
 * await Durable.Worker.create({ ... });
 * ```
 *
 * ```bash
 * # Concise: workflow lifecycle + activity execution only
 * HMSH_TELEMETRY=info node worker.js
 *
 * # Verbose: adds per-operation DISPATCH/RETURN spans + engine internals
 * HMSH_TELEMETRY=debug node worker.js
 * ```
 */
export const HMSH_TELEMETRY =
  (process.env.HMSH_TELEMETRY as 'info' | 'debug') || 'info';
/**
 * Default cleanup time for signal in the db when its associated job is completed.
 */
export const HMSH_SIGNAL_EXPIRE = 3_600; //seconds

/**
 * Default TTL for pending signals (signals that arrived before the hook registered).
 * The signaler can override this via the `$expire` field in the signal data
 * using a natural-language duration (e.g., '1h', '24h').
 */
export const HMSH_PENDING_SIGNAL_EXPIRE = 600; //seconds (10 minutes)

// HOTMESH STATUS CODES
export const HMSH_CODE_SUCCESS = 200;
export const HMSH_CODE_PENDING = 202;
export const HMSH_CODE_NOTFOUND = 404;
export const HMSH_CODE_INTERRUPT = 410;
export const HMSH_CODE_UNKNOWN = 500;
export const HMSH_CODE_TIMEOUT = 504;
export const HMSH_CODE_UNACKED = 999;
// DURABLE STATUS CODES
/**
 * This is thrown when a Durable has been interrupted by a sleepFor call.
 */
export const HMSH_CODE_DURABLE_SLEEP = 588;
/**
 * This is thrown when a Durable has been interrupted by a Promise.all call.
 */
export const HMSH_CODE_DURABLE_ALL = 589;
/**
 * This is thrown when a Durable has been interrupted by an execChild or startChild call.
 */
export const HMSH_CODE_DURABLE_CHILD = 590;
/**
 * This is thrown when a Durable has been interrupted by a proxyActivity call.
 */
export const HMSH_CODE_DURABLE_PROXY = 591;
/**
 * This is thrown when a Durable has been interrupted by a continueAsNew call.
 */
export const HMSH_CODE_DURABLE_CONTINUE = 592;
/**
 * This is thrown when a Durable has been interrupted by a waitForSignal call.
 */
export const HMSH_CODE_DURABLE_WAIT = 595;
/**
 * The timeout status code for Durable. This status code is thrown when Durable has encountered a timeout error and needs to aler the caller why the call failed.
 */
export const HMSH_CODE_DURABLE_TIMEOUT = 596;
/**
 * The maxed status code for Durable. This status code is used to indicate that the Durable has reached the maximum
 * number of attempts and should be halted. Thrown from a proxied activity or a flow to halt standard execution
 * and prevent further attempts.
 */
export const HMSH_CODE_DURABLE_MAXED = 597;
/**
 * The fatal status code for Durable. This status code is used to indicate that the Durable has encountered a fatal error. Throw from a proxied activity or a flow to halt standard execution.
 */
export const HMSH_CODE_DURABLE_FATAL = 598;
/**
 * The retryable status code for Durable. This status code is used to indicate that the Durable has encountered a retryable error (essentially unknown and covered by the standard retry policy).
 */
export const HMSH_CODE_DURABLE_RETRYABLE = 599;

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
export const HMSH_POISON_MESSAGE_THRESHOLD =
  parseInt(process.env.HMSH_POISON_MESSAGE_THRESHOLD, 10) || 5;
export const HMSH_MAX_CYCLES =
  parseInt(process.env.HMSH_MAX_CYCLES, 10) || 10_000;
export const HMSH_MAX_TIMEOUT_MS =
  parseInt(process.env.HMSH_MAX_TIMEOUT_MS, 10) || 60000;
export const HMSH_GRADUATED_INTERVAL_MS =
  parseInt(process.env.HMSH_GRADUATED_INTERVAL_MS, 10) || 5000;

// DURABLE
/**
 * The maximum number of attempts to retry a Durable job before it is considered failed.
 * @default 50
 */
export const HMSH_DURABLE_MAX_ATTEMPTS = 50;
/**
 * The maximum interval to wait before retrying a Durable job.
 * @default 120s
 */
export const HMSH_DURABLE_MAX_INTERVAL = '120s';
/**
 * The exponential backoff factor to apply to the interval between retries.
 * @default 5
 */
export const HMSH_DURABLE_EXP_BACKOFF = 5;
/**
 * The initial interval (in seconds) before the first retry attempt.
 * The retry formula is: initialInterval * backoffCoefficient^retryCount,
 * clamped by maximumInterval.
 * @default 1
 */
export const HMSH_DURABLE_INITIAL_INTERVAL = 1;

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
export const HMSH_BATCH_SIZE =
  parseInt(process.env.HMSH_BATCH_SIZE, 10) || 10;
/**
 * Minimum batch size under adaptive scaling (default: 1).
 *
 * When stream depth is high, the adaptive logic reduces batch size
 * to relieve back-pressure. This value is the floor — the smallest
 * batch the system will fetch per consume cycle.
 *
 *   - 1 (default): fully serial under max stress, safest
 *   - 2: retains some parallelism while limiting contention
 *
 * Both values produce equivalent throughput in practice (~233s for
 * 1000 concurrent workflows). The reduction from the configured
 * HMSH_BATCH_SIZE is what matters most — the floor is a safety net.
 */
export const HMSH_BATCH_SIZE_MIN =
  parseInt(process.env.HMSH_BATCH_SIZE_MIN, 10) || 1;

/**
 * Number of concurrent engine consumers per appId (default: 1).
 * Each consumer independently dequeues from the engine stream
 * using FOR UPDATE SKIP LOCKED, distributing message processing
 * across the mesh.
 */
export const HMSH_ENGINE_CONCURRENCY =
  parseInt(process.env.HMSH_ENGINE_CONCURRENCY, 10) || 1;

/**
 * Postgres stream reservation timeout in seconds (default: 30).
 *
 * This is the **starting** reservation timeout for the Postgres stream
 * consumer. When a consumer reserves a message from the stream, it must
 * acknowledge it within this window. If processing takes longer, the
 * message becomes available to other consumers — causing duplicate
 * delivery, collation errors, and wasted CPU.
 *
 * **Adaptive behavior:** The router automatically adjusts this timeout
 * at runtime based on stream depth. When the queue backs up (depth > 100),
 * the timeout doubles (up to 600s). When the queue drains (depth < 10),
 * it halves back toward this configured default. This prevents duplicate
 * delivery under burst load without manual intervention.
 *
 * **When to increase this value:** If you see `process-event-*-error`
 * warnings at `warn` level or `stream-reservation-timeout-adjusted` logs
 * scaling up frequently, your baseline is too low for your workload.
 * Setting a higher default reduces how aggressively the system must
 * adapt during load spikes.
 *
 * **Symptoms of a value that is too low:**
 *   - `collation-error` from `verifySyntheticInteger` (warn level)
 *   - `process-event-collation-rate-exceeded` warning with guidance
 *   - `stream-reservation-timeout-adjusted` logs showing rapid scaling
 *   - Workflow stalls or timeouts under sustained concurrent load
 *
 * @example
 * // Production with sustained high concurrency
 * HMSH_RESERVATION_TIMEOUT_S=120
 *
 * // Low-latency environments with fast processing
 * HMSH_RESERVATION_TIMEOUT_S=30  (default)
 */
export const HMSH_RESERVATION_TIMEOUT_S =
  parseInt(process.env.HMSH_RESERVATION_TIMEOUT_S, 10) || 30;

/**
 * Maximum reservation timeout in seconds for adaptive scaling (default: 1800).
 *
 * This is the ceiling for the adaptive reservation timeout — how far the
 * system is allowed to stretch under sustained load. The adaptive logic
 * only uses what it needs based on stream depth; this value defines the
 * upper bound, not the steady state.
 *
 * The tradeoff is recovery time after a consumer crash: if a consumer
 * reserves a message and dies, that message is unavailable until the
 * timeout expires. A higher ceiling means longer recovery from crashes
 * but prevents duplicate delivery under heavy sustained load.
 *
 * In practice, crashes are rare and the delay is bounded. The cost of
 * a ceiling that is too low — duplicate delivery, collation errors,
 * wasted CPU, workflow stalls — is far higher than a slightly longer
 * recovery window after a crash.
 *
 * **Tuning guidance:**
 *   - Dedicated infrastructure with ample CPU: lower ceiling is fine (600s)
 *   - Shared/multi-tenant or CPU-constrained: use the default (1800s)
 *   - Long-running batch imports or large workflow graphs: increase (3600s+)
 *   - Cloud deployments without CPU contention: the adaptive logic will
 *     naturally stay near the starting timeout and rarely approach the ceiling
 */
export const HMSH_RESERVATION_TIMEOUT_MAX_S =
  parseInt(process.env.HMSH_RESERVATION_TIMEOUT_MAX_S, 10) || 1800;

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

// ROUTER SCOUT - polls for visible messages when LISTEN/NOTIFY is insufficient
export const HMSH_ROUTER_SCOUT_INTERVAL_SECONDS =
  parseInt(process.env.HMSH_ROUTER_SCOUT_INTERVAL_SECONDS, 10) || 60;

const BASE_ROUTER_SCOUT_INTERVAL_MS = 7_000;
const TEST_ROUTER_SCOUT_INTERVAL_MS = 7_000;
export const HMSH_ROUTER_SCOUT_INTERVAL_MS = process.env.HMSH_ROUTER_SCOUT_INTERVAL_MS
  ? parseInt(process.env.HMSH_ROUTER_SCOUT_INTERVAL_MS, 10)
  : process.env.NODE_ENV === 'test'
    ? TEST_ROUTER_SCOUT_INTERVAL_MS
    : BASE_ROUTER_SCOUT_INTERVAL_MS;

// UTILS
export const HMSH_GUID_SIZE = Math.min(
  parseInt(process.env.HMSH_GUID_SIZE, 10) || 22,
  32,
);

/**
 * Default task queue name used when no task queue is specified
 */
export const DEFAULT_TASK_QUEUE = 'default';

// DURESS DETECTION — adaptive engine throttling based on processing latency
/**
 * EMA smoothing factor for duress latency tracking.
 * Higher = faster response to spikes, lower = more stable.
 * @default 0.3
 */
export const HMSH_DURESS_ALPHA =
  parseFloat(process.env.HMSH_DURESS_ALPHA) || 0.3;
/**
 * Number of messages between duress evaluations.
 * @default 10
 */
export const HMSH_DURESS_EVAL_INTERVAL =
  parseInt(process.env.HMSH_DURESS_EVAL_INTERVAL, 10) || 10;
/**
 * Max EMA (ms) below which the engine is considered healthy. No throttle applied.
 * @default 200
 */
export const HMSH_DURESS_HEALTHY_CEILING_MS =
  parseInt(process.env.HMSH_DURESS_HEALTHY_CEILING_MS, 10) || 200;
/**
 * Max EMA (ms) below which duress is mild. Light throttle (100-500ms).
 * @default 1000
 */
export const HMSH_DURESS_MILD_CEILING_MS =
  parseInt(process.env.HMSH_DURESS_MILD_CEILING_MS, 10) || 1000;
/**
 * Max EMA (ms) below which duress is moderate. Moderate throttle (500-2000ms).
 * Above this threshold, duress is severe (2000-5000ms throttle).
 * @default 5000
 */
export const HMSH_DURESS_MODERATE_CEILING_MS =
  parseInt(process.env.HMSH_DURESS_MODERATE_CEILING_MS, 10) || 5000;
/**
 * Minimum interval (ms) between quorum duress broadcasts.
 * @default 5000
 */
export const HMSH_DURESS_BROADCAST_INTERVAL_MS =
  parseInt(process.env.HMSH_DURESS_BROADCAST_INTERVAL_MS, 10) || 5000;
/**
 * Number of consecutive improving evaluations required before
 * dropping a duress level. Prevents oscillation.
 * @default 3
 */
export const HMSH_DURESS_HYSTERESIS_COUNT =
  parseInt(process.env.HMSH_DURESS_HYSTERESIS_COUNT, 10) || 3;

/**
 * PostgreSQL NOTIFY payload limit. If a job message exceeds this size,
 * a reference message is sent instead and the subscriber fetches via getState.
 * PostgreSQL hard limit is 8000 bytes; default 7500 provides safety margin.
 */
export const HMSH_NOTIFY_PAYLOAD_LIMIT =
  parseInt(process.env.HMSH_NOTIFY_PAYLOAD_LIMIT, 10) || 7500;

/**
 * PostgreSQL LISTEN/NOTIFY fallback polling interval in milliseconds.
 * Used when LISTEN/NOTIFY is unavailable or fails. Default 30 seconds.
 */
export const HMSH_ROUTER_POLL_FALLBACK_INTERVAL =
  parseInt(process.env.HOTMESH_POSTGRES_FALLBACK_INTERVAL, 10) || 30000;
