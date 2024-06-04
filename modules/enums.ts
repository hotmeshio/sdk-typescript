import { LogLevel } from '../types/logger';

// HOTMESH SYSTEM
export const HMSH_LOGLEVEL = (process.env.HMSH_LOGLEVEL as LogLevel) || 'info';

// HOTMESH STATUS CODES
export const HMSH_CODE_SUCCESS = 200;
export const HMSH_CODE_PENDING = 202;
export const HMSH_CODE_NOTFOUND = 404;
export const HMSH_CODE_INTERRUPT = 410;
export const HMSH_CODE_UNKNOWN = 500;
export const HMSH_CODE_TIMEOUT = 504;
export const HMSH_CODE_UNACKED = 999;
// DURABLE STATUS CODES
export const HMSH_CODE_DURABLE_SLEEP = 588;
export const HMSH_CODE_DURABLE_ALL = 589;
export const HMSH_CODE_DURABLE_CHILD = 590;
export const HMSH_CODE_DURABLE_PROXY = 591;
export const HMSH_CODE_DURABLE_WAIT = 595;
export const HMSH_CODE_DURABLE_TIMEOUT = 596;
export const HMSH_CODE_DURABLE_MAXED = 597;
export const HMSH_CODE_DURABLE_FATAL = 598;
export const HMSH_CODE_DURABLE_RETRYABLE = 599;

// HOTMESH MESSAGES
export const HMSH_STATUS_UNKNOWN = 'unknown';

// QUORUM
export const HMSH_QUORUM_ROLLCALL_CYCLES = 12; //max iterations
export const HMSH_QUORUM_DELAY_MS = 250;
export const HMSH_ACTIVATION_MAX_RETRY = 3;

// ENGINE
export const HMSH_OTT_WAIT_TIME =
  parseInt(process.env.HMSH_OTT_WAIT_TIME, 10) || 1000;
export const HMSH_EXPIRE_JOB_SECONDS =
  parseInt(process.env.HMSH_EXPIRE_JOB_SECONDS, 10) || 1;

// STREAM ROUTER
export const HMSH_MAX_RETRIES = parseInt(process.env.HMSH_MAX_RETRIES, 10) || 3;
export const HMSH_MAX_TIMEOUT_MS =
  parseInt(process.env.HMSH_MAX_TIMEOUT_MS, 10) || 60000;
export const HMSH_GRADUATED_INTERVAL_MS =
  parseInt(process.env.HMSH_GRADUATED_INTERVAL_MS, 10) || 5000;

// DURABLE
export const HMSH_DURABLE_MAX_ATTEMPTS = 3;
export const HMSH_DURABLE_MAX_INTERVAL = '120s';
export const HMSH_DURABLE_EXP_BACKOFF = 10;

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
