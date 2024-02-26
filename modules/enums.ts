// Engine Constants
export const STATUS_CODE_SUCCESS = 200;
export const STATUS_CODE_PENDING = 202;
export const STATUS_CODE_TIMEOUT = 504;
export const STATUS_CODE_INTERRUPT = 410;
export const OTT_WAIT_TIME = 1000;

// Stream Constants
export const MAX_RETRIES = 3; //local retry; 10, 100, 1000ms
export const MAX_TIMEOUT_MS = 60000;
export const GRADUATED_INTERVAL_MS = 5000;

export const BLOCK_DURATION = 15000; //Set to `15` so SIGINT/SIGTERM can interrupt; set to `0` to BLOCK indefinitely
export const TEST_BLOCK_DURATION = 1000; //Set to `1000` so tests can interrupt quickly
export const BLOCK_TIME_MS = process.env.NODE_ENV === 'test' ? TEST_BLOCK_DURATION : BLOCK_DURATION;

export const XCLAIM_DELAY_MS = 1000 * 60; //max time a message can be unacked before it is claimed by another
export const XCLAIM_COUNT = 3; //max number of times a message can be claimed by another before it is dead-lettered
export const XPENDING_COUNT = 10;

export const STATUS_CODE_UNACKED = 999;
export const STATUS_CODE_UNKNOWN = 500;
export const STATUS_MESSAGE_UNKNOWN = 'unknown';

// HotMesh Constants
export const EXPIRE_DURATION = 15;        // default expire in seconds; once job state semaphore reaches '0', this is applied to set Redis to expire the job HASH
export const BASE_FIDELITY_SECONDS = 15;  // granularity resolution window size
export const TEST_FIDELITY_SECONDS = 5;
export const FIDELITY_SECONDS = process.env.NODE_ENV === 'test' ? TEST_FIDELITY_SECONDS : BASE_FIDELITY_SECONDS

// DURABLE CONSTANTS
export const DURABLE_EXPIRE_SECONDS = 1;
