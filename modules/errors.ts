import { ActivityDuplex } from "../types/activity";
import { CollationFaultType, CollationStage } from "../types/collator";
import {
  HMSH_CODE_DURABLE_MAXED,
  HMSH_CODE_DURABLE_TIMEOUT,
  HMSH_CODE_DURABLE_FATAL,
  HMSH_CODE_DURABLE_INCOMPLETE,
  HMSH_CODE_NOTFOUND,
  HMSH_CODE_DURABLE_RETRYABLE,
  HMSH_CODE_DURABLE_SLEEPFOR,
  HMSH_CODE_DURABLE_WAITFOR } from "./enums";

class GetStateError extends Error {
  jobId: string;
  code = HMSH_CODE_NOTFOUND;
  constructor(jobId: string) {
    super(`${jobId} Not Found`);
    this.jobId = jobId;
  }
}
class SetStateError extends Error {
  constructor() {
    super("Error occurred while setting job state");
  }
}

//thrown when a signal set is incomplete but already configured
//if a waitFor set has 'n' items, this can be thrown `n - 1` times
class DurableIncompleteSignalError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = HMSH_CODE_DURABLE_INCOMPLETE;
  }
}

//the original waitFor error that is thrown for a new signal set
class DurableWaitForSignalError extends Error {
  code: number;
  signals: {signal: string, index: number}[]; //signal id and execution order in the workflow
  constructor(message: string, signals: {signal: string, index: number}[]) {
    super(message);
    this.signals = signals;
    this.code = HMSH_CODE_DURABLE_WAITFOR;
  }
}

class DurableSleepForError extends Error {
  code: number;
  duration: number; //seconds
  index: number;    //execution order in the workflow
  dimension: string; //hook dimension (e.g., ',0,1,0') (uses empty string for `null`)
  constructor(message: string, duration: number, index: number, dimension: string) {
    super(message);
    this.duration = duration;
    this.index = index;
    this.dimension = dimension;
    this.code = HMSH_CODE_DURABLE_SLEEPFOR;
  }
}
class DurableTimeoutError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = HMSH_CODE_DURABLE_TIMEOUT;
  }
}
class DurableMaxedError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = HMSH_CODE_DURABLE_MAXED;
  }
}
class DurableFatalError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = HMSH_CODE_DURABLE_FATAL;
  }
}
class DurableRetryError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = HMSH_CODE_DURABLE_RETRYABLE;
  }
}

class MapDataError extends Error {
  constructor() {
    super("Error occurred while mapping data");
  }
}

class RegisterTimeoutError extends Error {
  constructor() {
    super("Error occurred while registering activity timeout");
  }
}

class DuplicateJobError extends Error {
  constructor(jobId: string) {
    super("Duplicate job");
    this.message = `Duplicate job: ${jobId}`;
  }
}
class InactiveJobError extends Error {
  jobId: string;
  activityId: string;
  status: number; //non-positive integer
  constructor(jobId: string, status: number, activityId: string) {
    super("Inactive job");
    this.jobId = jobId;
    this.activityId = activityId;
    this.message = `Inactive job: ${jobId}`;
    this.status = status;
  }
}
class GenerationalError extends Error {
  expected: string;
  actual: string;
  jobId: string;
  activityId: string;
  dimensionalAddress: string;

  constructor(expected: string, actual: string, jobId: string, activityId: string, dimensionalAddress: string) {
    super("Generational Error");
    this.expected = expected;
    this.actual = actual;
    this.jobId = jobId;
    this.activityId = activityId;
    this.dimensionalAddress = dimensionalAddress;
  }
}

class ExecActivityError extends Error {
  constructor() {
    super("Error occurred while executing activity");
  }
}

class CollationError extends Error {
  status: number; //15-digit activity collation integer (889000001000001)
  leg: ActivityDuplex;
  stage: CollationStage; //enter | exit | confirm
  fault: CollationFaultType; //missing, invalid, etc

  constructor(status: number, leg: ActivityDuplex, stage: CollationStage, fault?: CollationFaultType) {
    super("collation-error");
    this.leg = leg;
    this.status = status;
    this.stage = stage;
    this.fault = fault;
  }
}

export {
  CollationError,
  DurableFatalError,
  DurableIncompleteSignalError,
  DurableMaxedError,
  DurableRetryError,
  DurableSleepForError,
  DurableTimeoutError,
  DurableWaitForSignalError,
  DuplicateJobError,
  ExecActivityError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
  MapDataError,
  RegisterTimeoutError,
  SetStateError,
};
