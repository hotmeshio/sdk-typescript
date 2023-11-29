import { ActivityDuplex } from "../types/activity";
import { CollationFaultType, CollationStage } from "../types/collator";

class GetStateError extends Error {
  constructor() {
    super("Error occurred while getting job state");
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
    this.code = 593;
  }
}

//the original waitFor error that is thrown for a new signal set
class DurableWaitForSignalError extends Error {
  code: number;
  signals: {signal: string, index: number}[]; //signal id and execution order in the workflow
  constructor(message: string, signals: {signal: string, index: number}[]) {
    super(message);
    this.signals = signals;
    this.code = 594;
  }
}

class DurableSleepError extends Error {
  code: number;
  duration: number; //seconds
  index: number;    //execution order in the workflow
  dimension: string; //hook dimension (e.g., ',0,1,0') (uses empty string for `null`)
  constructor(message: string, duration: number, index: number, dimension: string) {
    super(message);
    this.duration = duration;
    this.index = index;
    this.dimension = dimension;
    this.code = 595;
  }
}
class DurableTimeoutError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = 596;
  }
}
class DurableMaxedError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = 597;
  }
}
class DurableFatalError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = 598;
  }
}
class DurableRetryError extends Error {
  code: number;
  constructor(message: string) {
    super(message);
    this.code = 599;
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
  DurableTimeoutError,
  DurableMaxedError,
  DurableFatalError,
  DurableRetryError,
  DurableWaitForSignalError,
  DurableIncompleteSignalError,
  DurableSleepError,
  DuplicateJobError,
  GetStateError,
  SetStateError,
  MapDataError,
  RegisterTimeoutError,
  ExecActivityError
};
