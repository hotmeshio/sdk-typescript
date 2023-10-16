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
  DuplicateJobError,
  GetStateError,
  SetStateError,
  MapDataError,
  RegisterTimeoutError,
  ExecActivityError
};
