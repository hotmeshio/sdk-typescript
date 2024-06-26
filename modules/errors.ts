import { ActivityDuplex } from '../types/activity';
import { CollationFaultType, CollationStage } from '../types/collator';
import {
  DurableChildErrorType,
  DurableProxyErrorType,
  DurableSleepErrorType,
  DurableWaitForAllErrorType,
  DurableWaitForErrorType,
} from '../types/error';

import {
  HMSH_CODE_DURABLE_MAXED,
  HMSH_CODE_DURABLE_TIMEOUT,
  HMSH_CODE_DURABLE_FATAL,
  HMSH_CODE_NOTFOUND,
  HMSH_CODE_DURABLE_RETRYABLE,
  HMSH_CODE_DURABLE_WAIT,
  HMSH_CODE_DURABLE_PROXY,
  HMSH_CODE_DURABLE_CHILD,
  HMSH_CODE_DURABLE_ALL,
  HMSH_CODE_DURABLE_SLEEP,
} from './enums';

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
    super('Error occurred while setting job state');
  }
}

class DurableWaitForError extends Error {
  code: number;
  signalId: string;
  workflowId: string;
  index: number;
  workflowDimension: string; //hook workflowDimension (e.g., ',0,1,0') (use empty string for `null`)
  constructor(params: DurableWaitForErrorType) {
    super(`WaitFor Interruption`);
    this.signalId = params.signalId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_DURABLE_WAIT;
  }
}

class DurableProxyError extends Error {
  activityName: string;
  arguments: string[];
  backoffCoefficient: number;
  code: number;
  index: number;
  maximumAttempts: number;
  maximumInterval: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowDimension: string;
  workflowId: string;
  workflowTopic: string;
  constructor(params: DurableProxyErrorType) {
    super(`ProxyActivity Interruption`);
    this.arguments = params.arguments;
    this.workflowId = params.workflowId;
    this.workflowTopic = params.workflowTopic;
    this.parentWorkflowId = params.parentWorkflowId;
    this.originJobId = params.originJobId;
    this.index = params.index;
    this.activityName = params.activityName;
    this.workflowDimension = params.workflowDimension;
    this.backoffCoefficient = params.backoffCoefficient;
    this.maximumAttempts = params.maximumAttempts;
    this.maximumInterval = params.maximumInterval;
    this.code = HMSH_CODE_DURABLE_PROXY;
  }
}

class DurableChildError extends Error {
  await: boolean;
  arguments: string[];
  backoffCoefficient: number;
  code: number;
  workflowDimension: string;
  index: number;
  maximumAttempts: number;
  maximumInterval: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowId: string;
  workflowTopic: string;
  constructor(params: DurableChildErrorType) {
    super(`ExecChild Interruption`);
    this.arguments = params.arguments;
    this.workflowId = params.workflowId;
    this.workflowTopic = params.workflowTopic;
    this.parentWorkflowId = params.parentWorkflowId;
    this.originJobId = params.originJobId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_DURABLE_CHILD;
    this.await = params.await;
    this.backoffCoefficient = params.backoffCoefficient;
    this.maximumAttempts = params.maximumAttempts;
    this.maximumInterval = params.maximumInterval;
  }
}

class DurableWaitForAllError extends Error {
  items: any[];
  code: number;
  workflowDimension: string;
  size: number;
  index: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowId: string;
  workflowTopic: string;
  constructor(params: DurableWaitForAllErrorType) {
    super(`Collation Interruption`);
    this.items = params.items;
    this.size = params.size;
    this.workflowId = params.workflowId;
    this.workflowTopic = params.workflowTopic;
    this.parentWorkflowId = params.parentWorkflowId;
    this.originJobId = params.originJobId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_DURABLE_ALL;
  }
}

class DurableSleepError extends Error {
  workflowId: string;
  code: number;
  duration: number; //seconds
  index: number;
  workflowDimension: string; //empty string for null
  constructor(params: DurableSleepErrorType) {
    super(`SleepFor Interruption`);
    this.duration = params.duration;
    this.workflowId = params.workflowId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_DURABLE_SLEEP;
  }
}

class DurableTimeoutError extends Error {
  code: number;
  constructor(message: string, stack?: string) {
    super(message);
    if (this.stack) {
      this.stack = stack;
    }
    this.code = HMSH_CODE_DURABLE_TIMEOUT;
  }
}
class DurableMaxedError extends Error {
  code: number;
  constructor(message: string, stackTrace?: string) {
    super(message);
    if (stackTrace) {
      this.stack = stackTrace;
    }
    this.code = HMSH_CODE_DURABLE_MAXED;
  }
}
class DurableFatalError extends Error {
  code: number;
  constructor(message: string, stackTrace?: string) {
    super(message);
    if (stackTrace) {
      this.stack = stackTrace;
    }
    this.code = HMSH_CODE_DURABLE_FATAL;
  }
}
class DurableRetryError extends Error {
  code: number;
  constructor(message: string, stackTrace?: string) {
    super(message);
    if (stackTrace) {
      this.stack = stackTrace;
    }
    this.code = HMSH_CODE_DURABLE_RETRYABLE;
  }
}

class MapDataError extends Error {
  constructor() {
    super('Error occurred while mapping data');
  }
}

class RegisterTimeoutError extends Error {
  constructor() {
    super('Error occurred while registering activity timeout');
  }
}

class DuplicateJobError extends Error {
  jobId: string;
  constructor(jobId: string) {
    super('Duplicate job');
    this.jobId = jobId;
    this.message = `Duplicate job: ${jobId}`;
  }
}
class InactiveJobError extends Error {
  jobId: string;
  activityId: string;
  status: number; //non-positive integer
  constructor(jobId: string, status: number, activityId: string) {
    super('Inactive job');
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

  constructor(
    expected: string,
    actual: string,
    jobId: string,
    activityId: string,
    dimensionalAddress: string,
  ) {
    super('Generational Error');
    this.expected = expected;
    this.actual = actual;
    this.jobId = jobId;
    this.activityId = activityId;
    this.dimensionalAddress = dimensionalAddress;
  }
}

class ExecActivityError extends Error {
  constructor() {
    super('Error occurred while executing activity');
  }
}

class CollationError extends Error {
  status: number; //15-digit activity collation integer (889000001000001)
  leg: ActivityDuplex;
  stage: CollationStage; //enter | exit | confirm
  fault: CollationFaultType; //missing, invalid, etc

  constructor(
    status: number,
    leg: ActivityDuplex,
    stage: CollationStage,
    fault?: CollationFaultType,
  ) {
    super('collation-error');
    this.leg = leg;
    this.status = status;
    this.stage = stage;
    this.fault = fault;
  }
}

export {
  CollationError,
  DurableChildError,
  DurableFatalError,
  DurableMaxedError,
  DurableProxyError,
  DurableRetryError,
  DurableSleepError,
  DurableTimeoutError,
  DurableWaitForAllError,
  DurableWaitForError,
  DuplicateJobError,
  ExecActivityError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
  MapDataError,
  RegisterTimeoutError,
  SetStateError,
};
