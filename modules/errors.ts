import { ActivityDuplex } from '../types/activity';
import { CollationFaultType, CollationStage } from '../types/collator';
import {
  MeshFlowChildErrorType,
  MeshFlowProxyErrorType,
  MeshFlowSleepErrorType,
  MeshFlowWaitForAllErrorType,
  MeshFlowWaitForErrorType,
} from '../types/error';

import {
  HMSH_CODE_MESHFLOW_MAXED,
  HMSH_CODE_MESHFLOW_TIMEOUT,
  HMSH_CODE_MESHFLOW_FATAL,
  HMSH_CODE_NOTFOUND,
  HMSH_CODE_MESHFLOW_RETRYABLE,
  HMSH_CODE_MESHFLOW_WAIT,
  HMSH_CODE_MESHFLOW_PROXY,
  HMSH_CODE_MESHFLOW_CHILD,
  HMSH_CODE_MESHFLOW_ALL,
  HMSH_CODE_MESHFLOW_SLEEP,
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

class MeshFlowWaitForError extends Error {
  code: number;
  signalId: string;
  workflowId: string;
  index: number;
  workflowDimension: string; //hook workflowDimension (e.g., ',0,1,0') (use empty string for `null`)
  constructor(params: MeshFlowWaitForErrorType) {
    super(`WaitFor Interruption`);
    this.signalId = params.signalId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_MESHFLOW_WAIT;
  }
}

class MeshFlowProxyError extends Error {
  activityName: string;
  arguments: string[];
  backoffCoefficient: number;
  code: number;
  index: number;
  maximumAttempts: number;
  maximumInterval: number;
  originJobId: string | null;
  parentWorkflowId: string;
  expire: number;
  workflowDimension: string;
  workflowId: string;
  workflowTopic: string;
  constructor(params: MeshFlowProxyErrorType) {
    super(`ProxyActivity Interruption`);
    this.arguments = params.arguments;
    this.workflowId = params.workflowId;
    this.workflowTopic = params.workflowTopic;
    this.parentWorkflowId = params.parentWorkflowId;
    this.expire = params.expire;
    this.originJobId = params.originJobId;
    this.index = params.index;
    this.activityName = params.activityName;
    this.workflowDimension = params.workflowDimension;
    this.backoffCoefficient = params.backoffCoefficient;
    this.maximumAttempts = params.maximumAttempts;
    this.maximumInterval = params.maximumInterval;
    this.code = HMSH_CODE_MESHFLOW_PROXY;
  }
}

class MeshFlowChildError extends Error {
  await: boolean;
  arguments: string[];
  backoffCoefficient: number;
  code: number;
  expire: number;
  signalIn: boolean;
  workflowDimension: string;
  index: number;
  maximumAttempts: number;
  maximumInterval: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowId: string;
  workflowTopic: string;
  constructor(params: MeshFlowChildErrorType) {
    super(`ExecChild Interruption`);
    this.arguments = params.arguments;
    this.workflowId = params.workflowId;
    this.workflowTopic = params.workflowTopic;
    this.parentWorkflowId = params.parentWorkflowId;
    this.expire = params.expire;
    this.signalIn = params.signalIn;
    this.originJobId = params.originJobId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_MESHFLOW_CHILD;
    this.await = params.await;
    this.backoffCoefficient = params.backoffCoefficient;
    this.maximumAttempts = params.maximumAttempts;
    this.maximumInterval = params.maximumInterval;
  }
}

class MeshFlowWaitForAllError extends Error {
  items: any[];
  code: number;
  workflowDimension: string;
  size: number;
  index: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowId: string;
  workflowTopic: string;
  constructor(params: MeshFlowWaitForAllErrorType) {
    super(`Collation Interruption`);
    this.items = params.items;
    this.size = params.size;
    this.workflowId = params.workflowId;
    this.workflowTopic = params.workflowTopic;
    this.parentWorkflowId = params.parentWorkflowId;
    this.originJobId = params.originJobId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_MESHFLOW_ALL;
  }
}

class MeshFlowSleepError extends Error {
  workflowId: string;
  code: number;
  duration: number; //seconds
  index: number;
  workflowDimension: string; //empty string for null
  constructor(params: MeshFlowSleepErrorType) {
    super(`SleepFor Interruption`);
    this.duration = params.duration;
    this.workflowId = params.workflowId;
    this.index = params.index;
    this.workflowDimension = params.workflowDimension;
    this.code = HMSH_CODE_MESHFLOW_SLEEP;
  }
}

class MeshFlowTimeoutError extends Error {
  code: number;
  constructor(message: string, stack?: string) {
    super(message);
    if (this.stack) {
      this.stack = stack;
    }
    this.code = HMSH_CODE_MESHFLOW_TIMEOUT;
  }
}
class MeshFlowMaxedError extends Error {
  code: number;
  constructor(message: string, stackTrace?: string) {
    super(message);
    if (stackTrace) {
      this.stack = stackTrace;
    }
    this.code = HMSH_CODE_MESHFLOW_MAXED;
  }
}
class MeshFlowFatalError extends Error {
  code: number;
  constructor(message: string, stackTrace?: string) {
    super(message);
    if (stackTrace) {
      this.stack = stackTrace;
    }
    this.code = HMSH_CODE_MESHFLOW_FATAL;
  }
}
class MeshFlowRetryError extends Error {
  code: number;
  constructor(message: string, stackTrace?: string) {
    super(message);
    if (stackTrace) {
      this.stack = stackTrace;
    }
    this.code = HMSH_CODE_MESHFLOW_RETRYABLE;
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
  MeshFlowChildError,
  MeshFlowFatalError,
  MeshFlowMaxedError,
  MeshFlowProxyError,
  MeshFlowRetryError,
  MeshFlowSleepError,
  MeshFlowTimeoutError,
  MeshFlowWaitForAllError,
  MeshFlowWaitForError,
  DuplicateJobError,
  ExecActivityError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
  MapDataError,
  RegisterTimeoutError,
  SetStateError,
};
