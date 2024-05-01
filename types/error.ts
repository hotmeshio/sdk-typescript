export type DurableChildErrorType = {
  arguments: string[],
  await?: boolean,
  backoffCoefficient?: number,
  index: number,
  maximumAttempts?: number,
  maximumInterval?: number,
  originJobId: string | null,
  parentWorkflowId: string,
  workflowDimension: string,
  workflowId: string,
  workflowTopic: string,
};

export type DurableWaitForAllErrorType = {
  items: string[],
  workflowId: string,
  workflowTopic: string,
  parentWorkflowId: string,
  originJobId: string | null,
  size: number,
  index: number,
  workflowDimension: string
};

export type DurableProxyErrorType = {
  arguments: string[],
  activityName: string,
  backoffCoefficient?: number,
  index: number,
  maximumAttempts?: number,
  maximumInterval?: number,
  originJobId: string | null,
  parentWorkflowId: string,
  workflowDimension: string,
  workflowId: string,
  workflowTopic: string,
};

export type DurableWaitForErrorType = {
  signalId: string,
  index: number,
  workflowDimension: string
  workflowId: string;
};

export type DurableSleepErrorType = {
  duration: number,
  index: number,
  workflowDimension: string,
  workflowId: string,
};
