export type MemFlowChildErrorType = {
  arguments: string[];
  await?: boolean;
  backoffCoefficient?: number;
  index: number;
  expire?: number;
  persistent?: boolean;
  signalIn?: boolean;
  entity?: string;
  maximumAttempts?: number;
  maximumInterval?: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowDimension: string;
  workflowId: string;
  workflowTopic: string;
};

export type MemFlowWaitForAllErrorType = {
  items: string[];
  workflowId: string;
  workflowTopic: string;
  parentWorkflowId: string;
  originJobId: string | null;
  size: number;
  index: number;
  workflowDimension: string;
};

export type MemFlowProxyErrorType = {
  arguments: string[];
  activityName: string;
  backoffCoefficient?: number;
  index: number;
  expire?: number;
  maximumAttempts?: number;
  maximumInterval?: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowDimension: string;
  workflowId: string;
  workflowTopic: string;
};

export type MemFlowWaitForErrorType = {
  signalId: string;
  index: number;
  workflowDimension: string;
  workflowId: string;
};

export type MemFlowSleepErrorType = {
  duration: number;
  index: number;
  workflowDimension: string;
  workflowId: string;
};
