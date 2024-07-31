export type MeshFlowChildErrorType = {
  arguments: string[];
  await?: boolean;
  backoffCoefficient?: number;
  index: number;
  expire?: number;
  threshold?: number;
  signalIn?: boolean;
  maximumAttempts?: number;
  maximumInterval?: number;
  originJobId: string | null;
  parentWorkflowId: string;
  workflowDimension: string;
  workflowId: string;
  workflowTopic: string;
};

export type MeshFlowWaitForAllErrorType = {
  items: string[];
  workflowId: string;
  workflowTopic: string;
  parentWorkflowId: string;
  originJobId: string | null;
  size: number;
  index: number;
  workflowDimension: string;
};

export type MeshFlowProxyErrorType = {
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

export type MeshFlowWaitForErrorType = {
  signalId: string;
  index: number;
  workflowDimension: string;
  workflowId: string;
};

export type MeshFlowSleepErrorType = {
  duration: number;
  index: number;
  workflowDimension: string;
  workflowId: string;
};
