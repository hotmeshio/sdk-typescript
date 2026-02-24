import { StringAnyType } from './serializer';

export type ExportItem = [string | null, string, any];

/**
 * job export data can be large, particularly transitions the timeline
 */
export type ExportFields =
  | 'data'
  | 'state'
  | 'status'
  | 'timeline'
  | 'transitions';

export interface ExportOptions {
  /**
   * limit the export byte size by specifying an allowlist
   */
  allow?: Array<ExportFields>;

  /**
   * limit the export byte size by specifying a block list
   */
  block?: Array<ExportFields>;

  /**
   * If false, do not return timeline values (like child job response, proxy activity response, etc)
   * @default true
   */
  values?: boolean;
}

export type JobAction = {
  cursor: number;
  items: ExportItem[];
};

export interface JobActionExport {
  hooks: {
    [key: string]: JobAction;
  };
  main: JobAction;
}

export interface ActivityAction {
  action: string;
  target: string;
}

export interface JobTimeline {
  activity: string; //activity name
  dimension: string; //dimensional isolate path
  duplex: 'entry' | 'exit'; //activity entry or exit
  timestamp: string; //actually a number but too many digits for JS
  created?: string; //actually a number but too many digits for JS
  updated?: string; //actually a number but too many digits for JS
  actions?: ActivityAction[];
}

export interface DependencyExport {
  type: string;
  topic: string;
  gid: string;
  jid: string;
}

export interface ExportTransitions {
  [key: string]: string[];
}

export interface ExportCycles {
  [key: string]: string[];
}

export type TimelineType = {
  key: string;
  value: Record<string, any> | string | number | null;
  index: number;
  secondary?: number;
  dimension?: string;
};

export interface TransitionType {
  activity: string;
  dimensions: string;
  created: string;
  updated: string;
}

export interface DurableJobExport {
  data?: StringAnyType;
  state?: StringAnyType;
  status?: number;
  timeline?: TimelineType[];
  transitions?: TransitionType[];
}

export interface JobExport {
  dependencies: DependencyExport[];
  process: StringAnyType;
  status: string;
}

// ── Temporal-compatible workflow execution export types ────────────────────────

export type ExportMode = 'sparse' | 'verbose';

export type WorkflowEventType =
  | 'workflow_execution_started'
  | 'workflow_execution_completed'
  | 'workflow_execution_failed'
  | 'activity_task_scheduled'
  | 'activity_task_completed'
  | 'activity_task_failed'
  | 'child_workflow_execution_started'
  | 'child_workflow_execution_completed'
  | 'child_workflow_execution_failed'
  | 'timer_started'
  | 'timer_fired'
  | 'workflow_execution_signaled';

export type WorkflowEventCategory =
  | 'workflow'
  | 'activity'
  | 'child_workflow'
  | 'timer'
  | 'signal';

export interface WorkflowExecutionStartedAttributes {
  kind: 'workflow_execution_started';
  workflow_type: string;
  task_queue: string;
  input?: any;
}

export interface WorkflowExecutionCompletedAttributes {
  kind: 'workflow_execution_completed';
  result?: any;
}

export interface WorkflowExecutionFailedAttributes {
  kind: 'workflow_execution_failed';
  failure?: string;
}

export interface ActivityTaskScheduledAttributes {
  kind: 'activity_task_scheduled';
  activity_type: string;
  timeline_key: string;
  execution_index: number;
}

export interface ActivityTaskCompletedAttributes {
  kind: 'activity_task_completed';
  activity_type: string;
  result?: any;
  scheduled_event_id?: number;
  timeline_key: string;
  execution_index: number;
}

export interface ActivityTaskFailedAttributes {
  kind: 'activity_task_failed';
  activity_type: string;
  failure?: any;
  scheduled_event_id?: number;
  timeline_key: string;
  execution_index: number;
}

export interface ChildWorkflowExecutionStartedAttributes {
  kind: 'child_workflow_execution_started';
  child_workflow_id: string;
  awaited: boolean;
  timeline_key: string;
  execution_index: number;
}

export interface ChildWorkflowExecutionCompletedAttributes {
  kind: 'child_workflow_execution_completed';
  child_workflow_id: string;
  result?: any;
  initiated_event_id?: number;
  timeline_key: string;
  execution_index: number;
}

export interface ChildWorkflowExecutionFailedAttributes {
  kind: 'child_workflow_execution_failed';
  child_workflow_id: string;
  failure?: any;
  initiated_event_id?: number;
  timeline_key: string;
  execution_index: number;
}

export interface TimerStartedAttributes {
  kind: 'timer_started';
  duration_ms?: number;
  timeline_key: string;
  execution_index: number;
}

export interface TimerFiredAttributes {
  kind: 'timer_fired';
  timeline_key: string;
  execution_index: number;
}

export interface WorkflowExecutionSignaledAttributes {
  kind: 'workflow_execution_signaled';
  signal_name: string;
  input?: any;
  timeline_key: string;
  execution_index: number;
}

export type WorkflowEventAttributes =
  | WorkflowExecutionStartedAttributes
  | WorkflowExecutionCompletedAttributes
  | WorkflowExecutionFailedAttributes
  | ActivityTaskScheduledAttributes
  | ActivityTaskCompletedAttributes
  | ActivityTaskFailedAttributes
  | ChildWorkflowExecutionStartedAttributes
  | ChildWorkflowExecutionCompletedAttributes
  | ChildWorkflowExecutionFailedAttributes
  | TimerStartedAttributes
  | TimerFiredAttributes
  | WorkflowExecutionSignaledAttributes;

export interface WorkflowExecutionEvent {
  event_id: number;
  event_type: WorkflowEventType;
  category: WorkflowEventCategory;
  event_time: string;
  duration_ms: number | null;
  is_system: boolean;
  attributes: WorkflowEventAttributes;
}

export interface WorkflowExecutionSummary {
  total_events: number;
  activities: {
    total: number;
    completed: number;
    failed: number;
    system: number;
    user: number;
  };
  child_workflows: {
    total: number;
    completed: number;
    failed: number;
  };
  timers: number;
  signals: number;
}

export type WorkflowExecutionStatus = 'running' | 'completed' | 'failed';

export interface WorkflowExecution {
  workflow_id: string;
  workflow_type: string;
  task_queue: string;
  status: WorkflowExecutionStatus;
  start_time: string | null;
  close_time: string | null;
  duration_ms: number | null;
  result: any;
  events: WorkflowExecutionEvent[];
  summary: WorkflowExecutionSummary;
  children?: WorkflowExecution[];
}

export interface ExecutionExportOptions {
  mode?: ExportMode;
  exclude_system?: boolean;
  omit_results?: boolean;
  max_depth?: number;
}
