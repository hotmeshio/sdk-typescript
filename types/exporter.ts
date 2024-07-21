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

export interface MeshFlowJobExport {
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
