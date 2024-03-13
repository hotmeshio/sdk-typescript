import { StringAnyType, StringStringType } from "./serializer";

export type ExportItem = [(string | null), string, any];

export interface ExportOptions {};

export type JobAction = {
  cursor: number;
  items: ExportItem[];
};

export interface JobActionExport {
  hooks: {
    [key: string]: JobAction;
  };
  main: JobAction;
};

export interface ActivityAction {
  action: string;
  target: string;
}

export interface JobTimeline {
  activity: string; //activity name
  dimension: string; //dimensional isolate path
  duplex: 'entry' | 'exit'; //activity entry or exit
  timestamp: string; //actually a number but too many digits for JS
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
};

export interface ExportCycles {
  [key: string]: string[];
};

export interface DurableJobExport {
  data: StringAnyType;
  dependencies: DependencyExport[];
  state: StringAnyType;
  status: string;
  timeline: JobTimeline[];
  transitions: ExportTransitions;
  cycles: ExportCycles;
};

export interface JobExport {
  dependencies: DependencyExport[];
  process: StringAnyType;
  status: string;
};