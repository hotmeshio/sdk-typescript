import { JobOutput } from "./job";

interface CPULoad {
  [cpu: string]: string;
}

interface NetworkStat {
  iface: string;
  operstate: string;
  rx_bytes: number;
  rx_dropped: number;
  rx_errors: number;
  tx_bytes: number;
  tx_dropped: number;
  tx_errors: number;
  rx_sec: number;
  tx_sec: number;
  ms: number;
}

/** reveals: memory, cpu, network */
export interface SystemHealth {
  TotalMemoryGB: string;
  FreeMemoryGB: string;
  UsedMemoryGB: string;
  CPULoad: CPULoad[];
  NetworkStats: NetworkStat[];
}

export type ThrottleOptions = {
  /** target an engine OR worker by GUID */
  guid?: string;
  /** target a worker quorum */
  topic?: string;
  /** in milliseconds; default is 0 */
  throttle: number;
};

export interface QuorumProfile {
  namespace: string;
  app_id: string;
  engine_id: string;
  worker_topic?: string;
  stream?: string;
  stream_depth?: number;
  counts?: Record<string, number>;
  inited?: string;
  timestamp?: string;
  throttle?: number;
  reclaimDelay?: number;
  reclaimCount?: number;
  system?: SystemHealth;
  signature?: string; //stringified function
}

interface QuorumMessageBase {
  guid?: string;
  topic?: string;
  type?: string;
}

// Messages extending QuorumMessageBase
export interface PingMessage extends QuorumMessageBase {
  type: 'ping';
  originator: string; //guid
  details?: boolean;  //if true, all endpoints will include their profile
}

export interface WorkMessage extends QuorumMessageBase {
  type: 'work';
  originator: string; //guid
}

export interface CronMessage extends QuorumMessageBase {
  type: 'cron';
  originator: string; //guid
}

export interface PongMessage extends QuorumMessageBase {
  type: 'pong';
  guid: string;            //call initiator
  originator: string;      //clone of originator guid passed in ping
  profile?: QuorumProfile; //contains details about the engine/worker
}

export interface ActivateMessage extends QuorumMessageBase {
  type: 'activate';
  cache_mode: 'nocache' | 'cache';
  until_version: string;
}

export interface JobMessage extends QuorumMessageBase {
  type: 'job';
  topic: string; //this comes from the 'publishes' field in the YAML
  job: JobOutput
}

export interface ThrottleMessage extends QuorumMessageBase {
  type: 'throttle';
  guid?: string; //target engine AND workers with this guid
  topic?: string;  //target worker(s) matching this topic (pass null to only target the engine, pass undefined to target engine and workers)
  throttle: number; //0-n; millis
}

export interface RollCallMessage extends QuorumMessageBase {
  type: 'rollcall';
  guid?: string;    //target the engine quorum
  topic?: string | null;   //target a worker if string; suppress if `null`;
  interval: number; //every 'n' seconds
  max?: number;     //max broadcasts
  signature?: boolean; //include bound worker function in broadcast   
}

export interface JobMessageCallback {
  (topic: string, message: JobOutput): void;
}

export interface SubscriptionCallback {
  (topic: string, message: Record<string, any>): void;
}

export interface QuorumMessageCallback {
  (topic: string, message: QuorumMessage): void;
}

/**
 * The types in this file are used to define those messages that are sent
 * to hotmesh client instances when a new version is about to be activated.
 * These messages serve to coordinate the cache invalidation and switch-over
 * to the new version without any downtime and a coordinating parent server.
 */
export type QuorumMessage = PingMessage | PongMessage | ActivateMessage | WorkMessage | JobMessage | ThrottleMessage | RollCallMessage | CronMessage;
