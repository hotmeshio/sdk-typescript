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
  timestamp?: string;
  system?: SystemHealth;
}

//used for coordination (like version activation)
export interface PingMessage {
  type: 'ping';
  originator: string; //guid
  details?: boolean;  //if true, all endpoints will include their profile
}

export interface WorkMessage {
  type: 'work';
  originator: string; //guid
}

export interface CronMessage {
  type: 'cron';
  originator: string; //guid
}

export interface PongMessage {
  type: 'pong';
  guid: string;            //call initiator
  originator: string;      //clone of originator guid passed in ping
  profile?: QuorumProfile; //contains details about the engine/worker
}

export interface ActivateMessage {
  type: 'activate';
  cache_mode: 'nocache' | 'cache';
  until_version: string;
}

export interface JobMessage {
  type: 'job';
  topic: string; //this comes from the 'publishes' field in the YAML
  job: JobOutput
}

//delay in ms between fetches from the buffered stream (speed/slow down entire network)
export interface ThrottleMessage {
  type: 'throttle';
  guid?: string; //target the engine quorum
  topic?: string;  //target a worker quorum
  throttle: number; //0-n
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
export type QuorumMessage = PingMessage | PongMessage | ActivateMessage | WorkMessage | JobMessage | ThrottleMessage | CronMessage;
