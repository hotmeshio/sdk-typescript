import { JobOutput } from './job';
import { StringAnyType } from './serializer';

export interface CPULoad {
  [cpu: string]: string;
}

export interface NetworkStat {
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

/** Host-level resource snapshot collected at pong time. */
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
  /** entity/noun */
  entity?: string;
  /** in milliseconds; set to -1 for indefinite throttle */
  throttle: number;
  /** namespace */
  namespace?: string;
};

/**
 * Snapshot of a single engine or worker instance, returned by
 * `HotMesh.rollCall()`. Each connected instance responds to the
 * quorum PING with its current profile.
 *
 * **Engines** populate `stream` (the engine stream key).
 * **Workers** populate `worker_topic` (the task queue topic) and `stream`.
 *
 * Use `counts` and `error_count` for throughput and health monitoring.
 */
export interface QuorumProfile {
  /** Namespace the instance belongs to. */
  namespace: string;
  /** Application ID (matches `HotMeshConfig.appId`). */
  app_id: string;
  /** Unique instance GUID (engine or worker). */
  engine_id: string;
  /** Entity name (if applicable). */
  entity?: string;
  /** Worker task queue topic. Present only for worker instances. */
  worker_topic?: string;
  /** Stream key this instance consumes from. */
  stream?: string;
  /** Number of pending (unprocessed) messages in the stream. */
  stream_depth?: number;
  /**
   * Cumulative messages processed, keyed by status code.
   * Common codes: `'200'` (success), `'590'` (child workflow),
   * `'591'` (activity dispatch), `'500'` (error).
   */
  counts?: Record<string, number>;
  /**
   * Consecutive stream consumption errors. `0` = healthy.
   * Non-zero means the consumer is in exponential backoff recovery.
   */
  error_count?: number;
  /** ISO timestamp of when this instance was initialized. */
  inited?: string;
  /** ISO timestamp of when this profile was generated. */
  timestamp?: string;
  /** Current throttle delay in ms (`0` = no throttle). */
  throttle?: number;
  /** Interval (ms) before reclaiming unacknowledged messages. */
  reclaimDelay?: number;
  /** Max messages to reclaim per cycle. */
  reclaimCount?: number;
  /** Host-level memory, CPU, and network stats. */
  system?: SystemHealth;
  /** Stringified worker callback function (only if `signature: true` in rollcall). */
  signature?: string;
}

interface QuorumMessageBase {
  entity?: string;
  guid?: string;
  topic?: string;
  type?: string;
}

// Messages extending QuorumMessageBase
export interface PingMessage extends QuorumMessageBase {
  type: 'ping';
  originator: string; //guid
  details?: boolean; //if true, all endpoints will include their profile
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
  guid: string; //call initiator
  originator: string; //clone of originator guid passed in ping
  entity?: string; //optional entity
  profile?: QuorumProfile; //contains details about the engine/worker
}

export interface ActivateMessage extends QuorumMessageBase {
  type: 'activate';
  cache_mode: 'nocache' | 'cache';
  until_version: string;
}

export interface UserMessage extends QuorumMessageBase {
  type: 'user';
  topic: string;
  message: StringAnyType;
}

export interface JobMessage extends QuorumMessageBase {
  type: 'job';
  entity?: string;
  topic: string; //this comes from the 'publishes' field in the YAML
  job: JobOutput;
  /** if true, job.data is null due to payload size - subscriber should fetch via getState */
  _ref?: boolean;
}

export interface ThrottleMessage extends QuorumMessageBase {
  type: 'throttle';
  guid?: string; //target engine AND workers with this guid
  entity?: string;
  topic?: string; //target worker(s) matching this topic (pass null to only target the engine, pass undefined to target engine and workers)
  throttle: number; //0-n; millis
}

export interface RollCallMessage extends QuorumMessageBase {
  type: 'rollcall';
  guid?: string; //target the engine quorum
  entity?: string;
  topic?: string | null; //target a worker if string; suppress if `null`;
  interval: number; //every 'n' seconds
  max?: number; //max broadcasts
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

export type RollCallOptions = {
  delay?: number;
  namespace?: string;
};

export type SubscriptionOptions = {
  namespace?: string;
};

/**
 * The types in this file are used to define those messages that are sent
 * to hotmesh client instances when a new version is about to be activated.
 * These messages serve to coordinate the cache invalidation and switch-over
 * to the new version without any downtime and a coordinating parent server.
 */
export type QuorumMessage =
  | PingMessage
  | PongMessage
  | ActivateMessage
  | WorkMessage
  | JobMessage
  | ThrottleMessage
  | RollCallMessage
  | CronMessage
  | UserMessage;
