import { JobOutput } from "./job";

//used for coordination like version activation
export interface PingMessage {
  type: 'ping';
  originator: string; //guid
}

export interface WorkMessage {
  type: 'work';
  originator: string; //guid
}

export interface CronMessage {
  type: 'cron';
  originator: string; //guid
}

//used for coordination like version activation
export interface PongMessage {
  type: 'pong';
  originator: string; //clone of originator guid passed in ping
  guid: string;
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
