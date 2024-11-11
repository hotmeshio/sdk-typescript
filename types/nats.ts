import { ProviderClient, ProviderTransaction } from './provider';

// NOTE: Manually defines NATS types to avoid importing 'nats' package
// TODO: Generate file using 'nats' package/interface definitions

/** Connection Options for NATS */
export interface NatsConnectionOptions {
  servers?: string | string[];
  timeout?: number;
  name?: string;
  user?: string;
  pass?: string;
  token?: string;
  // Add other connection options as needed
}

/** Additional JetStream Options */
export interface NatsJetStreamOptions {
  domain?: string;
  prefix?: string;
  timeout?: number;
}

/** Combined Stream Options */
export interface NatsStreamOptions extends NatsConnectionOptions {
  jetstream?: NatsJetStreamOptions;
}

/** Type for NATS Client Options */
export type NatsClientOptions = NatsStreamOptions;

/** Interface representing a NATS Connection */
export interface NatsConnection extends ProviderClient {
  jetstream(options?: NatsJetStreamOptions): NatsJetStreamClient;
  jetstreamManager(options?: NatsJetStreamOptions): Promise<NatsJetStreamManager>;
  close(): Promise<void>;
}

/** Type representing the NATS Connection */
export type NatsClientType = NatsConnection;

/** Type representing the NATS Connection Function */
export type NatsClassType = (options: NatsClientOptions) => Promise<NatsConnection>;

/** Interface for JetStream Client */
export interface NatsJetStreamClient {
  publish(subject: string, data: Uint8Array, options?: NatsPublishOptions): Promise<NatsPubAck>;
  consumers: any; // Simplify as needed
  jetstreamManager(): Promise<NatsJetStreamManager>;
  // Additional methods as needed
}

/** **NatsJetStreamType** */
export type NatsJetStreamType = NatsJetStreamClient;

/** Interface for JetStream Manager */
export interface NatsJetStreamManager {
  streams: NatsStreamManager;
  consumers: NatsConsumerManager;
}

/** Interface for Stream Manager */
export interface NatsStreamManager {
  add(config: Partial<NatsStreamConfig>): Promise<void>;
  delete(stream: string): Promise<boolean>;
  info(stream: string): Promise<NatsStreamInfo>;
  list(): AsyncIterable<NatsStreamInfo>;
  deleteMessage(stream: string, seq: number): Promise<boolean>;
  update(stream: string, config: NatsStreamConfig ): Promise<void>;
}

/** Interface for Consumer Manager */
export interface NatsConsumerManager {
  add(stream: string, config: NatsConsumerConfig): Promise<void>;
  delete(stream: string, consumer: string): Promise<boolean>;
  info(stream: string, consumer: string): Promise<NatsConsumerInfo>;
  list(stream: string): AsyncIterable<NatsConsumerInfo>;
  // Additional methods as needed
}

/** Stream Configuration */
export interface NatsStreamConfig {
  name: string;
  subjects?: string[];
  retention?: NatsRetentionPolicy;
  storage?: NatsStorageType;
  num_replicas?: number;
  max_msgs?: number;
  max_age?: number; // In nanoseconds
  // Add other stream configurations as needed
}

/** Consumer Configuration */
export interface NatsConsumerConfig {
  durable_name?: string;
  deliver_subject?: string;
  deliver_group?: string;
  ack_policy?: NatsAckPolicy;
  ack_wait?: number; // In nanoseconds
  max_deliver?: number;
  // Add other consumer configurations as needed
}

/** Interface for JetStream Message */
export interface NatsJsMsg {
  seq: number;
  ack(): void;
  nak(): void;
  term(): void;
  inProgress(): void;
  working(): void;
  data: Uint8Array;
  subject: string;
  headers?: { [key: string]: string };
  info: NatsDeliveryInfo;
  // Additional properties and methods as needed
}

/** Delivery Info for a Message */
export interface NatsDeliveryInfo {
  stream: string;
  consumer: string;
  redeliveryCount: number;
  timestampNanos: number;
  pending: number;
  // Additional properties as needed
}

/** Interface for Publish Acknowledgment */
export interface NatsPubAck extends ProviderTransaction {
  stream: string;
  seq: number;
  duplicate: boolean;
}

/** Interface for Stream Information */
export interface NatsStreamInfo {
  config: NatsStreamConfig;
  state: NatsStreamState;
}

/** Stream State Information */
export interface NatsStreamState {
  messages: number;
  bytes: number;
  first_seq: number;
  last_seq: number;
  // Additional state properties as needed
}

/** Interface for Consumer Information */
export interface NatsConsumerInfo {
  name: string;
  stream_name: string;
  config: NatsConsumerConfig;
  delivered: NatsSequenceInfo;
  ack_floor: NatsSequenceInfo;
  num_ack_pending: number;
  num_redelivered: number;
  num_waiting: number;
  num_pending: number;
  // Additional properties as needed
}

/** Sequence Information */
export interface NatsSequenceInfo {
  consumer_seq: number;
  stream_seq: number;
}

/** Publish Options */
export interface NatsPublishOptions {
  msgID?: string;
  expect?: NatsMsgExpect;
  headers?: { [key: string]: string };
  // Additional options as needed
}

/** Message Expectation */
export interface NatsMsgExpect {
  lastMsgID?: string;
  streamName?: string;
  lastSequence?: number;
  // Additional expectations as needed
}

/** NATS Error */
export interface NatsError extends Error {
  code?: string;
  // Additional properties as needed
}

/** Retention Policies */
export type NatsRetentionPolicy = 'limits' | 'interest' | 'workqueue';

/** Storage Types */
export type NatsStorageType = 'file' | 'memory';

/** Acknowledgment Policies */
export type NatsAckPolicy = 'none' | 'all' | 'explicit';

/** Types for Specific Policies */
export type NatsRetentionPolicyWorkqueueType = 'workqueue';
export type NatsStorageMemoryType = 'memory';
export type NatsAckPolicyExplicitType = 'explicit';

/** NATS Message Type */
export type NatsMessageType = NatsJsMsg;

/** NATS Transaction Interface */
export interface NatsTransaction {
  msgs: NatsMessageType[];
  pubAcks: NatsPubAck[];
  execute(): Promise<void>;
  rollback(): Promise<void>;
}

/** NATS Client Error Type */
export type NatsErrorType = NatsError;

/** NATS Stream Info Type */
export type NatsStreamInfoType = NatsStreamInfo;

/** NATS Stream Config Type */
export type NatsStreamConfigType = Partial<NatsStreamConfig>;

/** NATS Consumer Config Type */
export type NatsConsumerConfigType = Partial<NatsConsumerConfig>;

/** NATS Publish Ack Type */
export type NatsPubAckType = NatsPubAck;
