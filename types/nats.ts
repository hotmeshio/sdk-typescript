import { 
  ConnectionOptions, 
  NatsConnection, 
  connect, 
  JetStreamClient, 
  JetStreamManager,
  StreamConfig as NatsStreamConfig,
  ConsumerConfig,
  JsMsg,
  PubAck,
  Msg,
  StreamInfo,
  NatsError
} from 'nats';
import { ProviderClient, ProviderTransaction } from './hotmesh';

export interface NatsStreamOptions extends ConnectionOptions {
  jetstream?: {
    domain?: string;
    prefix?: string;
    timeout?: number;
  };
}

export type NatsClientOptions = ConnectionOptions;
export type NatsClientType = NatsConnection & ProviderClient;
export type NatsClassType = typeof connect;
export type NatsJetStreamType = JetStreamClient;
export type NatsJetStreamManagerType = JetStreamManager;
export type NatsStreamConfigType = Partial<NatsStreamConfig>;
export type NatsConsumerConfigType = Partial<ConsumerConfig>;
export type NatsMessageType = JsMsg | Msg;
export type NatsPubAckType = PubAck & ProviderTransaction;
export type NatsStreamInfoType = StreamInfo;
export type NatsErrorType = NatsError;

export interface NatsTransaction {
  msgs: NatsMessageType[];
  pubAcks: NatsPubAckType[];
  execute(): Promise<void>;
  rollback(): Promise<void>;
}
