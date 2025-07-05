import { ILogger } from '../logger';
import {
  PublishMessageConfig,
  StreamConfig,
  StreamMessage,
  StreamStats,
} from '../../types/stream';
import { StringAnyType } from '../../types';
import { KeyStoreParams, KeyType } from '../../modules/key';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

export abstract class StreamService<
  ClientProvider extends ProviderClient,
  TransactionProvider extends ProviderTransaction,
> {
  protected streamClient: ClientProvider;
  protected storeClient: ProviderClient;
  protected namespace: string;
  protected logger: ILogger;
  protected appId: string;
  protected config: StreamConfig;

  constructor(
    streamClient: ClientProvider,
    storeClient: ProviderClient,
    config: StreamConfig = {},
  ) {
    this.streamClient = streamClient;
    this.storeClient = storeClient;
    this.config = config;
  }

  abstract init(
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<void>;

  abstract mintKey(type: KeyType, params: KeyStoreParams): string;

  // Core streaming operations
  abstract createStream(streamName: string): Promise<boolean>;
  abstract deleteStream(streamName: string): Promise<boolean>;

  // Consumer group operations
  abstract createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean>;
  abstract deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean>;

  // Message operations
  abstract publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): Promise<string[] | ProviderTransaction>;

  abstract consumeMessages(
    streamName: string,
    groupName: string,
    consumerName: string,
    options?: {
      batchSize?: number;
      blockTimeout?: number;
      autoAck?: boolean;
      enableBackoff?: boolean;     // enable backoff
      initialBackoff?: number;     // Initial backoff in ms
      maxBackoff?: number;         // Maximum backoff in ms
      maxRetries?: number;         // Maximum retries before giving up
      // New notification options
      enableNotifications?: boolean; // Override global setting
      notificationCallback?: (messages: StreamMessage[]) => void; // For event-driven consumption
    },
  ): Promise<StreamMessage[]>;

  abstract transact(): ProviderTransaction;

  // Message acknowledgment and deletion
  abstract ackAndDelete(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number>;

  abstract acknowledgeMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number | TransactionProvider>;

  abstract deleteMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number | TransactionProvider>;

  // **Generic Retry Method**
  abstract retryMessages(
    streamName: string,
    groupName: string,
    options?: {
      consumerName?: string;
      minIdleTime?: number;
      messageIds?: string[];
      delay?: number;
      maxRetries?: number;
      limit?: number;
    },
  ): Promise<StreamMessage[]>;

  // Monitoring and maintenance
  abstract getStreamStats(streamName: string): Promise<StreamStats>;

  abstract getStreamDepth(streamName: string): Promise<number>;
  abstract getStreamDepths(
    streamName: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]>;

  abstract trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number>;

  // Provider-specific helpers
  abstract getProviderSpecificFeatures(): {
    supportsBatching: boolean;
    supportsDeadLetterQueue: boolean;
    supportsOrdering: boolean;
    supportsTrimming: boolean;
    supportsRetry: boolean;
    supportsNotifications?: boolean; // New optional feature flag
    maxMessageSize: number;
    maxBatchSize: number;
  };

  // Optional notification management methods (implemented by providers that support them)
  stopNotificationConsumer?(streamName: string, groupName: string): Promise<void>;
  cleanup?(): Promise<void>;
}
