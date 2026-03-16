import { ILogger } from '../../../logger';
import { KeyService, KeyType } from '../../../../modules/key';
import { StreamService } from '../../index';
import { KeyStoreParams, StringAnyType } from '../../../../types';
import { PostgresClientType } from '../../../../types/postgres';
import {
  NotificationConsumer,
  PublishMessageConfig,
  StreamConfig,
  StreamMessage,
  StreamStats,
} from '../../../../types/stream';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';

import { deploySchema } from './kvtables';
import * as Stats from './stats';
import * as Messages from './messages';
import { ScoutManager } from './scout';
import { NotificationManager, getFallbackInterval } from './notifications';
import * as Lifecycle from './lifecycle';

/**
 * Resolved stream target containing the table name and simplified stream name.
 */
export interface StreamTarget {
  tableName: string;
  streamName: string;
  isEngine: boolean;
}

/**
 * PostgreSQL Stream Service
 *
 * Uses separate `engine_streams` and `worker_streams` tables for
 * security isolation and independent scaling. The `worker_streams`
 * table includes a `workflow_name` column for routing.
 */
class PostgresStreamService extends StreamService<
  PostgresClientType & ProviderClient,
  any
> {
  namespace: string;
  appId: string;
  logger: ILogger;

  // Scout manager
  private scoutManager: ScoutManager;

  // Notification manager
  private notificationManager: NotificationManager<PostgresStreamService>;

  constructor(
    streamClient: PostgresClientType & ProviderClient,
    storeClient: ProviderClient,
    config: StreamConfig = {},
  ) {
    super(streamClient, storeClient, config);
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.appId = appId;
    this.logger = logger;
    await deploySchema(this.streamClient, this.appId, this.logger);

    // Initialize scout manager
    this.scoutManager = new ScoutManager(
      this.streamClient,
      this.appId,
      this.getEngineTableName.bind(this),
      this.mintKey.bind(this),
      this.logger,
    );

    // Initialize notification manager
    this.notificationManager = new NotificationManager(
      this.streamClient,
      this.getEngineTableName.bind(this),
      () => getFallbackInterval(this.config),
      this.logger,
    );

    // Set up notification handler if supported
    if (this.streamClient.on && this.isNotificationsEnabled()) {
      this.notificationManager.setupClientNotificationHandler(this);
      this.notificationManager.startClientFallbackPoller(
        this.checkForMissedMessages.bind(this),
      );
      this.scoutManager.startRouterScoutPoller();
    }
  }

  private isNotificationsEnabled(): boolean {
    return Stats.isNotificationsEnabled(this.config);
  }

  private async checkForMissedMessages(): Promise<void> {
    await this.notificationManager.checkForMissedMessages(
      async (instance: PostgresStreamService, consumer: NotificationConsumer) => {
        return await instance.fetchMessages(
          consumer.streamName,
          consumer.groupName,
          consumer.consumerName,
          { batchSize: 10, enableBackoff: false, maxRetries: 1 },
        );
      },
    );
  }

  private async fetchAndDeliverMessages(
    consumer: NotificationConsumer,
  ): Promise<void> {
    try {
      const messages = await this.fetchMessages(
        consumer.streamName,
        consumer.groupName,
        consumer.consumerName,
        { batchSize: 10, enableBackoff: false, maxRetries: 1 },
      );

      if (messages.length > 0) {
        consumer.callback(messages);
      }
    } catch (error) {
      this.logger.error('postgres-stream-fetch-deliver-error', {
        streamName: consumer.streamName,
        groupName: consumer.groupName,
        error,
      });
    }
  }

  private getConsumerKey(streamName: string, groupName: string): string {
    return `${streamName}:${groupName}`;
  }

  /**
   * Resolves a conjoined stream key (e.g., `hmsh:appId:x:topic`) into
   * the correct table name and simplified stream name.
   */
  resolveStreamTarget(streamKey: string): StreamTarget {
    const isEngine = streamKey.endsWith(':');
    if (isEngine) {
      return {
        tableName: this.getEngineTableName(),
        streamName: this.appId,
        isEngine: true,
      };
    }
    // Extract the bare topic from hmsh:appId:x:topicName
    const parts = streamKey.split(':');
    const topic = parts[parts.length - 1];
    return {
      tableName: this.getWorkerTableName(),
      streamName: topic,
      isEngine: false,
    };
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, {
      ...params,
      appId: this.appId,
    });
  }

  transact(): ProviderTransaction {
    return {} as ProviderTransaction;
  }

  getEngineTableName(): string {
    return `${this.safeName(this.appId)}.engine_streams`;
  }

  getWorkerTableName(): string {
    return `${this.safeName(this.appId)}.worker_streams`;
  }

  safeName(appId: string): string {
    return appId.replace(/[^a-zA-Z0-9_]/g, '_');
  }

  async createStream(streamName: string): Promise<boolean> {
    return Lifecycle.createStream(streamName);
  }

  async deleteStream(streamName: string): Promise<boolean> {
    if (streamName === '*') {
      // Delete from both tables
      await Lifecycle.deleteStream(
        this.streamClient,
        this.getEngineTableName(),
        '*',
        this.logger,
      );
      return Lifecycle.deleteStream(
        this.streamClient,
        this.getWorkerTableName(),
        '*',
        this.logger,
      );
    }
    const target = this.resolveStreamTarget(streamName);
    return Lifecycle.deleteStream(
      this.streamClient,
      target.tableName,
      target.streamName,
      this.logger,
    );
  }

  async createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    return Lifecycle.createConsumerGroup(streamName, groupName);
  }

  async deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    const target = this.resolveStreamTarget(streamName);
    return Lifecycle.deleteConsumerGroup(
      this.streamClient,
      target.tableName,
      target.streamName,
      this.logger,
    );
  }

  /**
   * `publishMessages` can be roped into a transaction by the `store`
   * service. The `stream` provider generates SQL and params that are
   * added to the transaction for atomic execution.
   */
  async publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): Promise<string[] | ProviderTransaction> {
    const target = this.resolveStreamTarget(streamName);
    return Messages.publishMessages(
      this.streamClient,
      target.tableName,
      target.streamName,
      target.isEngine,
      messages,
      options,
      this.logger,
    );
  }

  _publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): { sql: string; params: any[] } {
    const target = this.resolveStreamTarget(streamName);
    return Messages.buildPublishSQL(
      target.tableName,
      target.streamName,
      target.isEngine,
      messages,
      options,
    );
  }

  async consumeMessages(
    streamName: string,
    groupName: string,
    consumerName: string,
    options?: {
      batchSize?: number;
      blockTimeout?: number;
      autoAck?: boolean;
      reservationTimeout?: number;
      enableBackoff?: boolean;
      initialBackoff?: number;
      maxBackoff?: number;
      maxRetries?: number;
      enableNotifications?: boolean;
      notificationCallback?: (messages: StreamMessage[]) => void;
    },
  ): Promise<StreamMessage[]> {
    // If notification callback is provided and notifications are enabled, set up listener
    if (options?.notificationCallback && this.shouldUseNotifications(options)) {
      return this.setupNotificationConsumer(
        streamName,
        groupName,
        consumerName,
        options.notificationCallback,
        options,
      );
    }

    // Otherwise, use traditional polling approach
    return this.fetchMessages(streamName, groupName, consumerName, options);
  }

  private shouldUseNotifications(options?: {
    enableNotifications?: boolean;
  }): boolean {
    const globalEnabled = this.isNotificationsEnabled();
    const optionEnabled = options?.enableNotifications;
    const enabled = optionEnabled !== undefined ? optionEnabled : globalEnabled;
    return enabled && this.streamClient.on !== undefined;
  }

  private async setupNotificationConsumer(
    streamName: string,
    groupName: string,
    consumerName: string,
    callback: (messages: StreamMessage[]) => void,
    options?: any,
  ): Promise<StreamMessage[]> {
    try {
      await this.notificationManager.setupNotificationConsumer(
        this,
        streamName,
        groupName,
        consumerName,
        callback,
      );

      // Do an initial fetch asynchronously to avoid blocking setup
      setImmediate(async () => {
        try {
          const fetchStart = Date.now();
          const initialMessages = await this.fetchMessages(
            streamName,
            groupName,
            consumerName,
            {
              ...options,
              enableBackoff: false,
              maxRetries: 1,
            },
          );

          this.logger.debug('postgres-stream-initial-fetch-complete', {
            streamName,
            groupName,
            messageCount: initialMessages.length,
            fetchDuration: Date.now() - fetchStart,
          });

          if (initialMessages.length > 0) {
            callback(initialMessages);
          }
        } catch (error) {
          this.logger.error('postgres-stream-initial-fetch-error', {
            streamName,
            groupName,
            error,
          });
        }
      });

      return [];
    } catch (error) {
      return this.fetchMessages(streamName, groupName, consumerName, options);
    }
  }

  async stopNotificationConsumer(
    streamName: string,
    groupName: string,
  ): Promise<void> {
    await this.notificationManager.stopNotificationConsumer(
      this,
      streamName,
      groupName,
    );
  }

  private async fetchMessages(
    streamName: string,
    groupName: string,
    consumerName: string,
    options?: {
      batchSize?: number;
      blockTimeout?: number;
      autoAck?: boolean;
      reservationTimeout?: number;
      enableBackoff?: boolean;
      initialBackoff?: number;
      maxBackoff?: number;
      maxRetries?: number;
    },
  ): Promise<StreamMessage[]> {
    const target = this.resolveStreamTarget(streamName);
    return Messages.fetchMessages(
      this.streamClient,
      target.tableName,
      target.streamName,
      target.isEngine,
      consumerName,
      options || {},
      this.logger,
    );
  }

  async ackAndDelete(
    streamName: string,
    groupName: string,
    messageIds: string[],
  ): Promise<number> {
    const target = this.resolveStreamTarget(streamName);
    return Messages.ackAndDelete(
      this.streamClient,
      target.tableName,
      target.streamName,
      messageIds,
      this.logger,
    );
  }

  async acknowledgeMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    return Messages.acknowledgeMessages(messageIds);
  }

  async deleteMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    const target = this.resolveStreamTarget(streamName);
    return Messages.deleteMessages(
      this.streamClient,
      target.tableName,
      target.streamName,
      messageIds,
      this.logger,
    );
  }

  async retryMessages(
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
  ): Promise<StreamMessage[]> {
    return Messages.retryMessages(streamName, groupName, options);
  }

  async getStreamStats(streamName: string): Promise<StreamStats> {
    const target = this.resolveStreamTarget(streamName);
    return Stats.getStreamStats(
      this.streamClient,
      target.tableName,
      target.streamName,
      this.logger,
    );
  }

  async getStreamDepth(streamName: string): Promise<number> {
    const target = this.resolveStreamTarget(streamName);
    return Stats.getStreamDepth(
      this.streamClient,
      target.tableName,
      target.streamName,
      this.logger,
    );
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    // Partition stream names by table type and query each table separately
    const engineStreams: { stream: string }[] = [];
    const workerStreams: { stream: string }[] = [];
    const streamNameMap = new Map<string, string>(); // resolvedName -> originalKey

    for (const s of streamNames) {
      const target = this.resolveStreamTarget(s.stream);
      streamNameMap.set(target.streamName, s.stream);
      if (target.isEngine) {
        engineStreams.push({ stream: target.streamName });
      } else {
        workerStreams.push({ stream: target.streamName });
      }
    }

    const results: { stream: string; depth: number }[] = [];

    if (engineStreams.length > 0) {
      const engineResults = await Stats.getStreamDepths(
        this.streamClient,
        this.getEngineTableName(),
        engineStreams,
        this.logger,
      );
      results.push(...engineResults);
    }

    if (workerStreams.length > 0) {
      const workerResults = await Stats.getStreamDepths(
        this.streamClient,
        this.getWorkerTableName(),
        workerStreams,
        this.logger,
      );
      results.push(...workerResults);
    }

    return results;
  }

  async trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number> {
    const target = this.resolveStreamTarget(streamName);
    return Stats.trimStream(
      this.streamClient,
      target.tableName,
      target.streamName,
      options,
      this.logger,
    );
  }

  getProviderSpecificFeatures() {
    return Stats.getProviderSpecificFeatures(this.config);
  }

  async cleanup(): Promise<void> {
    if (this.scoutManager) {
      await this.scoutManager.stopRouterScoutPoller();
    }

    if (this.notificationManager) {
      await this.notificationManager.cleanup(this);
    }
  }
}

export { PostgresStreamService };
