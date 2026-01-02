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
 * PostgreSQL Stream Service
 * 
 * High-performance stream message provider using PostgreSQL with LISTEN/NOTIFY.
 * 
 * ## Module Organization
 * 
 * This service is organized into focused modules following KISS principles:
 * - `postgres.ts` (this file) - Main orchestrator and service interface
 * - `kvtables.ts` - Schema deployment and table management
 * - `messages.ts` - Message CRUD operations (publish, fetch, ack, delete)
 * - `stats.ts` - Statistics and query operations
 * - `scout.ts` - Scout role coordination for polling visible messages
 * - `notifications.ts` - LISTEN/NOTIFY notification system with static state management
 * - `lifecycle.ts` - Stream and consumer group lifecycle operations
 * 
 * ## Lifecycle
 * 
 * ### Initialization (`init`)
 * 1. Deploy PostgreSQL schema (tables, indexes, triggers, functions)
 * 2. Create ScoutManager for coordinating visibility timeout polling
 * 3. Create NotificationManager for LISTEN/NOTIFY event handling
 * 4. Set up notification handler (once per client, shared across instances)
 * 5. Start fallback poller (backup for missed notifications)
 * 6. Start router scout poller (for visibility timeout processing)
 * 
 * ### Shutdown (`cleanup`)
 * 1. Stop router scout polling loop
 * 2. Release scout role if held
 * 3. Stop notification consumers for this instance
 * 4. UNLISTEN from channels when last instance disconnects
 * 5. Clean up fallback poller when last instance disconnects
 * 6. Remove notification handlers when last instance disconnects
 * 
 * ## Notification System (LISTEN/NOTIFY)
 * 
 * ### Real-time Message Delivery
 * - PostgreSQL trigger on INSERT sends NOTIFY when messages are immediately visible
 * - Messages with visibility timeout are NOT notified on INSERT
 * - Multiple service instances share the same client and notification handlers
 * - Static state ensures only ONE LISTEN per channel across all instances
 * 
 * ### Components
 * - **Notification Handler**: Listens for PostgreSQL NOTIFY events
 * - **Fallback Poller**: Polls every 30s (default) for missed messages
 * - **Router Scout**: Active role-holder polls visible messages frequently (~100ms)
 * - **Visibility Function**: `notify_visible_messages()` checks for expired timeouts
 * 
 * ## Scout Role (Visibility Timeout Processing)
 * 
 * When messages are published with visibility timeouts (delays), they need to be
 * processed when they become visible. The scout role ensures this happens efficiently:
 * 
 * 1. **Role Acquisition**: One instance per app acquires "router" scout role
 * 2. **Fast Polling**: Scout polls `notify_visible_messages()` every ~100ms
 * 3. **Notification**: Function triggers NOTIFY for streams with visible messages
 * 4. **Role Rotation**: Role expires after interval, another instance can claim it
 * 5. **Fallback**: Non-scouts sleep longer, try to acquire role periodically
 * 
 * ## Message Flow
 * 
 * ### Publishing
 * 1. Messages inserted into partitioned table
 * 2. If immediately visible → INSERT trigger sends NOTIFY
 * 3. If visibility timeout → no NOTIFY (scout will handle when visible)
 * 
 * ### Consuming (Event-Driven)
 * 1. Consumer calls `consumeMessages` with notification callback
 * 2. Service executes LISTEN on channel `stream_{name}_{group}`
 * 3. On NOTIFY → fetch messages → invoke callback
 * 4. Initial fetch done immediately (catch any queued messages)
 * 
 * ### Consuming (Polling)
 * 1. Consumer calls `consumeMessages` without callback
 * 2. Service directly queries and reserves messages
 * 3. Returns messages synchronously
 * 
 * ## Reliability Guarantees
 * 
 * - **Notification Fallback**: Poller catches missed notifications every 30s
 * - **Visibility Scout**: Ensures delayed messages are processed when visible
 * - **Graceful Degradation**: Falls back to polling if LISTEN fails
 * - **Shared State**: Multiple instances coordinate via static maps
 * - **Race Condition Safe**: SKIP LOCKED prevents message duplication
 * 
 * @example
 * ```typescript
 * // Initialize service
 * const service = new PostgresStreamService(client, storeClient, config);
 * await service.init('namespace', 'appId', logger);
 * 
 * // Event-driven consumption (recommended)
 * await service.consumeMessages('stream', 'group', 'consumer', {
 *   notificationCallback: (messages) => {
 *     // Process messages in real-time
 *   }
 * });
 * 
 * // Polling consumption
 * const messages = await service.consumeMessages('stream', 'group', 'consumer', {
 *   batchSize: 10
 * });
 * 
 * // Cleanup on shutdown
 * await service.cleanup();
 * ```
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
      this.getTableName.bind(this),
      this.mintKey.bind(this),
      this.logger,
    );

    // Initialize notification manager
    this.notificationManager = new NotificationManager(
      this.streamClient,
      this.getTableName.bind(this),
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

  getTableName(): string {
    return `${this.safeName(this.appId)}.streams`;
  }

  safeName(appId: string): string {
    return appId.replace(/[^a-zA-Z0-9_]/g, '_');
  }

  async createStream(streamName: string): Promise<boolean> {
    return Lifecycle.createStream(streamName);
  }

  async deleteStream(streamName: string): Promise<boolean> {
    return Lifecycle.deleteStream(
      this.streamClient,
      this.getTableName(),
      streamName,
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
    return Lifecycle.deleteConsumerGroup(
      this.streamClient,
      this.getTableName(),
      streamName,
      groupName,
      this.logger,
    );
  }

  /**
   * `publishMessages` can be roped into a transaction by the `store`
   * service. If so, it will add the SQL and params to the
   * transaction. [Process Overview]: The engine keeps a reference
   * to the `store` and `stream` providers; it asks the `store` to
   * create a transaction and then starts adding store commands to the
   * transaction. The engine then calls the router to publish a
   * message using the `stream` provider (which the router keeps
   * a reference to), and provides the transaction object.
   * The `stream` provider then calls this method to generate
   * the SQL and params for the transaction (but, of course, the sql
   * is not executed until the engine calls the `exec` method on
   * the transaction object provided by `store`).
   *
   * NOTE: this strategy keeps `stream` and `store` operations separate but
   * allows calls to the stream to be roped into a single SQL transaction.
   */
  async publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): Promise<string[] | ProviderTransaction> {
    return Messages.publishMessages(
      this.streamClient,
      this.getTableName(),
      streamName,
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
    return Messages.buildPublishSQL(
      this.getTableName(),
      streamName,
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
      reservationTimeout?: number; // in seconds
      enableBackoff?: boolean; // enable backoff
      initialBackoff?: number; // Initial backoff in ms
      maxBackoff?: number; // Maximum backoff in ms
      maxRetries?: number; // Maximum retries before giving up
      // New notification options
      enableNotifications?: boolean; // Override global setting
      notificationCallback?: (messages: StreamMessage[]) => void; // For event-driven consumption
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

    // If option is explicitly set, use that; otherwise use global setting
    const enabled = optionEnabled !== undefined ? optionEnabled : globalEnabled;

    // Also check if client supports notifications
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

          // If we got messages, call the callback
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

      // Return empty array immediately to avoid blocking
      return [];
    } catch (error) {
      // Fall back to polling if setup fails
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
      reservationTimeout?: number; // in seconds
      enableBackoff?: boolean; // enable backoff
      initialBackoff?: number; // Initial backoff in ms
      maxBackoff?: number; // Maximum backoff in ms
      maxRetries?: number; // Maximum retries before giving up
    },
  ): Promise<StreamMessage[]> {
    return Messages.fetchMessages(
      this.streamClient,
      this.getTableName(),
      streamName,
      groupName,
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
    return Messages.ackAndDelete(
      this.streamClient,
      this.getTableName(),
      streamName,
      groupName,
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
    return Messages.deleteMessages(
      this.streamClient,
      this.getTableName(),
      streamName,
      groupName,
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
    return Stats.getStreamStats(
      this.streamClient,
      this.getTableName(),
      streamName,
      this.logger,
    );
  }

  async getStreamDepth(streamName: string): Promise<number> {
    return Stats.getStreamDepth(
      this.streamClient,
      this.getTableName(),
      streamName,
      this.logger,
    );
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    return Stats.getStreamDepths(
      this.streamClient,
      this.getTableName(),
      streamNames,
      this.logger,
    );
  }

  async trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number> {
    return Stats.trimStream(
      this.streamClient,
      this.getTableName(),
      streamName,
      options,
      this.logger,
    );
  }

  getProviderSpecificFeatures() {
    return Stats.getProviderSpecificFeatures(this.config);
  }

  // Cleanup method to be called when shutting down
  async cleanup(): Promise<void> {
    // Stop router scout polling loop
    if (this.scoutManager) {
      await this.scoutManager.stopRouterScoutPoller();
    }

    // Clean up notification consumers
    if (this.notificationManager) {
      await this.notificationManager.cleanup(this);
    }
  }
}

export { PostgresStreamService };
