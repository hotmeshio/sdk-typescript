import { ILogger } from '../../../logger';
import { KeyService, KeyType } from '../../../../modules/key';
import { parseStreamMessage, sleepFor } from '../../../../modules/utils';
import { StreamService } from '../../index';
import { KeyStoreParams, StringAnyType } from '../../../../types';
import {
  PostgresClientType,
  PostgresNotification,
} from '../../../../types/postgres';
import {
  PublishMessageConfig,
  StreamConfig,
  StreamMessage,
  StreamStats,
} from '../../../../types/stream';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';

import { deploySchema, getNotificationChannelName } from './kvtables';

interface NotificationConsumer {
  streamName: string;
  groupName: string;
  consumerName: string;
  callback: (messages: StreamMessage[]) => void;
  isListening: boolean;
  lastFallbackCheck: number;
}

class PostgresStreamService extends StreamService<
  PostgresClientType & ProviderClient,
  any
> {
  namespace: string;
  appId: string;
  logger: ILogger;

  // Static maps to manage notifications across all instances sharing the same client
  private static clientNotificationConsumers: Map<
    PostgresClientType & ProviderClient,
    Map<string, Map<PostgresStreamService, NotificationConsumer>>
  > = new Map();
  private static clientNotificationHandlers: Map<
    PostgresClientType & ProviderClient,
    boolean
  > = new Map();
  private static clientFallbackPollers: Map<
    PostgresClientType & ProviderClient,
    NodeJS.Timeout
  > = new Map();

  // Instance-level tracking for cleanup
  private instanceNotificationConsumers: Set<string> = new Set();
  private notificationHandlerBound: (
    notification: PostgresNotification,
  ) => void;

  constructor(
    streamClient: PostgresClientType & ProviderClient,
    storeClient: ProviderClient,
    config: StreamConfig = {},
  ) {
    super(streamClient, storeClient, config);
    this.notificationHandlerBound = this.handleNotification.bind(this);
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.appId = appId;
    this.logger = logger;
    await deploySchema(this.streamClient, this.appId, this.logger);

    // Set up notification handler if supported
    if (this.streamClient.on && this.isNotificationsEnabled()) {
      this.setupClientNotificationHandler();
      this.startClientFallbackPoller();
    }
  }

  private setupClientNotificationHandler(): void {
    // Check if notification handler is already set up for this client
    if (
      PostgresStreamService.clientNotificationHandlers.get(this.streamClient)
    ) {
      return;
    }

    // Initialize notification consumer map for this client if it doesn't exist
    if (
      !PostgresStreamService.clientNotificationConsumers.has(this.streamClient)
    ) {
      PostgresStreamService.clientNotificationConsumers.set(
        this.streamClient,
        new Map(),
      );
    }

    // Set up the notification handler for this client
    this.streamClient.on('notification', this.handleNotification.bind(this));

    // Mark this client as having a notification handler
    PostgresStreamService.clientNotificationHandlers.set(
      this.streamClient,
      true,
    );
  }

  private startClientFallbackPoller(): void {
    // Check if fallback poller already exists for this client
    if (PostgresStreamService.clientFallbackPollers.has(this.streamClient)) {
      return;
    }

    const fallbackIntervalId = setInterval(() => {
      this.checkForMissedMessages();
    }, this.getFallbackInterval());

    PostgresStreamService.clientFallbackPollers.set(
      this.streamClient,
      fallbackIntervalId,
    );
  }

  private isNotificationsEnabled(): boolean {
    return this.config?.postgres?.enableNotifications !== false; // Default: true
  }

  private getFallbackInterval(): number {
    return this.config?.postgres?.notificationFallbackInterval || 30000; // Default: 30 seconds
  }

  private getNotificationTimeout(): number {
    return this.config?.postgres?.notificationTimeout || 5000; // Default: 5 seconds
  }

  private async checkForMissedMessages(): Promise<void> {
    const now = Date.now();
    const clientNotificationConsumers =
      PostgresStreamService.clientNotificationConsumers.get(this.streamClient);

    if (!clientNotificationConsumers) {
      return;
    }

    for (const [
      consumerKey,
      instanceMap,
    ] of clientNotificationConsumers.entries()) {
      for (const [instance, consumer] of instanceMap.entries()) {
        if (
          consumer.isListening &&
          now - consumer.lastFallbackCheck > this.getFallbackInterval()
        ) {
          try {
            const messages = await instance.fetchMessages(
              consumer.streamName,
              consumer.groupName,
              consumer.consumerName,
              { batchSize: 10, enableBackoff: false, maxRetries: 1 },
            );

            if (messages.length > 0) {
              instance.logger.debug('postgres-stream-fallback-messages', {
                streamName: consumer.streamName,
                groupName: consumer.groupName,
                messageCount: messages.length,
              });
              consumer.callback(messages);
            }

            consumer.lastFallbackCheck = now;
          } catch (error) {
            instance.logger.error('postgres-stream-fallback-error', {
              streamName: consumer.streamName,
              groupName: consumer.groupName,
              error,
            });
          }
        }
      }
    }
  }

  private handleNotification(notification: PostgresNotification): void {
    try {
      // Only handle stream notifications (channels starting with "stream_")
      // Ignore pub/sub notifications from sub provider which use different channel names
      if (!notification.channel.startsWith('stream_')) {
        // This is likely a pub/sub notification from the sub provider, ignore it
        this.logger.debug('postgres-stream-ignoring-sub-notification', {
          channel: notification.channel,
          payloadPreview: notification.payload.substring(0, 100),
        });
        return;
      }

      this.logger.debug('postgres-stream-processing-notification', {
        channel: notification.channel,
      });

      const payload = JSON.parse(notification.payload);
      const { stream_name, group_name } = payload;

      if (!stream_name || !group_name) {
        this.logger.warn('postgres-stream-invalid-notification', {
          notification,
        });
        return;
      }

      const consumerKey = this.getConsumerKey(stream_name, group_name);
      const clientNotificationConsumers =
        PostgresStreamService.clientNotificationConsumers.get(
          this.streamClient,
        );

      if (!clientNotificationConsumers) {
        return;
      }

      const instanceMap = clientNotificationConsumers.get(consumerKey);
      if (!instanceMap) {
        return;
      }

      // Trigger immediate message fetch for all instances with this consumer
      for (const [instance, consumer] of instanceMap.entries()) {
        if (consumer.isListening) {
          instance.fetchAndDeliverMessages(consumer);
        }
      }
    } catch (error) {
      this.logger.error('postgres-stream-notification-parse-error', {
        notification,
        error,
      });
    }
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
    return true;
  }

  async deleteStream(streamName: string): Promise<boolean> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      if (streamName === '*') {
        await client.query(`DELETE FROM ${tableName}`);
      } else {
        await client.query(`DELETE FROM ${tableName} WHERE stream_name = $1`, [
          streamName,
        ]);
      }
      return true;
    } catch (error) {
      this.logger.error(`postgres-stream-delete-error-${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    return true;
  }

  async deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      await client.query(
        `DELETE FROM ${tableName} WHERE stream_name = $1 AND group_name = $2`,
        [streamName, groupName],
      );
      return true;
    } catch (error) {
      this.logger.error(
        `postgres-stream-delete-group-error-${streamName}.${groupName}`,
        { error },
      );
      throw error;
    }
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
    const { sql, params } = this._publishMessages(streamName, messages);
    if (
      options?.transaction &&
      typeof options.transaction.addCommand === 'function'
    ) {
      //call addCommand and return the transaction object
      options.transaction.addCommand(
        sql,
        params,
        'array',
        (rows: { id: number }[]) => rows.map((row) => row.id.toString()),
      );
      return options.transaction as ProviderTransaction;
    } else {
      try {
        const ids: string[] = [];
        const res = await this.streamClient.query(sql, params);
        for (const row of res.rows) {
          ids.push(row.id.toString());
        }
        return ids;
      } catch (error) {
        this.logger.error(`postgres-stream-publish-error-${streamName}`, {
          error,
        });
        throw error;
      }
    }
  }

  _publishMessages(
    streamName: string,
    messages: string[],
  ): { sql: string; params: any[] } {
    const tableName = this.getTableName();
    const groupName = streamName.endsWith(':') ? 'ENGINE' : 'WORKER';
    const insertValues = messages
      .map((_, idx) => `($1, $2, $${idx + 3})`)
      .join(', ');

    return {
      sql: `INSERT INTO ${tableName} (stream_name, group_name, message) VALUES ${insertValues} RETURNING id`,
      params: [streamName, groupName, ...messages],
    };
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
    const startTime = Date.now();
    const consumerKey = this.getConsumerKey(streamName, groupName);
    const channelName = getNotificationChannelName(streamName, groupName);

    // Get or create notification consumer map for this client
    let clientNotificationConsumers =
      PostgresStreamService.clientNotificationConsumers.get(this.streamClient);
    if (!clientNotificationConsumers) {
      clientNotificationConsumers = new Map();
      PostgresStreamService.clientNotificationConsumers.set(
        this.streamClient,
        clientNotificationConsumers,
      );
    }

    // Get or create instance map for this consumer key
    let instanceMap = clientNotificationConsumers.get(consumerKey);
    if (!instanceMap) {
      instanceMap = new Map();
      clientNotificationConsumers.set(consumerKey, instanceMap);

      // Set up LISTEN for this channel (only once per channel across all instances)
      try {
        const listenStart = Date.now();
        await this.streamClient.query(`LISTEN "${channelName}"`);
        this.logger.debug('postgres-stream-listen-start', {
          streamName,
          groupName,
          channelName,
          listenDuration: Date.now() - listenStart,
        });
      } catch (error) {
        this.logger.error('postgres-stream-listen-error', {
          streamName,
          groupName,
          channelName,
          error,
        });
        // Fall back to polling if LISTEN fails
        return this.fetchMessages(streamName, groupName, consumerName, options);
      }
    }

    // Register or update consumer for this instance
    const consumer: NotificationConsumer = {
      streamName,
      groupName,
      consumerName,
      callback,
      isListening: true,
      lastFallbackCheck: Date.now(),
    };

    instanceMap.set(this, consumer);

    // Track this consumer for cleanup
    this.instanceNotificationConsumers.add(consumerKey);

    this.logger.debug('postgres-stream-notification-setup-complete', {
      streamName,
      groupName,
      instanceCount: instanceMap.size,
      setupDuration: Date.now() - startTime,
    });

    // Do an initial fetch asynchronously to avoid blocking setup
    // This ensures we don't miss any messages that were already in the queue
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
  }

  async stopNotificationConsumer(
    streamName: string,
    groupName: string,
  ): Promise<void> {
    const consumerKey = this.getConsumerKey(streamName, groupName);
    const clientNotificationConsumers =
      PostgresStreamService.clientNotificationConsumers.get(this.streamClient);

    if (!clientNotificationConsumers) {
      return;
    }

    const instanceMap = clientNotificationConsumers.get(consumerKey);
    if (!instanceMap) {
      return;
    }

    const consumer = instanceMap.get(this);
    if (consumer) {
      consumer.isListening = false;
      instanceMap.delete(this);

      // Remove from instance tracking
      this.instanceNotificationConsumers.delete(consumerKey);

      // If no more instances for this consumer key, stop listening and clean up
      if (instanceMap.size === 0) {
        clientNotificationConsumers.delete(consumerKey);
        const channelName = getNotificationChannelName(streamName, groupName);
        try {
          await this.streamClient.query(`UNLISTEN "${channelName}"`);
          this.logger.debug('postgres-stream-unlisten', {
            streamName,
            groupName,
            channelName,
          });
        } catch (error) {
          this.logger.error('postgres-stream-unlisten-error', {
            streamName,
            groupName,
            channelName,
            error,
          });
        }
      }
    }
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
    const client = this.streamClient;
    const tableName = this.getTableName();
    const enableBackoff = options?.enableBackoff ?? false;
    const initialBackoff = options?.initialBackoff ?? 100; // Default initial backoff: 100ms
    const maxBackoff = options?.maxBackoff ?? 3000; // Default max backoff: 3 seconds
    const maxRetries = options?.maxRetries ?? 3; // Set a finite default, e.g., 3 retries

    let backoff = initialBackoff;
    let retries = 0;

    try {
      while (retries < maxRetries) {
        retries++;
        const batchSize = options?.batchSize || 1;
        const reservationTimeout = options?.reservationTimeout || 30;

        // Simplified query for better performance - especially for notification-triggered fetches
        const res = await client.query(
          `UPDATE ${tableName} 
           SET reserved_at = NOW(), reserved_by = $4
           WHERE id IN (
             SELECT id FROM ${tableName}
             WHERE stream_name = $1 
               AND group_name = $2
               AND (reserved_at IS NULL OR reserved_at < NOW() - INTERVAL '${reservationTimeout} seconds')
               AND expired_at IS NULL
             ORDER BY id
             LIMIT $3
             FOR UPDATE SKIP LOCKED
           )
           RETURNING id, message`,
          [streamName, groupName, batchSize, consumerName],
        );

        const messages: StreamMessage[] = res.rows.map((row: any) => ({
          id: row.id.toString(),
          data: parseStreamMessage(row.message),
        }));

        if (messages.length > 0 || !enableBackoff) {
          return messages;
        }

        // Apply backoff if enabled and no messages found
        await sleepFor(backoff);
        backoff = Math.min(backoff * 2, maxBackoff); // Exponential backoff
      }

      // Return empty array if maxRetries is reached and still no messages
      return [];
    } catch (error) {
      this.logger.error(`postgres-stream-consumer-error-${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async ackAndDelete(
    streamName: string,
    groupName: string,
    messageIds: string[],
  ): Promise<number> {
    return await this.deleteMessages(streamName, groupName, messageIds);
  }

  async acknowledgeMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    // No-op for this implementation
    return messageIds.length;
  }

  async deleteMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      const ids = messageIds.map((id) => parseInt(id));

      // Perform a soft delete by setting `expired_at` to the current timestamp
      await client.query(
        `UPDATE ${tableName}
         SET expired_at = NOW()
         WHERE stream_name = $1 AND id = ANY($2::bigint[]) AND group_name = $3`,
        [streamName, ids, groupName],
      );

      return messageIds.length;
    } catch (error) {
      this.logger.error(`postgres-stream-delete-error-${streamName}`, {
        error,
      });
      throw error;
    }
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
    // Implement retry logic if needed
    return [];
  }

  async getStreamStats(streamName: string): Promise<StreamStats> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      const res = await client.query(
        `SELECT COUNT(*) AS available_count 
        FROM ${tableName} 
        WHERE stream_name = $1 AND expired_at IS NULL`,
        [streamName],
      );
      return {
        messageCount: parseInt(res.rows[0].available_count, 10),
      };
    } catch (error) {
      this.logger.error(`postgres-stream-stats-error-${streamName}`, { error });
      throw error;
    }
  }

  async getStreamDepth(streamName: string): Promise<number> {
    const stats = await this.getStreamStats(streamName);
    return stats.messageCount;
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      const streams = streamNames.map((s) => s.stream);

      const res = await client.query(
        `SELECT stream_name, COUNT(*) AS count
         FROM ${tableName}
         WHERE stream_name = ANY($1::text[])
         GROUP BY stream_name`,
        [streams],
      );

      const result = res.rows.map((row: any) => ({
        stream: row.stream_name,
        depth: parseInt(row.count, 10),
      }));

      return result;
    } catch (error) {
      this.logger.error('postgres-stream-depth-error', { error });
      throw error;
    }
  }

  async trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      let expiredCount = 0;

      if (options.maxLen !== undefined) {
        const res = await client.query(
          `WITH to_expire AS (
            SELECT id FROM ${tableName}
            WHERE stream_name = $1
            ORDER BY id ASC
            OFFSET $2
          )
          UPDATE ${tableName}
          SET expired_at = NOW()
          WHERE id IN (SELECT id FROM to_expire)`,
          [streamName, options.maxLen],
        );
        expiredCount += res.rowCount;
      }

      if (options.maxAge !== undefined) {
        const res = await client.query(
          `UPDATE ${tableName}
          SET expired_at = NOW()
          WHERE stream_name = $1 AND created_at < NOW() - INTERVAL '${options.maxAge} milliseconds'`,
          [streamName],
        );
        expiredCount += res.rowCount;
      }

      return expiredCount;
    } catch (error) {
      this.logger.error(`postgres-stream-trim-error-${streamName}`, { error });
      throw error;
    }
  }

  getProviderSpecificFeatures() {
    return {
      supportsBatching: true,
      supportsDeadLetterQueue: false,
      supportsOrdering: true,
      supportsTrimming: true,
      supportsRetry: false,
      supportsNotifications: this.isNotificationsEnabled(),
      maxMessageSize: 1024 * 1024,
      maxBatchSize: 256,
    };
  }

  // Cleanup method to be called when shutting down
  async cleanup(): Promise<void> {
    // Clean up this instance's notification consumers
    const clientNotificationConsumers =
      PostgresStreamService.clientNotificationConsumers.get(this.streamClient);
    if (clientNotificationConsumers) {
      // Remove this instance from all consumer maps
      for (const consumerKey of this.instanceNotificationConsumers) {
        const instanceMap = clientNotificationConsumers.get(consumerKey);
        if (instanceMap) {
          const consumer = instanceMap.get(this);
          if (consumer) {
            consumer.isListening = false;
            instanceMap.delete(this);

            // If no more instances for this consumer, stop listening
            if (instanceMap.size === 0) {
              clientNotificationConsumers.delete(consumerKey);
              const channelName = getNotificationChannelName(
                consumer.streamName,
                consumer.groupName,
              );
              try {
                await this.streamClient.query(`UNLISTEN "${channelName}"`);
                this.logger.debug('postgres-stream-cleanup-unlisten', {
                  streamName: consumer.streamName,
                  groupName: consumer.groupName,
                  channelName,
                });
              } catch (error) {
                this.logger.error('postgres-stream-cleanup-unlisten-error', {
                  streamName: consumer.streamName,
                  groupName: consumer.groupName,
                  channelName,
                  error,
                });
              }
            }
          }
        }
      }
    }

    // Clear instance tracking
    this.instanceNotificationConsumers.clear();

    // If no more consumers exist for this client, clean up static resources
    if (clientNotificationConsumers && clientNotificationConsumers.size === 0) {
      // Remove client from static maps
      PostgresStreamService.clientNotificationConsumers.delete(
        this.streamClient,
      );
      PostgresStreamService.clientNotificationHandlers.delete(
        this.streamClient,
      );

      // Stop fallback poller for this client
      const fallbackIntervalId =
        PostgresStreamService.clientFallbackPollers.get(this.streamClient);
      if (fallbackIntervalId) {
        clearInterval(fallbackIntervalId);
        PostgresStreamService.clientFallbackPollers.delete(this.streamClient);
      }

      // Remove notification handler
      if (this.streamClient.removeAllListeners) {
        this.streamClient.removeAllListeners('notification');
      } else if (this.streamClient.off && this.notificationHandlerBound) {
        this.streamClient.off('notification', this.notificationHandlerBound);
      }
    }
  }
}

export { PostgresStreamService };
