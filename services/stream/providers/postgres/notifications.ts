import { ILogger } from '../../../logger';
import { PostgresClientType, PostgresNotification } from '../../../../types/postgres';
import { NotificationConsumer, StreamMessage } from '../../../../types/stream';
import { ProviderClient } from '../../../../types/provider';
import { HMSH_ROUTER_POLL_FALLBACK_INTERVAL } from '../../../../modules/enums';
import { getNotificationChannelName } from './kvtables';

/**
 * Manages PostgreSQL LISTEN/NOTIFY for stream message notifications.
 * Handles static state shared across all service instances using the same client.
 *
 * Channel naming uses table-type prefixes (eng_ / wrk_) instead of group_name,
 * since engine_streams and worker_streams are separate tables.
 */
export class NotificationManager<TService> {
  // Static maps shared across all instances with the same client
  private static clientNotificationConsumers: Map<
    any,
    Map<string, Map<any, NotificationConsumer>>
  > = new Map();
  private static clientNotificationHandlers: Map<any, boolean> = new Map();
  private static clientFallbackPollers: Map<any, NodeJS.Timeout> = new Map();

  // Instance-level tracking
  private instanceNotificationConsumers: Set<string> = new Set();
  private notificationHandlerBound: (notification: PostgresNotification) => void;

  constructor(
    private client: PostgresClientType & ProviderClient,
    private getTableName: () => string,
    private getFallbackInterval: () => number,
    private logger: ILogger,
  ) {
    this.notificationHandlerBound = this.handleNotification.bind(this);
  }

  /**
   * Set up notification handler for this client (once per client).
   */
  setupClientNotificationHandler(serviceInstance: TService): void {
    if (NotificationManager.clientNotificationHandlers.get(this.client)) {
      return;
    }

    if (!NotificationManager.clientNotificationConsumers.has(this.client)) {
      NotificationManager.clientNotificationConsumers.set(
        this.client,
        new Map(),
      );
    }

    this.client.on('notification', this.notificationHandlerBound);
    NotificationManager.clientNotificationHandlers.set(this.client, true);
  }

  /**
   * Start fallback poller for missed notifications (once per client).
   */
  startClientFallbackPoller(
    checkForMissedMessages: () => Promise<void>,
  ): void {
    if (NotificationManager.clientFallbackPollers.has(this.client)) {
      return;
    }

    const interval = this.getFallbackInterval();

    const fallbackIntervalId = setInterval(() => {
      checkForMissedMessages().catch((error) => {
        this.logger.error('postgres-stream-fallback-poller-error', { error });
      });
    }, interval);

    NotificationManager.clientFallbackPollers.set(
      this.client,
      fallbackIntervalId,
    );
  }

  /**
   * Check for missed messages (fallback polling).
   */
  async checkForMissedMessages(
    fetchMessages: (
      instance: TService,
      consumer: NotificationConsumer,
    ) => Promise<StreamMessage[]>,
  ): Promise<void> {
    const now = Date.now();

    // Check for visible messages using notify_visible_messages function
    try {
      const tableName = this.getTableName();
      const schemaName = tableName.split('.')[0];

      const result = await this.client.query(
        `SELECT ${schemaName}.notify_visible_messages() as count`
      );

      const notificationCount = result.rows[0]?.count || 0;

      if (notificationCount > 0) {
        this.logger.info('postgres-stream-visibility-notifications', {
          count: notificationCount,
        });
      }
    } catch (error) {
      if (error.message?.includes('Client was closed')) {
        return;
      }
      this.logger.debug('postgres-stream-visibility-function-unavailable', {
        error: error.message,
      });
    }

    // Traditional fallback check for active notification consumers
    const clientNotificationConsumers =
      NotificationManager.clientNotificationConsumers.get(this.client);

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
            const messages = await fetchMessages(
              instance as TService,
              consumer,
            );

            if (messages.length > 0) {
              this.logger.debug('postgres-stream-fallback-messages', {
                streamName: consumer.streamName,
                groupName: consumer.groupName,
                messageCount: messages.length,
              });
              consumer.callback(messages);
            }

            consumer.lastFallbackCheck = now;
          } catch (error) {
            if (error.message?.includes('Client was closed')) {
              consumer.isListening = false;
              return;
            }
            this.logger.error('postgres-stream-fallback-error', {
              streamName: consumer.streamName,
              groupName: consumer.groupName,
              error,
            });
          }
        }
      }
    }
  }

  /**
   * Handle incoming PostgreSQL notification.
   * Channels use table-type prefixes: eng_ for engine, wrk_ for worker.
   */
  private handleNotification(notification: PostgresNotification): void {
    try {
      // Only handle stream notifications (eng_ or wrk_ prefixed)
      if (!notification.channel.startsWith('eng_') && !notification.channel.startsWith('wrk_')) {
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
      const { stream_name, table_type } = payload;

      if (!stream_name || !table_type) {
        this.logger.warn('postgres-stream-invalid-notification', {
          notification,
        });
        return;
      }

      // Derive groupName from table_type for consumer key lookup
      const groupName = table_type === 'engine' ? 'ENGINE' : 'WORKER';
      const consumerKey = this.getConsumerKey(stream_name, groupName);
      const clientNotificationConsumers =
        NotificationManager.clientNotificationConsumers.get(this.client);

      if (!clientNotificationConsumers) {
        return;
      }

      const instanceMap = clientNotificationConsumers.get(consumerKey);
      if (!instanceMap) {
        return;
      }

      for (const [instance, consumer] of instanceMap.entries()) {
        if (consumer.isListening) {
          const serviceInstance = instance as any;
          if (serviceInstance.fetchAndDeliverMessages) {
            serviceInstance.fetchAndDeliverMessages(consumer);
          }
        }
      }
    } catch (error) {
      this.logger.error('postgres-stream-notification-parse-error', {
        notification,
        error,
      });
    }
  }

  /**
   * Set up notification consumer for a stream/group.
   * Uses table-type channel naming (eng_ / wrk_).
   */
  async setupNotificationConsumer(
    serviceInstance: TService,
    streamName: string,
    groupName: string,
    consumerName: string,
    callback: (messages: StreamMessage[]) => void,
  ): Promise<void> {
    const startTime = Date.now();
    const isEngine = groupName === 'ENGINE';

    // Resolve the stream name to get the simplified form for channel naming and consumer key
    const serviceAny = serviceInstance as any;
    let resolvedStreamName = streamName;
    if (serviceAny.resolveStreamTarget) {
      const target = serviceAny.resolveStreamTarget(streamName);
      resolvedStreamName = target.streamName;
    }

    // Use resolved stream name for consumer key so it matches notification payloads
    const consumerKey = this.getConsumerKey(resolvedStreamName, groupName);
    const channelName = getNotificationChannelName(resolvedStreamName, isEngine);

    // Get or create notification consumer map for this client
    let clientNotificationConsumers =
      NotificationManager.clientNotificationConsumers.get(this.client);
    if (!clientNotificationConsumers) {
      clientNotificationConsumers = new Map();
      NotificationManager.clientNotificationConsumers.set(
        this.client,
        clientNotificationConsumers,
      );
    }

    // Get or create instance map for this consumer key
    let instanceMap = clientNotificationConsumers.get(consumerKey);
    if (!instanceMap) {
      instanceMap = new Map();
      clientNotificationConsumers.set(consumerKey, instanceMap);

      // Set up LISTEN for this channel (only once per channel)
      try {
        const listenStart = Date.now();
        await this.client.query(`LISTEN "${channelName}"`);
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
        throw error;
      }
    }

    // Register consumer for this instance
    const consumer: NotificationConsumer = {
      streamName,
      groupName,
      consumerName,
      callback,
      isListening: true,
      lastFallbackCheck: Date.now(),
    };

    instanceMap.set(serviceInstance, consumer);
    this.instanceNotificationConsumers.add(consumerKey);

    this.logger.debug('postgres-stream-notification-setup-complete', {
      streamName,
      groupName,
      instanceCount: instanceMap.size,
      setupDuration: Date.now() - startTime,
    });
  }

  /**
   * Stop notification consumer for a stream/group.
   */
  async stopNotificationConsumer(
    serviceInstance: TService,
    streamName: string,
    groupName: string,
  ): Promise<void> {
    const isEngine = groupName === 'ENGINE';
    const serviceAny = serviceInstance as any;
    let resolvedStreamName = streamName;
    if (serviceAny.resolveStreamTarget) {
      const target = serviceAny.resolveStreamTarget(streamName);
      resolvedStreamName = target.streamName;
    }

    const consumerKey = this.getConsumerKey(resolvedStreamName, groupName);
    const clientNotificationConsumers =
      NotificationManager.clientNotificationConsumers.get(this.client);

    if (!clientNotificationConsumers) {
      return;
    }

    const instanceMap = clientNotificationConsumers.get(consumerKey);
    if (!instanceMap) {
      return;
    }

    const consumer = instanceMap.get(serviceInstance);
    if (consumer) {
      consumer.isListening = false;
      instanceMap.delete(serviceInstance);
      this.instanceNotificationConsumers.delete(consumerKey);

      if (instanceMap.size === 0) {
        clientNotificationConsumers.delete(consumerKey);
        const channelName = getNotificationChannelName(resolvedStreamName, isEngine);

        try {
          await this.client.query(`UNLISTEN "${channelName}"`);
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

  /**
   * Clean up notification consumers for this instance.
   */
  async cleanup(serviceInstance: TService): Promise<void> {
    const clientNotificationConsumers =
      NotificationManager.clientNotificationConsumers.get(this.client);

    // FIRST: Stop fallback poller to prevent queries during cleanup
    const fallbackIntervalId =
      NotificationManager.clientFallbackPollers.get(this.client);
    if (fallbackIntervalId) {
      clearInterval(fallbackIntervalId);
      NotificationManager.clientFallbackPollers.delete(this.client);
    }

    if (clientNotificationConsumers) {
      for (const consumerKey of this.instanceNotificationConsumers) {
        const instanceMap = clientNotificationConsumers.get(consumerKey);
        if (instanceMap) {
          const consumer = instanceMap.get(serviceInstance);
          if (consumer) {
            consumer.isListening = false;
            instanceMap.delete(serviceInstance);

            if (instanceMap.size === 0) {
              clientNotificationConsumers.delete(consumerKey);

              // Extract resolved stream name and isEngine from the consumer key
              // Consumer key format: resolvedStreamName:groupName
              const isEngine = consumer.groupName === 'ENGINE';
              const resolvedStreamName = consumerKey.substring(0, consumerKey.lastIndexOf(':'));
              const channelName = getNotificationChannelName(resolvedStreamName, isEngine);

              try {
                await this.client.query(`UNLISTEN "${channelName}"`);
                this.logger.debug('postgres-stream-cleanup-unlisten', {
                  streamName: consumer.streamName,
                  groupName: consumer.groupName,
                  channelName,
                });
              } catch (error) {
                if (!error.message?.includes('Client was closed')) {
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
    }

    this.instanceNotificationConsumers.clear();

    if (clientNotificationConsumers && clientNotificationConsumers.size === 0) {
      NotificationManager.clientNotificationConsumers.delete(this.client);
      NotificationManager.clientNotificationHandlers.delete(this.client);

      if (this.client.removeAllListeners) {
        this.client.removeAllListeners('notification');
      } else if (this.client.off && this.notificationHandlerBound) {
        this.client.off('notification', this.notificationHandlerBound);
      }
    }
  }

  /**
   * Get consumer key from stream and group names.
   */
  private getConsumerKey(streamName: string, groupName: string): string {
    return `${streamName}:${groupName}`;
  }
}

/**
 * Get configuration values for notification settings.
 */
export function getFallbackInterval(config: any): number {
  return config?.postgres?.notificationFallbackInterval || HMSH_ROUTER_POLL_FALLBACK_INTERVAL;
}

export function getNotificationTimeout(config: any): number {
  return config?.postgres?.notificationTimeout || 5000;
}
