import crypto from 'crypto';

import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../../modules/key';
import { ILogger } from '../../../logger';
import { SubService } from '../../index';
import { SubscriptionCallback } from '../../../../types/quorum';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';
import { PostgresClientType } from '../../../../types/postgres';

class PostgresSubService extends SubService<
  PostgresClientType & ProviderClient
> {
  // Static maps to manage subscriptions across all instances sharing the same client
  private static clientSubscriptions: Map<PostgresClientType & ProviderClient, Map<string, Map<PostgresSubService, SubscriptionCallback>>> = new Map();
  private static clientHandlers: Map<PostgresClientType & ProviderClient, boolean> = new Map();

  // Instance-level subscriptions for cleanup
  private instanceSubscriptions: Set<string> = new Set();

  constructor(
    eventClient: PostgresClientType & ProviderClient,
    storeClient?: PostgresClientType & ProviderClient,
  ) {
    super(eventClient, storeClient);
  }

  async init(
    namespace = HMNS,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<void> {
    this.namespace = namespace;
    this.logger = logger;
    this.appId = appId;
    this.engineId = engineId;
    this.setupNotificationHandler();
  }

  private setupNotificationHandler(): void {
    // Check if notification handler is already set up for this client
    if (PostgresSubService.clientHandlers.get(this.eventClient)) {
      return;
    }

    // Initialize subscription map for this client if it doesn't exist
    if (!PostgresSubService.clientSubscriptions.has(this.eventClient)) {
      PostgresSubService.clientSubscriptions.set(this.eventClient, new Map());
    }

    // Set up the notification handler for this client
    this.eventClient.on(
      'notification',
      (msg: { channel: string; payload: any }) => {
        const clientSubscriptions = PostgresSubService.clientSubscriptions.get(this.eventClient);
        const callbacks = clientSubscriptions?.get(msg.channel);
        if (callbacks && callbacks.size > 0) {
          try {
            const payload = JSON.parse(msg.payload || '{}');
            // Call all callbacks registered for this channel across all SubService instances
            callbacks.forEach(callback => {
              try {
                callback(msg.channel, payload);
              } catch (err) {
                this.logger?.error(`Error in subscription callback for ${msg.channel}:`, err);
              }
            });
          } catch (err) {
            this.logger?.error(
              `Error parsing message for topic ${msg.channel}:`,
              err,
            );
          }
        }
      },
    );

    // Mark this client as having a notification handler
    PostgresSubService.clientHandlers.set(this.eventClient, true);
  }

  transact(): ProviderTransaction {
    throw new Error('Transactions are not supported in lightweight pub/sub');
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('Namespace not set');
    return KeyService.mintKey(this.namespace, type, params);
  }

  mintSafeKey(type: KeyType, params: KeyStoreParams): [string, string] {
    const originalKey = this.mintKey(type, params);

    if (originalKey.length <= 63) {
      return [originalKey, originalKey];
    }

    const { appId = '', engineId = '' } = params;
    const baseKey = `${this.namespace}:${appId}:${type}`;
    const maxHashLength = 63 - baseKey.length - 1; // Reserve space for `:` delimiter

    const engineIdHash = crypto
      .createHash('sha256')
      .update(engineId)
      .digest('hex')
      .substring(0, maxHashLength);
    const safeKey = `${baseKey}:${engineIdHash}`;

    return [originalKey, safeKey];
  }

  async subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    topic?: string,
  ): Promise<void> {
    const [originalKey, safeKey] = this.mintSafeKey(keyType, {
      appId,
      engineId: topic,
    });

    // Get or create subscription map for this client
    let clientSubscriptions = PostgresSubService.clientSubscriptions.get(this.eventClient);
    if (!clientSubscriptions) {
      clientSubscriptions = new Map();
      PostgresSubService.clientSubscriptions.set(this.eventClient, clientSubscriptions);
    }

    // Get or create callback array for this channel
    let callbacks = clientSubscriptions.get(safeKey);
    if (!callbacks) {
      callbacks = new Map();
      clientSubscriptions.set(safeKey, callbacks);
      
      // Start listening to the safe topic (only once per channel across all instances)
      await this.eventClient.query(`LISTEN "${safeKey}"`);
    }

    // Add this callback to the list
    callbacks.set(this, callback);
    
    // Track this subscription for cleanup
    this.instanceSubscriptions.add(safeKey);

    this.logger.debug(`postgres-subscribe`, { originalKey, safeKey, totalCallbacks: callbacks.size });
  }

  async unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    topic?: string,
  ): Promise<void> {
    const [originalKey, safeKey] = this.mintSafeKey(keyType, {
      appId,
      engineId: topic,
    });

    const clientSubscriptions = PostgresSubService.clientSubscriptions.get(this.eventClient);
    if (!clientSubscriptions) {
      return;
    }

    const callbacks = clientSubscriptions.get(safeKey);
    if (!callbacks || callbacks.size === 0) {
      return;
    }

    // Remove callback from this specific instance
    callbacks.delete(this);
    
    // Remove from instance tracking
    this.instanceSubscriptions.delete(safeKey);

    // Stop listening to the safe topic if no more callbacks exist
    if (callbacks.size === 0) {
      clientSubscriptions.delete(safeKey);
      await this.eventClient.query(`UNLISTEN "${safeKey}"`);
    }
    
    this.logger.debug(`postgres-unsubscribe`, { originalKey, safeKey, remainingCallbacks: callbacks.size });
  }

  /**
   * Cleanup method to remove all subscriptions for this instance.
   * Should be called when the SubService instance is being destroyed.
   */
  async cleanup(): Promise<void> {
    const clientSubscriptions = PostgresSubService.clientSubscriptions.get(this.eventClient);
    if (!clientSubscriptions) {
      return;
    }

    for (const safeKey of this.instanceSubscriptions) {
      const callbacks = clientSubscriptions.get(safeKey);
      if (callbacks) {
        callbacks.delete(this);
        
        // If no more callbacks exist for this channel, stop listening
        if (callbacks.size === 0) {
          clientSubscriptions.delete(safeKey);
          try {
            await this.eventClient.query(`UNLISTEN "${safeKey}"`);
          } catch (err) {
            this.logger?.error(`Error unlistening from ${safeKey}:`, err);
          }
        }
      }
    }

    this.instanceSubscriptions.clear();
    
    // If no more subscriptions exist for this client, remove it from static maps
    if (clientSubscriptions.size === 0) {
      PostgresSubService.clientSubscriptions.delete(this.eventClient);
      PostgresSubService.clientHandlers.delete(this.eventClient);
    }
  }

  async publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    topic?: string,
  ): Promise<boolean> {
    const [originalKey, safeKey] = this.mintSafeKey(keyType, {
      appId,
      engineId: topic,
    });
    // Publish the message using the safe topic
    const payload = JSON.stringify(message).replace(/'/g, "''");
    await this.storeClient.query(`NOTIFY "${safeKey}", '${payload}'`);
    this.logger.debug(`postgres-publish`, { originalKey, safeKey });
    return true;
  }

  async psubscribe(): Promise<void> {
    throw new Error('Pattern subscriptions are not supported in PostgreSQL');
  }

  async punsubscribe(): Promise<void> {
    throw new Error('Pattern subscriptions are not supported in PostgreSQL');
  }
}

export { PostgresSubService };
