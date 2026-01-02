import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../../modules/key';
import { ILogger } from '../../../logger';
import { SubService } from '../../index';
import {
  IORedisClientType as RedisClientType,
  IORedisMultiType as RedisMultiType,
} from '../../../../types/redis';
import { SubscriptionCallback } from '../../../../types/quorum';

class IORedisSubService extends SubService<RedisClientType> {
  private subscriptions: Map<string, Set<SubscriptionCallback>> = new Map();
  private messageHandlerSet = false;
  private pmessageHandlerSet = false;

  constructor(eventClient: RedisClientType, storeClient: RedisClientType) {
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
  }

  transact(): RedisMultiType {
    return this.eventClient.multi();
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, params);
  }

  async subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    
    // Add callback to set (supports multiple subscribers per topic)
    let callbacks = this.subscriptions.get(topic);
    const isFirstSubscription = !callbacks;
    if (!callbacks) {
      callbacks = new Set();
      this.subscriptions.set(topic, callbacks);
    }
    callbacks.add(callback);
    
    this.logger.debug('ioredis-subscribe', {
      topic,
      isFirstSubscription,
      totalCallbacks: callbacks.size,
      messageHandlerSet: this.messageHandlerSet,
    });
    
    // Set up message handler only once
    if (!this.messageHandlerSet) {
      this.eventClient.on('message', (channel: string, message: string) => {
        try {
          const payload = JSON.parse(message);
          const cbs = this.subscriptions.get(channel);
          this.logger.debug('ioredis-message-received', {
            channel,
            callbackCount: cbs?.size || 0,
            payload,
          });
          if (cbs) {
            // Call all callbacks for this channel
            cbs.forEach(cb => {
              try {
                cb(channel, payload);
              } catch (err) {
                this.logger.error(`Error in callback for ${channel}`, err);
              }
            });
          }
        } catch (e) {
          this.logger.error(`Error parsing message: ${message}`, e);
        }
      });
      this.messageHandlerSet = true;
    }
    
    await this.eventClient.subscribe(topic, (err) => {
      if (err) {
        this.logger.error(`Error subscribing to: ${topic}`, err);
      }
    });
  }

  async unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    const callbacks = this.subscriptions.get(topic);
    
    this.logger.debug('ioredis-unsubscribe', {
      topic,
      hadCallbacks: !!callbacks,
      callbackCount: callbacks?.size || 0,
    });
    
    // Only unsubscribe from Redis if no more callbacks exist
    if (callbacks && callbacks.size > 0) {
      this.subscriptions.delete(topic);
      await this.eventClient.unsubscribe(topic);
    }
  }

  async psubscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    
    // Add callback to set (supports multiple subscribers per topic)
    let callbacks = this.subscriptions.get(topic);
    if (!callbacks) {
      callbacks = new Set();
      this.subscriptions.set(topic, callbacks);
    }
    callbacks.add(callback);
    
    // Set up pmessage handler only once
    if (!this.pmessageHandlerSet) {
      this.eventClient.on('pmessage', (pattern, channel, message) => {
        try {
          const payload = JSON.parse(message);
          const cbs = this.subscriptions.get(pattern);
          if (cbs) {
            // Call all callbacks for this pattern
            cbs.forEach(cb => {
              try {
                cb(channel, payload);
              } catch (err) {
                this.logger.error(`Error in callback for ${pattern}`, err);
              }
            });
          }
        } catch (e) {
          this.logger.error(`Error parsing message: ${message}`, e);
        }
      });
      this.pmessageHandlerSet = true;
    }
    
    await this.eventClient.psubscribe(topic, (err) => {
      if (err) {
        this.logger.error(`Error subscribing to: ${topic}`, err);
      }
    });
  }

  async punsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    const callbacks = this.subscriptions.get(topic);
    
    // Only unsubscribe from Redis if no more callbacks exist
    if (callbacks && callbacks.size > 0) {
      this.subscriptions.delete(topic);
      await this.eventClient.punsubscribe(topic);
    }
  }

  async publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    engineId?: string,
  ): Promise<boolean> {
    const topic = this.mintKey(keyType, { appId, engineId });
    //NOTE: `storeClient.publish` is used,
    //      because a Redis connection with subscriptions
    //      may not publish (is read only).
    this.logger.debug('ioredis-publish', { topic, message });
    try {
      const status: number = await this.storeClient.publish(
        topic,
        JSON.stringify(message),
      );
      return status === 1;
    } catch (error) {
      // Connection closed during test cleanup - log and continue gracefully
      if (error?.message?.includes('Connection is closed')) {
        this.logger.debug('ioredis-publish-connection-closed', { topic, message: message.type });
        return false;
      }
      // Re-throw unexpected errors
      throw error;
    }
  }
}

export { IORedisSubService };
