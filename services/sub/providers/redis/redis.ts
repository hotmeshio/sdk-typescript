import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../../modules/key';
import { ILogger } from '../../../logger';
import { SubService } from '../../index';
import {
  RedisRedisClientType as ClientProvider,
  RedisRedisMultiType as TransactionProvider,
} from '../../../../types/redis';
import { SubscriptionCallback } from '../../../../types/quorum';

class RedisSubService extends SubService<ClientProvider> {
  private subscriptions: Map<string, Set<SubscriptionCallback>> = new Map();

  constructor(eventClient: ClientProvider, storeClient: ClientProvider) {
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

  transact(): TransactionProvider {
    const multi = this.eventClient.multi();
    return multi as unknown as TransactionProvider;
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
    if (this.eventClient) {
      const topic = this.mintKey(keyType, { appId, engineId });
      
      // Add callback to set (supports multiple subscribers per topic)
      let callbacks = this.subscriptions.get(topic);
      const isFirstSubscription = !callbacks;
      if (!callbacks) {
        callbacks = new Set();
        this.subscriptions.set(topic, callbacks);
      }
      callbacks.add(callback);
      
      this.logger.debug('redis-subscribe', {
        topic,
        isFirstSubscription,
        totalCallbacks: callbacks.size,
      });
      
      // Only subscribe to Redis once per topic
      if (isFirstSubscription) {
        await this.eventClient.subscribe(topic, (message) => {
          try {
            const payload = JSON.parse(message);
            const cbs = this.subscriptions.get(topic);
            this.logger.debug('redis-message-received', {
              topic,
              callbackCount: cbs?.size || 0,
              payload,
            });
            if (cbs) {
              // Call all callbacks for this topic
              cbs.forEach(cb => {
                try {
                  cb(topic, payload);
                } catch (err) {
                  this.logger.error(`Error in callback for ${topic}`, err);
                }
              });
            }
          } catch (e) {
            this.logger.error(`Error parsing message: ${message}`, e);
          }
        });
      }
    }
  }

  async unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    const callbacks = this.subscriptions.get(topic);
    
    this.logger.debug('redis-unsubscribe', {
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
    if (this.eventClient) {
      const topic = this.mintKey(keyType, { appId, engineId });
      
      // Add callback to set (supports multiple subscribers per topic)
      let callbacks = this.subscriptions.get(topic);
      if (!callbacks) {
        callbacks = new Set();
        this.subscriptions.set(topic, callbacks);
      }
      callbacks.add(callback);
      
      await this.eventClient.pSubscribe(topic, (message, channel) => {
        try {
          const payload = JSON.parse(message);
          const cbs = this.subscriptions.get(topic);
          if (cbs) {
            // Call all callbacks for this topic
            cbs.forEach(cb => {
              try {
                cb(channel, payload);
              } catch (err) {
                this.logger.error(`Error in callback for ${topic}`, err);
              }
            });
          }
        } catch (e) {
          this.logger.error(`Error parsing message: ${message}`, e);
        }
      });
    }
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
      await this.eventClient.pUnsubscribe(topic);
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
    this.logger.debug('redis-publish', { topic, message });
    const status: number = await this.storeClient.publish(
      topic,
      JSON.stringify(message),
    );
    return status > 0;
  }
}

export { RedisSubService };
