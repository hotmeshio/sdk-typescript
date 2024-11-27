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
    const self = this;
    const topic = this.mintKey(keyType, { appId, engineId });
    await this.eventClient.subscribe(topic, (err) => {
      if (err) {
        self.logger.error(`Error subscribing to: ${topic}`, err);
      }
    });
    this.eventClient.on('message', (channel: string, message: string) => {
      if (channel === topic) {
        try {
          const payload = JSON.parse(message);
          callback(topic, payload);
        } catch (e) {
          self.logger.error(`Error parsing message: ${message}`, e);
        }
      }
    });
  }

  async unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    await this.eventClient.unsubscribe(topic);
  }

  async psubscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const self = this;
    const topic = this.mintKey(keyType, { appId, engineId });
    await this.eventClient.psubscribe(topic, (err) => {
      if (err) {
        self.logger.error(`Error subscribing to: ${topic}`, err);
      }
    });
    this.eventClient.on('pmessage', (pattern, channel, message) => {
      if (pattern === topic) {
        try {
          const payload = JSON.parse(message);
          callback(channel, payload);
        } catch (e) {
          self.logger.error(`Error parsing message: ${message}`, e);
        }
      }
    });
  }

  async punsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });
    await this.eventClient.punsubscribe(topic);
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
    const status: number = await this.storeClient.publish(
      topic,
      JSON.stringify(message),
    );
    return status === 1;
  }
}

export { IORedisSubService };
