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

class RedisSubService extends SubService<ClientProvider, TransactionProvider> {
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
      const self = this;
      const topic = this.mintKey(keyType, { appId, engineId });
      await this.eventClient.subscribe(topic, (message) => {
        try {
          const payload = JSON.parse(message);
          callback(topic, payload);
        } catch (e) {
          self.logger.error(`Error parsing message: ${message}`, e);
        }
      });
    }
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
    if (this.eventClient) {
      const self = this;
      const topic = this.mintKey(keyType, { appId, engineId });
      await this.eventClient.pSubscribe(topic, (message, channel) => {
        try {
          const payload = JSON.parse(message);
          callback(channel, payload);
        } catch (e) {
          self.logger.error(`Error parsing message: ${message}`, e);
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
    await this.eventClient.pUnsubscribe(topic);
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
    return status > 0;
  }
}

export { RedisSubService };
