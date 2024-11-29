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
  NatsClientType,
  NatsSubscriptionType,
} from '../../../../types/nats';
import { ProviderClient, ProviderTransaction } from '../../../../types/provider';

class NatsSubService extends SubService<NatsClientType & ProviderClient> {
  private subscriptions: Map<string, NatsSubscriptionType>;

  constructor(eventClient: NatsClientType, storeClient: NatsClientType) {
    super(eventClient, storeClient);
    this.subscriptions = new Map();
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

  transact(): ProviderTransaction {
    // NATS does not support transactions like Redis.
    // Return an empty object or throw an error if not supported.
    return {} as ProviderTransaction;
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, params).replace(/:/g, '.').replace(/\.$/g, '.X');
  }

  async subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    topic?: string,
  ): Promise<void> {
    const subject = this.mintKey(keyType, { appId, engineId: topic });
    const subscription = this.eventClient.subscribe(subject);
    this.subscriptions.set(subject, subscription);
    this.logger.debug(`nats-subscribe ${subject}`);

    (async () => {
      for await (const msg of subscription) {
        try {
          const payload = JSON.parse(msg.data.toString());
          callback(subject, payload);
        } catch (e) {
          this.logger.error(`nats-subscribe-message-parse-error: ${msg.data.toString()}`, e);
        }
      }
    })().catch((err) => {
      this.logger.error(`nats-subscribe-error ${subject}`, err);
    });
  }

  async unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const subject = this.mintKey(keyType, { appId, engineId });
    const subscription = this.subscriptions.get(subject);
    if (subscription) {
      subscription.unsubscribe();
      this.subscriptions.delete(subject);
      this.logger.debug(`nats-unsubscribe ${subject}`);
    } else {
      this.logger.warn(`nats-unsubscribe-error ${subject}`);
    }
  }

  async psubscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    topic?: string,
  ): Promise<void> {
    const subject = this.mintKey(keyType, { appId, engineId: topic });
    const subscription = this.eventClient.subscribe(subject);
    this.subscriptions.set(subject, subscription);
    this.logger.debug(`nats-psubscribe ${subject}`);

    (async () => {
      for await (const msg of subscription) {
        try {
          const payload = JSON.parse(msg.data.toString());
          callback(msg.subject, payload);
        } catch (e) {
          this.logger.error(`nats-parse-psubscription-message-error ${msg.data.toString()}`, e);
        }
      }
    })().catch((err) => {
      this.logger.error(`nats-pattern-psubscription-error ${subject}`, err);
    });
  }

  async punsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    topic?: string,
  ): Promise<void> {
    const subject = this.mintKey(keyType, { appId, engineId: topic });
    const subscription = this.subscriptions.get(subject);
    if (subscription) {
      subscription.unsubscribe();
      this.subscriptions.delete(subject);
      this.logger.debug(`nats-punsubscribe ${subject}`);
    } else {
      this.logger.warn(`nats-punsubscribe-error ${subject}`);
    }
  }

  async publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    topic?: string,
  ): Promise<boolean> {
    const subject = this.mintKey(keyType, { appId, engineId: topic });
    try {
      // Use the storeClient for publishing if necessary
      this.storeClient.publish(subject, JSON.stringify(message));
      this.logger.debug(`nats-publish ${subject}`);
      return true;
    } catch (err) {
      this.logger.error(`nats-publish-error ${subject}`, err);
      return false;
    }
  }
}

export { NatsSubService };
