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

class PostgresSubService extends SubService<PostgresClientType & ProviderClient> {

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
  }

  transact(): ProviderTransaction {
    throw new Error('Transactions are not supported in lightweight pub/sub');
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('Namespace not set');
    return KeyService.mintKey(this.namespace, type, params);
  }

  async subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const topic = this.mintKey(keyType, { appId, engineId });

    // Start listening to the topic
    await this.eventClient.query(`LISTEN "${topic}"`);
    this.logger.info(`postgres-subscribe ${topic}`);

    // Set up the notification handler
    this.eventClient.on('notification', (msg: {channel: string, payload: any}) => {
      if (msg.channel === topic) {
        try {
          const payload = JSON.parse(msg.payload || '{}');
          callback(topic, payload);
        } catch (err) {
          this.logger.error(`Error parsing message for topic ${topic}:`, err);
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

    // Stop listening to the topic
    await this.eventClient.query(`UNLISTEN "${topic}"`);
    this.logger.info(`postgres-unsubscribe ${topic}`);
  }

  async publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    engineId?: string,
  ): Promise<boolean> {
    const topic = this.mintKey(keyType, { appId, engineId });
  
    // Publish the message using NOTIFY
    const payload = JSON.stringify(message).replace(/'/g, "''");
    await this.storeClient.query(`NOTIFY "${topic}", '${payload}'`);
    this.logger.info(`postgres-publish ${topic}`);
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
