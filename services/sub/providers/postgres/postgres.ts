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

  mintSafeKey(type: KeyType, params: KeyStoreParams): [string, string] {
    const originalKey = this.mintKey(type, params);

    if (originalKey.length <= 63) {
      return [originalKey, originalKey];
    }

    const { appId = '', engineId = '' } = params;
    const baseKey = `${this.namespace}:${appId}:${type}`;
    const maxHashLength = 63 - baseKey.length - 1; // Reserve space for `:` delimiter

    const engineIdHash = crypto.createHash('sha256').update(engineId).digest('hex').substring(0, maxHashLength);
    const safeKey = `${baseKey}:${engineIdHash}`;

    return [originalKey, safeKey];
  }

  async subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const [originalKey, safeKey] = this.mintSafeKey(keyType, { appId, engineId });

    // Start listening to the safe topic
    await this.eventClient.query(`LISTEN "${safeKey}"`);
    this.logger.debug(`postgres-subscribe`, { originalKey, safeKey });

    // Set up the notification handler
    this.eventClient.on('notification', (msg: { channel: string; payload: any }) => {
      if (msg.channel === safeKey) {
        try {
          const payload = JSON.parse(msg.payload || '{}');
          callback(safeKey, payload);
        } catch (err) {
          this.logger.error(`Error parsing message for topic ${safeKey}:`, err);
        }
      }
    });
  }

  async unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void> {
    const [originalKey, safeKey] = this.mintSafeKey(keyType, { appId, engineId });

    // Stop listening to the safe topic
    await this.eventClient.query(`UNLISTEN "${safeKey}"`);
    this.logger.debug(`postgres-subscribe`, { originalKey, safeKey });
  }

  async publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    engineId?: string,
  ): Promise<boolean> {
    const [originalKey, safeKey] = this.mintSafeKey(keyType, { appId, engineId });

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
