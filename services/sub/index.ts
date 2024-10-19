import { KeyStoreParams, KeyType } from '../../modules/key';
import { ILogger } from '../logger';
import { SubscriptionCallback } from '../../types/quorum';

abstract class SubService<Client, MultiClient> {
  protected eventClient: Client;
  protected storeClient: Client;
  protected namespace: string;
  protected logger: ILogger;
  protected appId: string;

  constructor(eventClient: Client, storeClient: Client) {
    this.eventClient = eventClient;
    this.storeClient = storeClient;
  }

  abstract init(
    namespace: string,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<void>;

  abstract getMulti(): MultiClient;

  abstract mintKey(type: KeyType, params: KeyStoreParams): string;

  abstract subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void>;

  abstract unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void>;

  abstract psubscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    engineId?: string,
  ): Promise<void>;

  abstract punsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    engineId?: string,
  ): Promise<void>;

  abstract publish(
    keyType: KeyType,
    message: Record<string, any>,
    appId: string,
    engineId?: string
  ): Promise<boolean>;
}

export { SubService };