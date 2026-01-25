import { KeyStoreParams, KeyType } from '../../modules/key';
import { ILogger } from '../logger';
import { SubscriptionCallback } from '../../types/quorum';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

abstract class SubService<ClientProvider extends ProviderClient> {
  protected eventClient: ClientProvider;
  protected storeClient: ProviderClient;
  protected namespace: string;
  protected engineId: string;
  protected logger: ILogger;
  protected appId: string;

  constructor(eventClient: ClientProvider, storeClient: ProviderClient) {
    this.eventClient = eventClient;
    this.storeClient = storeClient;
  }

  abstract init(
    namespace: string,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<void>;

  abstract transact(): ProviderTransaction;

  abstract mintKey(type: KeyType, params: KeyStoreParams): string;

  abstract subscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    topic?: string,
  ): Promise<void>;

  abstract unsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    topic?: string,
  ): Promise<void>;

  abstract psubscribe(
    keyType: KeyType.QUORUM,
    callback: SubscriptionCallback,
    appId: string,
    topic?: string,
  ): Promise<void>;

  abstract punsubscribe(
    keyType: KeyType.QUORUM,
    appId: string,
    topic?: string,
  ): Promise<void>;

  abstract publish(
    keyType: KeyType,
    message: Record<string, any>,
    appId: string,
    topic?: string,
    transaction?: ProviderTransaction,
  ): Promise<boolean>;
}

export { SubService };
