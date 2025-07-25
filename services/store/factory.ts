import { identifyProvider } from '../../modules/utils';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { StoreInitializable } from './providers/store-initializable';
import { PostgresStoreService } from './providers/postgres/postgres';
import { StoreService } from './index';

class StoreServiceFactory {
  static async init(
    providerClient: ProviderClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<
    StoreService<ProviderClient, ProviderTransaction> & StoreInitializable
  > {
    let service: StoreService<ProviderClient, ProviderTransaction> &
      StoreInitializable;
    if (identifyProvider(providerClient) === 'postgres') {
      service = new PostgresStoreService(providerClient);
    } //etc
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StoreServiceFactory };
