import { identifyProvider } from '../../modules/utils';
import { ILogger } from '../logger';
import { PostgresClientType } from '../../types/postgres';
import { ProviderClient } from '../../types/provider';

import { PostgresSearchService } from './providers/postgres/postgres';

import { SearchService } from './index';

class SearchServiceFactory {
  static async init(
    providerClient: ProviderClient,
    storeProviderClient: ProviderClient | undefined,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<SearchService<ProviderClient>> {
    let service: SearchService<ProviderClient>;
    if (identifyProvider(providerClient) === 'postgres') {
      service = new PostgresSearchService(
        providerClient as PostgresClientType & ProviderClient,
        storeProviderClient as PostgresClientType & ProviderClient,
      );
    } //etc
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { SearchServiceFactory };
