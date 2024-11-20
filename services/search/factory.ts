import { identifyProvider } from '../../modules/utils';
import { ILogger } from '../logger';

import { SearchService } from './index';
import { PostgresSearchService } from './providers/postgres/postgres';
import { IORedisSearchService } from './providers/redis/ioredis';
import { RedisSearchService } from './providers/redis/redis';
import { PostgresClientType } from '../../types/postgres';
import { ProviderClient } from '../../types/provider';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';

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
    } else if (identifyProvider(providerClient) === 'redis') {
      service = new RedisSearchService(
        providerClient as RedisRedisClientType,
        storeProviderClient as RedisRedisClientType,
      );
    } else {
      service = new IORedisSearchService(
        providerClient as IORedisClientType,
        storeProviderClient as IORedisClientType,
      );
    }
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { SearchServiceFactory };
