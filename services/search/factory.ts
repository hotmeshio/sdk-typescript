import { identifyProvider } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient } from '../../types/hotmesh';

import { IORedisSearchService } from './providers/redis/ioredis';
import { RedisSearchService } from './providers/redis/redis';

import { SearchService } from './index';

class SearchServiceFactory {
  static async init(
    redisClient: ProviderClient,
    redisStoreClient: ProviderClient | undefined,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<SearchService<ProviderClient>> {
    let service: SearchService<ProviderClient>;
    if (identifyProvider(redisClient) === 'redis') {
      service = new RedisSearchService(
        redisClient as RedisRedisClientType,
        redisStoreClient as RedisRedisClientType,
      );
    } else {
      service = new IORedisSearchService(
        redisClient as IORedisClientType,
        redisStoreClient as IORedisClientType,
      );
    }
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { SearchServiceFactory };
