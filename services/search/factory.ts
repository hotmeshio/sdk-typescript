import { identifyRedisType } from '../../modules/utils';
import {
  RedisClient,
  RedisRedisClientType,
  IORedisClientType,
} from '../../types/redis';
import { ILogger } from '../logger';

import { IORedisSearchService } from './providers/redis/ioredis';
import { RedisSearchService } from './providers/redis/redis';

import { SearchService } from './index';

class SearchServiceFactory {
  static async init(
    redisClient: RedisClient,
    redisStoreClient: RedisClient | undefined,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<SearchService<any>> {
    let service: SearchService<any>;
    if (identifyRedisType(redisClient) === 'redis') {
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
