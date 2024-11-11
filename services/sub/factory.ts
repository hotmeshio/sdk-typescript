import { identifyProvider } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

import { IORedisSubService } from './providers/redis/ioredis';
import { RedisSubService } from './providers/redis/redis';

import { SubService } from './index';

class SubServiceFactory {
  static async init(
    redisClient: ProviderClient,
    redisStoreClient: ProviderClient,
    namespace: string,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<SubService<ProviderClient, ProviderTransaction>> {
    let service: SubService<ProviderClient, ProviderTransaction>;
    if (identifyProvider(redisClient) === 'redis') {
      service = new RedisSubService(
        redisClient as RedisRedisClientType,
        redisStoreClient as RedisRedisClientType,
      );
    } else {
      service = new IORedisSubService(
        redisClient as IORedisClientType,
        redisStoreClient as IORedisClientType,
      );
    }
    await service.init(namespace, appId, engineId, logger);
    return service;
  }
}

export { SubServiceFactory };
