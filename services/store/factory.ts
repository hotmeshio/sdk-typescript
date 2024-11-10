import { identifyProvider } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/hotmesh';

import { IORedisStoreService } from './providers/redis/ioredis';
import { RedisStoreService } from './providers/redis/redis';
import { StoreInitializable } from './providers/store-initializable';

import { StoreService } from './index';

class StoreServiceFactory {
  static async init(
    redisClient: ProviderClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<
    StoreService<ProviderClient, ProviderTransaction> & StoreInitializable
  > {
    let service: StoreService<ProviderClient, ProviderTransaction> &
      StoreInitializable;
    if (identifyProvider(redisClient) === 'redis') {
      service = new RedisStoreService(redisClient as RedisRedisClientType);
    } else {
      service = new IORedisStoreService(redisClient as IORedisClientType);
    }
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StoreServiceFactory };
