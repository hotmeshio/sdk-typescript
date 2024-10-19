import { identifyRedisType } from '../../modules/utils';
import {
  RedisClient,
  RedisRedisClientType,
  IORedisClientType,
} from '../../types/redis';
import { ILogger } from '../logger';

import { IORedisStoreService } from './providers/redis/ioredis';
import { RedisStoreService } from './providers/redis/redis';
import { StoreService } from './index';
import { StoreInitializable } from './providers/store-initializable';

class StoreServiceFactory {
  static async init(
    redisClient: RedisClient,
    namespace: string,
    appId: string,
    logger: ILogger
  ): Promise<StoreService<any, any> & StoreInitializable> {
    let service: StoreService<any, any> & StoreInitializable;
    if (identifyRedisType(redisClient) === 'redis') {
      service = new RedisStoreService(redisClient as RedisRedisClientType);
    } else {
      service = new IORedisStoreService(redisClient as IORedisClientType);
    }
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StoreServiceFactory };
