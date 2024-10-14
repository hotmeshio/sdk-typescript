// store/factory.ts

import { identifyRedisType } from '../../modules/utils';
import {
  RedisClient,
  RedisRedisClientType,
  IORedisClientType,
} from '../../types/redis';
import { ILogger } from '../logger';

import { IORedisStoreService } from './clients/ioredis';
import { RedisStoreService } from './clients/redis';

import { StoreService } from './index';

class StoreServiceFactory {
  static async init(
    redisClient: RedisClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<StoreService<any, any>> {
    let service: StoreService<any, any>;
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
