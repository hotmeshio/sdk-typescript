import { identifyRedisType } from '../../modules/utils';
import {
  RedisClient,
  RedisRedisClientType,
  IORedisClientType,
} from '../../types/redis';
import { ILogger } from '../logger';

import { IORedisSubService } from './providers/redis/ioredis';
import { RedisSubService } from './providers/redis/redis';

import { SubService } from './index';

class SubServiceFactory {
  static async init(
    redisClient: RedisClient,
    redisStoreClient: RedisClient,
    namespace: string,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<SubService<any, any>> {
    let service: SubService<any, any>;
    if (identifyRedisType(redisClient) === 'redis') {
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
