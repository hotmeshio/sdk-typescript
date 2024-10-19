import { identifyRedisType } from '../../modules/utils';
import {
  RedisClient,
  RedisRedisClientType,
  IORedisClientType,
} from '../../types/redis';
import { ILogger } from '../logger';

import { IORedisStreamService } from './providers/redis/ioredis';
import { RedisStreamService } from './providers/redis/redis';

import { StreamService } from './index';
import { StreamInitializable } from './providers/stream-initializable';

class StreamServiceFactory {
  static async init(
    redisClient: RedisClient,
    redisStoreClient: RedisClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<StreamService<any, any> & StreamInitializable<any, any>> {
    let service: StreamService<any, any> & StreamInitializable<any, any>;
    if (identifyRedisType(redisClient) === 'redis') {
      service = new RedisStreamService(
        redisClient as RedisRedisClientType,
        redisStoreClient as RedisRedisClientType,
      );
    } else {
      service = new IORedisStreamService(
        redisClient as IORedisClientType,
        redisStoreClient as IORedisClientType,
      );
    }
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StreamServiceFactory };
