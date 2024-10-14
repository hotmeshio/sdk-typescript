// stream/factory.ts
import { identifyRedisType } from '../../modules/utils';
import {
  RedisClient,
  RedisRedisClientType,
  IORedisClientType,
} from '../../types/redis';
import { ILogger } from '../logger';

import { IORedisStreamService } from './clients/ioredis';
import { RedisStreamService } from './clients/redis';

import { StreamService } from './index';

class StreamServiceFactory {
  static async init(
    redisClient: RedisClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<StreamService<any, any>> {
    let service: StreamService<any, any>;
    if (identifyRedisType(redisClient) === 'redis') {
      service = new RedisStreamService(redisClient as RedisRedisClientType);
    } else {
      service = new IORedisStreamService(redisClient as IORedisClientType);
    }
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StreamServiceFactory };
