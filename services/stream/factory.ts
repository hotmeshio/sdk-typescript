import { identifyRedisType } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/hotmesh';

import { IORedisStreamService } from './providers/redis/ioredis';
import { RedisStreamService } from './providers/redis/redis';
import { StreamInitializable } from './providers/stream-initializable';

import { StreamService } from './index';

class StreamServiceFactory {
  static async init(
    redisClient: ProviderClient,
    redisStoreClient: ProviderClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<
    StreamService<ProviderClient, ProviderTransaction> & StreamInitializable
  > {
    let service: StreamService<ProviderClient, ProviderTransaction> &
      StreamInitializable;
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
    } //etc
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StreamServiceFactory };
