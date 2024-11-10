import { identifyProvider } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/hotmesh';

import { IORedisStreamService } from './providers/redis/ioredis';
import { RedisStreamService } from './providers/redis/redis';
import { StreamInitializable } from './providers/stream-initializable';

import { StreamService } from './index';
import { NatsStreamService } from './providers/nats/nats';
import { NatsClientType } from '../../types/nats';

class StreamServiceFactory {
  static async init(
    provider: ProviderClient,
    storeProvider: ProviderClient,
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<
    StreamService<ProviderClient, ProviderTransaction> & StreamInitializable
  > {
      let service: StreamService<ProviderClient, ProviderTransaction> &
      StreamInitializable;
      const providerType = identifyProvider(provider);
      if (providerType === 'nats') {
        let storeProvider: RedisRedisClientType | IORedisClientType;
        if (identifyProvider(storeProvider) === 'redis') {
          storeProvider = storeProvider as RedisRedisClientType;
        } else { //ioredis
          storeProvider = storeProvider as IORedisClientType;
        }
        service = new NatsStreamService(
          provider as NatsClientType,
          storeProvider,
        );
    } else if (providerType === 'redis') {
        service = new RedisStreamService(
          provider as RedisRedisClientType,
          storeProvider as RedisRedisClientType,
        );
    } else { //ioredis
      service = new IORedisStreamService(
        provider as IORedisClientType,
        storeProvider as IORedisClientType,
      );
    } //etc
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StreamServiceFactory };
