import { identifyProvider } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { NatsClientType } from '../../types/nats';
import { PostgresClientType } from '../../types';

import { IORedisStreamService } from './providers/redis/ioredis';
import { RedisStreamService } from './providers/redis/redis';
import { StreamInitializable } from './providers/stream-initializable';
import { NatsStreamService } from './providers/nats/nats';
import { PostgresStreamService } from './providers/postgres/postgres';

import { StreamService } from './index';

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
      } else {
        //ioredis
        storeProvider = storeProvider as IORedisClientType;
      }
      service = new NatsStreamService(
        provider as NatsClientType,
        storeProvider,
      );
    } else if (providerType === 'postgres') {
      service = new PostgresStreamService(
        provider as PostgresClientType & ProviderClient,
        storeProvider as IORedisClientType,
      );
    } else if (providerType === 'redis') {
      service = new RedisStreamService(
        provider as RedisRedisClientType,
        storeProvider as RedisRedisClientType,
      );
    } else if (providerType === 'ioredis') {
      service = new IORedisStreamService(
        provider as IORedisClientType,
        storeProvider as IORedisClientType,
      );
    } //etc register other providers here
    await service.init(namespace, appId, logger);
    return service;
  }
}

export { StreamServiceFactory };
