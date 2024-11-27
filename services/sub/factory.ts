import { identifyProvider } from '../../modules/utils';
import { RedisRedisClientType, IORedisClientType } from '../../types/redis';
import { ILogger } from '../logger';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

import { IORedisSubService } from './providers/redis/ioredis';
import { RedisSubService } from './providers/redis/redis';

import { SubService } from './index';
import { PostgresSubService } from './providers/postgres/postgres';
import { PostgresClientType } from '../../types';

class SubServiceFactory {
  static async init(
    providerClient: ProviderClient,
    providerStoreClient: ProviderClient,
    namespace: string,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<SubService<ProviderClient>> {
    let service: SubService<ProviderClient>;
    if (identifyProvider(providerClient) === 'redis') {
      service = new RedisSubService(
        providerClient as RedisRedisClientType,
        providerStoreClient as RedisRedisClientType,
      );
    } else if (identifyProvider(providerClient) === 'postgres') {
      service = new PostgresSubService(
        providerClient as PostgresClientType & ProviderClient,
        providerStoreClient as PostgresClientType & ProviderClient,
      );
    } else {
      service = new IORedisSubService(
        providerClient as IORedisClientType,
        providerStoreClient as IORedisClientType,
      );
    }
    await service.init(namespace, appId, engineId, logger);
    return service;
  }
}

export { SubServiceFactory };
