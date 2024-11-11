import { guid, identifyProvider, polyfill } from '../../modules/utils';
import { HotMeshEngine, HotMeshWorker } from '../../types/hotmesh';
import { ProviderConfig } from '../../types/provider';
import {
  RedisRedisClassType,
  RedisRedisClientOptions,
  IORedisClassType,
  IORedisClientOptions,
} from '../../types/redis';
import { NatsClassType, NatsClientOptions } from '../../types/nats';

import { RedisConnection as IORedisConnection } from './providers/ioredis';
import { RedisConnection } from './providers/redis';
import { NatsConnection } from './providers/nats';

import { AbstractConnection } from './index';

export class ConnectorService {
  /**
   * Initialize `store`, `stream`, and `subscription` clients for any provider.
   */
  static async initClients(
    target: HotMeshEngine | HotMeshWorker,
  ): Promise<void> {
    const connections = target.connections;

    if (connections) {
      // Expanded form
      if (connections.store) {
        await ConnectorService.initClient(connections.store, target, 'store');
      }
      if (connections.stream) {
        await ConnectorService.initClient(connections.stream, target, 'stream');
      }
      if (connections.sub) {
        await ConnectorService.initClient(connections.sub, target, 'sub');
      }
      //todo: add search after refactoring
    } else {
      // Collapsed form
      const ProviderConfig = polyfill.providerConfig(target);
      if (ProviderConfig) {
        await ConnectorService.initClient(ProviderConfig, target, 'store');
        await ConnectorService.initClient(ProviderConfig, target, 'stream');
        await ConnectorService.initClient(ProviderConfig, target, 'sub');
        //todo: add search after refactoring
      }
    }
  }

  static async initClient(
    ProviderConfig: ProviderConfig,
    target: HotMeshEngine | HotMeshWorker,
    field: string,
  ) {
    if (target[field]) {
      return;
    }
    const providerClass = ProviderConfig.class;
    const options = ProviderConfig.options;
    const providerName = identifyProvider(providerClass);

    let clientInstance: AbstractConnection<any, any>;
    const id = guid();

    switch (providerName) {
      case 'redis':
        clientInstance = await RedisConnection.connect(
          id,
          providerClass as RedisRedisClassType,
          options as RedisRedisClientOptions,
        );
        break;
      case 'ioredis':
        clientInstance = await IORedisConnection.connect(
          id,
          providerClass as IORedisClassType,
          options as IORedisClientOptions,
        );
        break;
      case 'nats':
        clientInstance = await NatsConnection.connect(
          id,
          providerClass as NatsClassType,
          options as NatsClientOptions,
        );
        break;
      default:
        throw new Error(`Unknown provider class: ${providerName}`);
    }

    target[field] = clientInstance.getClient();
  }
}
