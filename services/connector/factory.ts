import { guid, identifyProvider } from '../../modules/utils';
import {
  ConnectionConfig,
  HotMeshEngine,
  HotMeshWorker,
} from '../../types/hotmesh';
import {
  RedisRedisClassType,
  RedisRedisClientOptions,
  IORedisClassType,
  IORedisClientOptions,
} from '../../types/redis';
import {
  NatsClassType,
  NatsClientOptions,
} from '../../types/nats';

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
    } else {
      // Collapsed form
      const connectionConfig = target.connection || target.redis;
      if (connectionConfig) {
        await ConnectorService.initClient(connectionConfig, target, 'store');
        await ConnectorService.initClient(connectionConfig, target, 'stream');
        await ConnectorService.initClient(connectionConfig, target, 'sub');
      }
    }
  }

  static async initClient(
    connectionConfig: ConnectionConfig,
    target: HotMeshEngine | HotMeshWorker, 
    field: string,
  ) {
    const providerClass = connectionConfig.class;
    const options = connectionConfig.options;
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
