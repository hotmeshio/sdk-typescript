import { guid, identifyProvider, polyfill } from '../../modules/utils';
import { HotMeshEngine, HotMeshWorker } from '../../types/hotmesh';
import { ProviderConfig, ProviderNativeClient } from '../../types/provider';
import {
  RedisRedisClassType,
  RedisRedisClientOptions,
  IORedisClassType,
  IORedisClientOptions,
} from '../../types/redis';
import { NatsClassType, NatsClientOptions } from '../../types/nats';
import { PostgresClassType, PostgresClientOptions } from '../../types/postgres';

import { RedisConnection as IORedisConnection } from './providers/ioredis';
import { NatsConnection } from './providers/nats';
import { PostgresConnection } from './providers/postgres';
import { RedisConnection } from './providers/redis';

import { AbstractConnection } from './index';

export class ConnectorService {

  static async disconnectAll(): Promise<void> {
    await RedisConnection.disconnectAll();
    await IORedisConnection.disconnectAll();
    await PostgresConnection.disconnectAll();
    await NatsConnection.disconnectAll();
  };

  /**
   * Connect to a provider (redis, nats, postgres) and return the native
   * client. Connections are handled by the engine and worker routers at
   * initialization, but the factory method provided here is useful
   * for testing provider configurations.
   */
  static async connectClient(
    ProviderConfig: ProviderConfig,
  ): Promise<ProviderNativeClient> {
    const target: { client?: ProviderNativeClient } = {};
    await ConnectorService.initClient(
      ProviderConfig,
      target as HotMeshEngine | HotMeshWorker,
      'client',
    );
    return target.client;
  }

  /**
   * Initialize `store`, `stream`, and `subscription` clients for any provider.
   * @private
   */
  static async initClients(
    target: HotMeshEngine | HotMeshWorker,
  ): Promise<void> {
    let connections = target.connections;

    if (!connections) {
      const ProviderConfig = polyfill.providerConfig(target);
      connections = target.connections = {
        store: { ...ProviderConfig },
        stream: { ...ProviderConfig },
        sub: { ...ProviderConfig },
      };
    }
    // Expanded form
    if (connections.store) {
      await ConnectorService.initClient(connections.store, target, 'store');
    }
    if (connections.stream) {
      await ConnectorService.initClient(connections.stream, target, 'stream');
    }
    if (connections.sub) {
      await ConnectorService.initClient(connections.sub, target, 'sub');
      // use store for publishing events if same as subscription
      // if (connections.sub.class === connections.store.class) {
      //   connections.pub = {
      //     class: connections.store.class,
      //     options: { ...connections.store.options },
      //     provider: connections.store.provider,
      //   };
      //   target.pub = target.store;
      // } else {
        //initialize a separate client for publishing events
        connections.pub = {
          class: connections.sub.class,
          options: { ...connections.sub.options },
          provider: connections.sub.provider,
        };
        await ConnectorService.initClient(connections.pub, target, 'pub');
      // }
    }
    // TODO: add search after refactoring
  }

  /**
   * Binds a provider client native instance to the target object.
   * @private
   */
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
    const providerName = ProviderConfig.provider || identifyProvider(providerClass); //e.g. 'postgres.poolclient'
    const providerType = providerName.split('.')[0]; //e.g. 'postgres'

    let clientInstance: AbstractConnection<any, any>;
    const id = guid();

    switch (providerType) {
      case 'redis':
        clientInstance = await RedisConnection.connect(
          id,
          providerClass as RedisRedisClassType,
          options as RedisRedisClientOptions,
          { provider: providerName },
        );
        break;
      case 'ioredis':
        clientInstance = await IORedisConnection.connect(
          id,
          providerClass as IORedisClassType,
          options as IORedisClientOptions,
          { provider: providerName },
        );
        break;
      case 'nats':
        clientInstance = await NatsConnection.connect(
          id,
          providerClass as NatsClassType,
          options as NatsClientOptions,
          { provider: providerName },
        );
        break;
      case 'postgres':
        //if connecting as a poolClient for subscription, auto connect the client
        const bAutoConnect = field === 'sub';
        clientInstance = await PostgresConnection.connect(
          id,
          providerClass as PostgresClassType,
          options as PostgresClientOptions,
          { connect: bAutoConnect, provider: providerName },
        );        
        break;
      default:
        throw new Error(`Unknown provider type: ${providerType}`);
    }
    // Bind the resolved client instance to the target object
    target[field] = clientInstance.getClient();
  }
}
