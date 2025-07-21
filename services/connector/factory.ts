import { guid, identifyProvider, polyfill } from '../../modules/utils';
import { HotMeshEngine, HotMeshWorker } from '../../types/hotmesh';
import { ProviderConfig, ProviderNativeClient } from '../../types/provider';
import { NatsClassType, NatsClientOptions } from '../../types/nats';
import { PostgresClassType, PostgresClientOptions } from '../../types/postgres';

import { NatsConnection } from './providers/nats';
import { PostgresConnection } from './providers/postgres';

import { AbstractConnection } from './index';

export class ConnectorService {
  static async disconnectAll(): Promise<void> {
    await PostgresConnection.disconnectAll();
    await NatsConnection.disconnectAll();
  }

  /**
   * Connect to a provider (postgres, nats) and return the native
   * client. Connections are handled by the engine and worker routers at
   * initialization, but the factory method provided here is useful
   * for testing provider configurations.
   */
  static async connectClient(
    ProviderConfig: ProviderConfig,
    taskQueue?: string,
  ): Promise<ProviderNativeClient> {
    const target: { client?: ProviderNativeClient } = {};
    await ConnectorService.initClient(
      ProviderConfig,
      target as HotMeshEngine | HotMeshWorker,
      'client',
      taskQueue,
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
    let connection = polyfill.providerConfig(target);

    if (!('store' in connection)) {
      connection = target.connection = {
        ...connection,
        store: { ...connection },
        stream: { ...connection },
        sub: { ...connection },
      };
    }

    // Extract taskQueue from target for connection pooling
    const taskQueue = target.taskQueue;

    // Expanded form
    if (connection.store) {
      await ConnectorService.initClient(
        connection.store,
        target,
        'store',
        taskQueue,
      );
    }
    if (connection.stream) {
      await ConnectorService.initClient(
        connection.stream,
        target,
        'stream',
        taskQueue,
      );
    }
    if (connection.sub) {
      await ConnectorService.initClient(
        connection.sub,
        target,
        'sub',
        taskQueue,
      );
      // use store for publishing events if same as subscription
      if (connection.sub.class !== connection.store.class) {
        //initialize a separate client for publishing events, using
        //the same provider as the subscription client
        connection.pub = {
          class: connection.sub.class,
          options: { ...connection.sub.options },
          provider: connection.sub.provider,
        };
        await ConnectorService.initClient(
          connection.pub,
          target,
          'pub',
          taskQueue,
        );
      }
    }
  }

  /**
   * Binds a provider client native instance to the target object.
   * @private
   */
  static async initClient(
    ProviderConfig: ProviderConfig,
    target: HotMeshEngine | HotMeshWorker,
    field: string,
    taskQueue?: string,
  ) {
    if (target[field]) {
      return;
    }
    const providerClass = ProviderConfig.class;
    const options = ProviderConfig.options;
    const providerName =
      ProviderConfig.provider || identifyProvider(providerClass); //e.g. 'postgres.poolclient'
    const providerType = providerName.split('.')[0]; //e.g. 'postgres'

    let clientInstance: AbstractConnection<any, any>;
    const id = guid();

    switch (providerType) {
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
        // Use taskQueue-based connection pooling for PostgreSQL
        clientInstance =
          await PostgresConnection.getOrCreateTaskQueueConnection(
            id,
            taskQueue,
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
