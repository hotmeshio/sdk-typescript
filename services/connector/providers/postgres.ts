import { AbstractConnection } from '..';
import {
  PostgresClientOptions,
  PostgresClientType,
  PostgresClassType,
  PostgresPoolClientType,
} from '../../../types/postgres';
import { guid, hashOptions } from '../../../modules/utils';
import {
  HMSH_PG_CONNECT_TIMEOUT_MS,
  HMSH_PG_KEEPALIVE_DELAY_MS,
  HMSH_PG_RESILIENT,
} from '../../../modules/enums';

import { ResilientPostgresClient } from './postgres-resilient-client';

/**
 * Connection defaults HotMesh applies to the clients it creates. Only
 * keys the caller left unset are filled; an explicit caller value
 * (including `connectionString` parameters) always wins.
 */
export function withConnectionDefaults(
  options: PostgresClientOptions,
): PostgresClientOptions {
  const resolved: PostgresClientOptions = { ...options };
  if (
    resolved.connectionTimeoutMillis === undefined &&
    HMSH_PG_CONNECT_TIMEOUT_MS > 0
  ) {
    resolved.connectionTimeoutMillis = HMSH_PG_CONNECT_TIMEOUT_MS;
  }
  if (resolved.keepAlive === undefined) {
    resolved.keepAlive = true;
  }
  if (resolved.keepAliveInitialDelayMillis === undefined) {
    resolved.keepAliveInitialDelayMillis = HMSH_PG_KEEPALIVE_DELAY_MS;
  }
  if (resolved.application_name === undefined) {
    resolved.application_name = 'hotmesh';
  }
  return resolved;
}

/** Attach a logging `'error'` listener to an emitter that has none. */
function guardErrors(target: any, label: string): void {
  if (
    target &&
    typeof target.on === 'function' &&
    typeof target.listenerCount === 'function' &&
    target.listenerCount('error') === 0
  ) {
    target.on('error', (error: Error) => {
      AbstractConnection.logger.warn(label, {
        error: error?.message,
        code: (error as any)?.code,
      });
    });
  }
}

class PostgresConnection extends AbstractConnection<
  PostgresClassType,
  PostgresClientOptions
> {
  defaultOptions: PostgresClientOptions = {
    host: 'postgres',
    port: 5432,
    user: 'postgres',
    password: 'password',
    database: 'hotmesh',
    max: 20,
    idleTimeoutMillis: 30_000,
  };

  //statically track all clients (//call 'release')
  protected static poolClientInstances: Set<PostgresPoolClientType> = new Set();

  //statically track all connections (//call 'end')
  protected static connectionInstances: Set<PostgresClientType> = new Set();

  //track connections by taskQueue + database config for reuse
  protected static taskQueueConnections: Map<string, PostgresConnection> =
    new Map();

  /**
   * Get comprehensive connection statistics for monitoring taskQueue pooling effectiveness
   */
  static getConnectionStats(): {
    totalPoolClients: number;
    totalConnections: number;
    taskQueueConnections: number;
    taskQueueDetails: Array<{
      key: string;
      connectionId: string;
      reusedCount: number;
    }>;
  } {
    const taskQueueDetails = Array.from(
      this.taskQueueConnections.entries(),
    ).map(([key, connection]) => ({
      key,
      connectionId: connection.getConnectionId() || 'unknown',
      reusedCount: (connection as any).reusedCount || 0,
    }));

    return {
      totalPoolClients: this.poolClientInstances.size,
      totalConnections: this.connectionInstances.size,
      taskQueueConnections: this.taskQueueConnections.size,
      taskQueueDetails,
    };
  }

  /**
   * Log current connection statistics - useful for debugging connection pooling
   */
  static logConnectionStats(logger?: any): void {
    const stats = this.getConnectionStats();
    const message = `PostgreSQL Connection Stats: ${stats.totalConnections} total connections, ${stats.taskQueueConnections} taskQueue pools, ${stats.totalPoolClients} pool clients`;

    if (logger) {
      logger.info('postgres-connection-stats', {
        ...stats,
        message,
      });
    } else {
      console.log(message, stats);
    }
  }

  /**
   * Check taskQueue pooling effectiveness - returns metrics about connection reuse
   */
  static getPoolingEffectiveness(): {
    totalConnections: number;
    taskQueuePools: number;
    totalReuses: number;
    averageReusesPerPool: number;
    poolingEfficiency: number; // percentage of connections that are pooled
  } {
    const stats = this.getConnectionStats();
    const totalReuses = stats.taskQueueDetails.reduce(
      (sum, detail) => sum + detail.reusedCount,
      0,
    );
    const averageReusesPerPool =
      stats.taskQueueConnections > 0
        ? totalReuses / stats.taskQueueConnections
        : 0;
    const poolingEfficiency =
      stats.totalConnections > 0
        ? stats.taskQueueConnections / stats.totalConnections * 100
        : 0;

    return {
      totalConnections: stats.totalConnections,
      taskQueuePools: stats.taskQueueConnections,
      totalReuses,
      averageReusesPerPool: Math.round(averageReusesPerPool * 100) / 100,
      poolingEfficiency: Math.round(poolingEfficiency * 100) / 100,
    };
  }

  //the specific connection instance
  poolClientInstance: PostgresPoolClientType;

  async createConnection(
    clientConstructor: any,
    options: PostgresClientOptions,
    config: { connect?: boolean; provider?: string } = {},
  ): Promise<PostgresClientType> {
    try {
      let connection:
        | PostgresClientType
        | PostgresPoolClientType
        | PostgresClassType;
      if (
        config.provider === 'postgres.poolclient' ||
        PostgresConnection.isPoolClient(clientConstructor)
      ) {
        // It's a PoolClient
        connection = clientConstructor as PostgresPoolClientType;
        //an idle pooled client that loses its socket is re-emitted on the
        //pool; never let that reach an unhandled 'error' event
        guardErrors(clientConstructor, 'postgres-pool-client-lost');
        if (config.connect) {
          const client = await clientConstructor.connect();
          //a checked-out client has no pool listener while it is held
          guardErrors(client, 'postgres-pool-held-client-lost');
          //register the connection singularly to be 'released' later
          PostgresConnection.poolClientInstances.add(client);
          this.poolClientInstance = client;
        }
      } else if (HMSH_PG_RESILIENT) {
        // It's a Client: wrap it so the session survives restarts
        const resilient = new ResilientPostgresClient(
          clientConstructor as PostgresClassType,
          withConnectionDefaults(options),
          this.id ?? guid(),
          PostgresConnection.logger,
        );
        await resilient.connect();
        connection = resilient as unknown as PostgresClientType;
      } else {
        // It's a Client
        connection = new (clientConstructor as PostgresClassType)(options);
        await connection.connect();
        await connection.query('SELECT 1');
      }

      //register the connection statically to be 'ended' later
      PostgresConnection.connectionInstances.add(connection);
      return connection;
    } catch (error) {
      PostgresConnection.logger.error(`postgres-provider-connection-failed`, {
        host: options.host ?? 'unknown',
        database: options.database ?? 'unknown',
        port: options.port ?? 'unknown',
        error,
      });
      throw new Error(`postgres-provider-connection-failed: ${error.message}`);
    }
  }

  getClient(): PostgresClientType {
    if (!this.connection) {
      throw new Error('Postgres client is not connected');
    }
    return this.poolClientInstance || this.connection;
  }

  /**
   * Get the connection ID for monitoring purposes
   */
  getConnectionId(): string | null {
    return this.id;
  }

  public static async disconnectAll(): Promise<void> {
    //log stats
    //this.logConnectionStats();
    if (!this.disconnecting) {
      this.disconnecting = true;
      try {
        // Clear the reuse caches first so no caller is handed a closing connection
        this.taskQueueConnections.clear();
        this.instances.clear();
        await this.disconnectPoolClients();
        await this.disconnectConnections();
      } finally {
        this.disconnecting = false;
      }
    }
  }

  public static async disconnectPoolClients(): Promise<void> {
    const clients = Array.from(this.poolClientInstances.values());
    this.poolClientInstances.clear();
    for (const instance of clients) {
      try {
        instance.release();
      } catch (error) {
        this.logger.debug('postgres-pool-client-release-error', { error });
      }
    }
  }

  public static async disconnectConnections(): Promise<void> {
    const connections = Array.from(this.connectionInstances.values());
    this.connectionInstances.clear();
    await Promise.allSettled(
      connections.map((instance) =>
        Promise.resolve().then(() => instance.end()),
      ),
    );
  }

  async closeConnection(connection: PostgresClientType): Promise<void> {
    //no-op (handled by disconnectAll)
  }

  public static isPoolClient(client: any): client is PostgresPoolClientType {
    return !(isNaN(client?.totalCount) && isNaN(client?.idleCount));
  }

  /**
   * Creates a taskQueue-based connection key for connection pooling.
   * This allows multiple providers (store, sub, stream) to reuse the same connection
   * when they share the same taskQueue and database configuration.
   */
  private static createTaskQueueConnectionKey(
    taskQueue: string | undefined,
    options: PostgresClientOptions,
    provider?: string,
  ): string {
    const configHash = hashOptions(options);
    const providerType = provider?.split('.')[0] || 'postgres';
    return `${providerType}:${taskQueue || 'default'}:${configHash}`;
  }

  /**
   * Gets or creates a PostgreSQL connection based on taskQueue and database configuration.
   * If a connection already exists for the same taskQueue + config, it will be reused.
   * This optimization reduces connection overhead for PostgreSQL providers.
   */
  public static async getOrCreateTaskQueueConnection(
    id: string,
    taskQueue: string | undefined,
    clientConstructor: PostgresClassType,
    options: PostgresClientOptions,
    config: { connect?: boolean; provider?: string } = {},
  ): Promise<PostgresConnection> {
    // Only use taskQueue pooling for PostgreSQL providers
    if (!taskQueue || !config.provider?.startsWith('postgres')) {
      return await this.connect(id, clientConstructor, options, config);
    }

    const connectionKey = this.createTaskQueueConnectionKey(
      taskQueue,
      options,
      config.provider,
    );

    // Check if we already have a connection for this taskQueue + config
    if (this.taskQueueConnections.has(connectionKey)) {
      const existingConnection = this.taskQueueConnections.get(connectionKey)!;

      // Track reuse count for monitoring
      (existingConnection as any).reusedCount =
        ((existingConnection as any).reusedCount || 0) + 1;

      this.logger.debug('postgres-connection-reused', {
        connectionKey,
        taskQueue,
        originalId: existingConnection.id,
        newId: id,
        reuseCount: (existingConnection as any).reusedCount,
      });
      return existingConnection;
    }

    // Create new connection and cache it for the taskQueue
    const newConnection = await this.connect(
      id,
      clientConstructor,
      options,
      config,
    );

    // Initialize reuse tracking
    (newConnection as any).reusedCount = 0;

    this.taskQueueConnections.set(connectionKey, newConnection);

    this.logger.debug('postgres-connection-created-for-taskqueue', {
      connectionKey,
      taskQueue,
      connectionId: id,
    });

    return newConnection;
  }

  static async getTransactionClient(
    transactionClient: any,
  ): Promise<['client' | 'poolclient', PostgresClientType]> {
    let client: PostgresClientType;
    let type: 'client' | 'poolclient';
    if (PostgresConnection.isPoolClient(transactionClient)) {
      type = 'poolclient';
      client = await (transactionClient as PostgresPoolClientType).connect();
    } else {
      type = 'client';
      client = transactionClient as PostgresClientType;
    }
    return [type, client];
  }
}

export { PostgresConnection };
