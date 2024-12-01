import { AbstractConnection } from '..';
import {
  PostgresClientOptions,
  PostgresClientType,
  PostgresClassType,
  PostgresPoolClientType,
} from '../../../types/postgres';

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

  protected static poolClientInstances: Set<PostgresPoolClientType> = new Set(); //call 'release'
  protected static connectionInstances: Set<PostgresClientType> = new Set(); //call 'end'
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
        if (config.connect) {
          const client = await clientConstructor.connect();
          //register the connection statically to be 'released' later
          PostgresConnection.poolClientInstances.add(client);
          this.poolClientInstance = client;
        }
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

  public static async disconnectAll(): Promise<void> {
    if (!this.disconnecting) {
      this.disconnecting = true;
      await this.disconnectPoolClients();
      await this.disconnectConnections();
      this.disconnecting = false;
    }
  }

  public static async disconnectPoolClients(): Promise<void> {
    Array.from(this.poolClientInstances.values()).map((instance) => {
      instance.release();
    });
    this.poolClientInstances.clear();
  }

  public static async disconnectConnections(): Promise<void> {
    Array.from(this.connectionInstances.values()).map((instance) => {
      instance.end();
    });
    this.connectionInstances.clear();
  }

  async closeConnection(connection: PostgresClientType): Promise<void> {
    //no-op (handled by disconnectAll)
  }

  public static isPoolClient(client: any): client is PostgresPoolClientType {
    return !(isNaN(client?.totalCount) && isNaN(client?.idleCount));
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
