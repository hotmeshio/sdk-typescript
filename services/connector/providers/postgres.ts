import { AbstractConnection } from '..';
import {
  PostgresClientOptions,
  PostgresClientType,
  PostgresClassType,
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

  async createConnection(
    clientConstructor: any,
    options: PostgresClientOptions,
  ): Promise<PostgresClientType> {
    const connection = new clientConstructor(options);
    try {
      await connection.connect();
      await connection.query('SELECT 1');
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
    return this.connection;
  }

  async closeConnection(connection: PostgresClientType): Promise<void> {
    if (!connection) {
      PostgresConnection.logger.warn(`postgres-not-connected-cannot-close`);
      return;
    }
    await connection.end();
  }
}

export { PostgresConnection };
