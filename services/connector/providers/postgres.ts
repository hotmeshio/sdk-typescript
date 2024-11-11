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
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'hotmesh',
    database: 'postgres',
    max: 20,
    idleTimeoutMillis: 30000,
  };

  async createConnection(
    Pool: PostgresClassType,
    options: PostgresClientOptions,
  ): Promise<PostgresClientType> {
    const connection = new Pool(options);
    // Sanity check
    try {
      await connection.query('SELECT 1');
      return connection;
    } catch (error) {
      console.error(`Failed to connect to PostgreSQL: ${error.message}`);
      throw new Error(`Failed to connect to PostgreSQL: ${error.message}`);
    }
  }

  getClient(): PostgresClientType {
    if (!this.connection) {
      throw new Error('Postgres client is not connected');
    }
    return this.connection;
  }

  closeConnection(connection: PostgresClientType): Promise<void> {
    return connection.end();
  }
}

export { PostgresConnection };
