import { AbstractConnection } from '..';
import {
  RedisRedisClassType as RedisClassType,
  RedisRedisClientType as RedisClientType,
  RedisRedisClientOptions as RedisClientOptions,
} from '../../../types/redis';

class RedisConnection extends AbstractConnection<
  RedisClassType,
  RedisClientOptions
> {

  defaultOptions: RedisClientOptions = {
    socket: {
      host: 'localhost',
      port: 6379,
      tls: false,
    },
  };

  async createConnection(
    Redis: Partial<RedisClassType>,
    options: RedisClientOptions,
  ): Promise<Partial<RedisClientType>> {
    return new Promise((resolve, reject) => {
      const client = Redis.createClient(
        options,
      ) as unknown as RedisClientType;

      client.on('error', (error: any) => {
        reject(error);
      });

      client.on('ready', () => {
        resolve(client);
      });

      client.connect();
    });
  }

  getClient(): RedisClientType {
    if (!this.connection) {
      throw new Error('Redis client is not connected');
    }
    return this.connection;
  }

  async closeConnection(connection: any): Promise<void> {
    await connection.quit();
  }
}

export { RedisConnection, RedisClientType };
