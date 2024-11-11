import { AbstractConnection } from '..';
import {
  IORedisClientOptions as RedisClientOptions,
  IORedisClassType as RedisClassType,
  IORedisClientType as RedisClientType,
} from '../../../types/redis';

class RedisConnection extends AbstractConnection<
  RedisClassType,
  RedisClientOptions
> {
  defaultOptions: RedisClientOptions = {
    host: 'localhost',
    port: 6379,
  };

  async createConnection(
    Redis: RedisClassType,
    options: RedisClientOptions,
  ): Promise<RedisClientType> {
    return new Redis(options);
  }

  getClient(): RedisClientType {
    if (!this.connection) {
      throw new Error('Redis client is not connected');
    }
    return this.connection;
  }

  async closeConnection(connection: RedisClientType): Promise<void> {
    await connection.quit();
  }
}

export { RedisConnection };
