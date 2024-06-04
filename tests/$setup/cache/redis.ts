import { createClient, RedisClientOptions } from 'redis';

import config from '../config';
import {
  RedisRedisClientType as RedisClientType,
  RedisRedisMultiType as RedisMultiType,
} from '../../../types/redis';

class RedisConnection {
  private connection: RedisClientType | null = null;
  private static instances: Map<string, RedisConnection> = new Map();
  private id: string | null = null;

  private static clientOptions: RedisClientOptions = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };

  private async createConnection(
    options: RedisClientOptions,
  ): Promise<RedisClientType> {
    return new Promise((resolve, reject) => {
      const client = createClient(options);

      client.on('error', (error) => {
        reject(error);
      });

      client.on('ready', () => {
        resolve(client as unknown as RedisClientType);
      });

      client.connect();
    });
  }

  public async getClient(): Promise<RedisClientType> {
    if (!this.connection) {
      throw new Error('Redis client is not connected');
    }

    return this.connection;
  }

  public async disconnect(): Promise<void> {
    if (this.connection) {
      await this.connection.quit();
      this.connection = null;
    }

    if (this.id) {
      RedisConnection.instances.delete(this.id);
    }
  }

  public static async getConnection(
    id: string,
    options?: Partial<RedisClientOptions>,
  ): Promise<RedisConnection> {
    if (this.instances.has(id)) {
      return this.instances.get(id)!;
    }

    const instance = new RedisConnection();
    const mergedOptions = { ...this.clientOptions, ...options };
    instance.connection = await instance.createConnection(mergedOptions);
    instance.id = id;
    this.instances.set(id, instance);
    return instance;
  }

  public static async disconnectAll(): Promise<void> {
    await Promise.all(
      Array.from(this.instances.values()).map((instance) =>
        instance.disconnect(),
      ),
    );
    this.instances.clear();
  }
}

export { RedisConnection, RedisClientType, RedisMultiType };
