import { Redis, RedisOptions as RedisClientOptions } from 'ioredis';
import config from '../config';
import { 
  IORedisClassType,
  IORedisClientType,
  IORedisClientType as RedisClientType,
  IORedisMultiType as RedisMultiType } from '../../../types/redis';

class RedisConnection {
  private connection: any | null = null;
  private static instances: Map<string, RedisConnection> = new Map();
  private id: string | null = null;

  private static clientOptions: RedisClientOptions = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };

  private async createConnection(options: RedisClientOptions): Promise<Redis> {
    return new Promise((resolve) => {
      resolve(new Redis(options));
    });
  }

  public async getClient(): Promise<IORedisClientType> {
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

  public static async getConnection(id: string, options?: Partial<RedisClientOptions>): Promise<RedisConnection> {
    if (this.instances.has(id)) {
      return this.instances.get(id) as RedisConnection;
    }
    const instance = new RedisConnection();
    const mergedOptions = { ...this.clientOptions, ...options };
    instance.connection = await instance.createConnection(mergedOptions);
    instance.id = id;
    this.instances.set(id, instance);
    return instance;
  }

  public static async disconnectAll(): Promise<void> {
    await Promise.all(Array.from(this.instances.values()).map((instance) => instance.disconnect()));
    this.instances.clear();
  }
}

export { RedisConnection, RedisClientType, RedisMultiType };
