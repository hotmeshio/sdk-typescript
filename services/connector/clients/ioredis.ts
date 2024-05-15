import {
  IORedisClientOptions as RedisClientOptions,
  IORedisClassType as RedisClassType,
  IORedisClientType as RedisClientType } from '../../../types/redis';

class RedisConnection {
  private connection: any | null = null;
  private static instances: Map<string, RedisConnection> = new Map();
  private id: string | null = null;

  private static clientOptions: RedisClientOptions = {
    host: 'localhost',
    port: 6379,
    //password: config.REDIS_PASSWORD,
    //db: config.REDIS_DATABASE,
  };

  private async createConnection(Redis: RedisClassType, options: RedisClientOptions): Promise<any> {
    return new Redis(options);
  }

  public getClient(): RedisClientType {
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

  public static async connect(id: string, Redis: Partial<RedisClassType>, options?: RedisClientOptions): Promise<RedisConnection> {
    if (this.instances.has(id)) {
      return this.instances.get(id) as RedisConnection;
    }
    const instance = new RedisConnection();
    const opts = options ? { ...options } : { ...this.clientOptions };
    instance.connection = await instance.createConnection(Redis as RedisClassType, opts);
    instance.id = id;
    this.instances.set(id, instance);
    return instance;
  }

  public static async disconnectAll(): Promise<void> {
    await Promise.all(Array.from(this.instances.values()).map((instance) => instance.disconnect()));
    this.instances.clear();
  }
}

export { RedisConnection };
