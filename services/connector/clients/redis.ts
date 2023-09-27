import {
  RedisClientType,
  RedisClientOptions,
  RedisClassType } from '../../../types/redisclient';

class RedisConnection {
  private connection: RedisClientType | null = null;
  private static instances: Map<string, RedisConnection> = new Map();
  private id: string | null = null;

  private static clientOptions: RedisClientOptions = {
    socket: {
      host: 'localhost',
      port: 6379,
      tls: false,
    },
    //password: config.REDIS_PASSWORD,
    //database: config.REDIS_DATABASE,
  };

  private async createConnection(Redis: RedisClassType, options: RedisClientOptions): Promise<RedisClientType> {
    return new Promise((resolve, reject) => {
      const client = Redis.createClient(options);

      client.on('error', (error: any) => {
        reject(error);
      });

      client.on('ready', () => {
        resolve(client);
      });

      client.connect();
    });
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

  public static async connect(id: string, Redis: RedisClassType, options?: RedisClientOptions): Promise<RedisConnection> {
    if (this.instances.has(id)) {
      return this.instances.get(id)!;
    }
    const instance = new RedisConnection();
    const opts = options ? { ...options } : { ...this.clientOptions };
    instance.connection = await instance.createConnection(Redis, opts);
    instance.id = id;
    this.instances.set(id, instance);
    return instance;
  }

  public static async disconnectAll(): Promise<void> {
    await Promise.all(Array.from(this.instances.values()).map((instance) => instance.disconnect()));
    this.instances.clear();
  }
}

export { RedisConnection, RedisClientType };
