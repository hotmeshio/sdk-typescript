import { createClient, RedisClientOptions } from 'redis';

type RedisClientType = ReturnType<typeof createClient>;

interface RedisMultiType {
  XADD(key: string, id: string, fields: any): this;
  XACK(key: string, group: string, id: string): this;
  XDEL(key: string, id: string): this;
  HDEL(key: string, itemId: string): this;
  HGET(key: string, itemId: string): this;
  HGETALL(key: string): this;
  HINCRBYFLOAT(key: string, itemId: string, value: number): this;
  HMGET(key: string, itemIds: string[]): this;
  HSET(key: string, values: Record<string, string>): this;
  LRANGE(key: string, start: number, end: number): this;
  RPUSH(key: string, value: string): this;
  ZADD(key: string, values: { score: string, value: string }): this;
  sendCommand(command: string[]): Promise<any>;
  exec: () => Promise<unknown[]>;
}

type RedisClassType = { createClient: (options: RedisClientOptions) => any };

export {
  RedisClassType,
  RedisClientType,
  RedisMultiType,
  RedisClientOptions,
}