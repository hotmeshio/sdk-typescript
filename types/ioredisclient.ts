import { Redis, ChainableCommander, RedisOptions } from 'ioredis';

type RedisClassType = { new (options: RedisOptions): Redis };

export {
  RedisClassType,
  Redis as RedisClientType,
  ChainableCommander as RedisMultiType,
  RedisOptions as RedisClientOptions,
}
