import {
  RedisClassType,
  RedisClientType as RCT,
  RedisMultiType as RMT,
  RedisClientOptions as RCO
} from './redisclient';
import {
  RedisClassType as IORRedisClassType,
  RedisClientType as IORCT,
  RedisMultiType as IORMT,
  RedisClientOptions as IORCO
} from './ioredisclient';

type RedisClass = RedisClassType | IORRedisClassType;
type RedisClient = RCT | IORCT;
type RedisMulti = RMT | IORMT;
type RedisOptions = RCO | IORCO;

type MultiResponseFlags = (string | number)[]; // e.g., [3, 2, '0']

export {
  RedisClass,
  RedisClient,
  RedisMulti,
  RedisOptions,
  MultiResponseFlags,
}
