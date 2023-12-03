import * as Redis from 'redis';

import config from '../../../$setup/config'
import { Durable } from '../../../../services/durable';

export class RedisOSTest extends Durable.RedisOS {

  redisClass = Redis;

  redisOptions = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };
}
