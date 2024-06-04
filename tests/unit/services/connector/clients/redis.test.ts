import * as Redis from 'redis';

import config from '../../../../$setup/config';
import { RedisConnection } from '../../../../../services/connector/clients/redis';
import {
  RedisRedisClientOptions as RedisClientOptions,
  RedisRedisClientType as RedisClientType,
  RedisRedisClassType,
} from '../../../../../types/redis';

describe('RedisConnection', () => {
  let redisConnection: RedisConnection;
  let redisClient: RedisClientType;

  const options: RedisClientOptions = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };

  // Connect to Redis before running tests
  beforeAll(async () => {
    redisConnection = await RedisConnection.connect(
      'testId',
      Redis as unknown as RedisRedisClassType,
      options,
    );
    redisClient = redisConnection.getClient();
  });

  // Disconnect after all tests are done
  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  it('should connect to Redis', () => {
    expect(redisClient).toBeDefined();
  });

  it('should set and get value from Redis', async () => {
    await redisClient.set('testKey', 'redis testValue');
    const value = await redisClient.get('testKey');
    expect(value).toBe('redis testValue');
  });

  it('should throw an error if getClient is called without a connection', async () => {
    await redisConnection.disconnect();
    expect(() => redisConnection.getClient()).toThrow(
      'Redis client is not connected',
    );
  });
});
