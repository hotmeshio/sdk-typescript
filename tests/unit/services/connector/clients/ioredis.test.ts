import Redis from 'ioredis';
import config from '../../../../$setup/config';
import { RedisConnection } from '../../../../../services/connector/clients/ioredis'; 
import { RedisClientOptions, RedisClientType } from '../../../../../types/ioredisclient';

describe('RedisConnection', () => {
  let redisConnection: RedisConnection;
  let redisClient: RedisClientType;

  const options: RedisClientOptions = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };

  // Connect to Redis before running tests
  beforeAll(async () => {
    redisConnection = await RedisConnection.connect('testId', Redis, options);
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
    await redisClient.set('testKey', 'ioredis testValue');
    const value = await redisClient.get('testKey');
    expect(value).toBe('ioredis testValue');
  });

  it('should throw an error if getClient is called without a connection', async () => {
    await redisConnection.disconnect();
    expect(() => redisConnection.getClient()).toThrow('Redis client is not connected');
  });
});
