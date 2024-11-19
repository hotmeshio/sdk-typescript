import { RedisConnection } from '../../$setup/cache/ioredis';

describe('FUNCTIONAL | IORedisConnection', () => {
  afterEach(async () => {
    await RedisConnection.disconnectAll();
  });

  it('should create a connection to Redis', async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection');

    expect(redisConnection).toBeInstanceOf(RedisConnection);
    expect(await redisConnection.getClient()).not.toBeNull();
  });

  it('should disconnect from the provider client', async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection');
    await redisConnection.disconnect();

    expect(redisConnection.getClient()).rejects.toThrow(
      'Redis client is not connected',
    );
  });

  it('should disconnect all instances', async () => {
    await RedisConnection.getConnection('test-connection-1');
    await RedisConnection.getConnection('test-connection-2');
    await RedisConnection.disconnectAll();

    expect(RedisConnection['instances'].size).toBe(0);
  });

  it('should set and get a value from the provider client', async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection');
    const redisClient = await redisConnection.getClient();

    await redisClient.set('test-key', 'test-value');
    const val = await redisClient.get('test-key');
    expect(val).toBe('test-value');
  });

  it('should set and get a hash in Redis', async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection');
    const redisClient = await redisConnection.getClient();
    const obj = { a: 'b' };
    await redisClient.hset('test-hash', obj);
    const val = await redisClient.hgetall('test-hash');
    expect(val).toEqual(obj);
  });

  it('publishes and subscribes', async () => {
    const publisher = await RedisConnection.getConnection('publisher');
    const subscriber = await RedisConnection.getConnection('subscriber');
    const publisherClient = await publisher.getClient();
    const subscriberClient = await subscriber.getClient();
    await subscriberClient.subscribe('article', () => {});
    subscriberClient.on('message', (_, message) => {
      expect(message).toBe('message');
    });
    await publisherClient.publish('article', 'message');
  });
});
