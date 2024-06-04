import { RedisConnection } from '../../$setup/cache/redis';

describe('FUNCTIONAL | RedisConnection', () => {
  afterEach(async () => {
    await RedisConnection.disconnectAll();
  });

  it('should create a connection to Redis', async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection');

    expect(redisConnection).toBeInstanceOf(RedisConnection);
    expect(await redisConnection.getClient()).not.toBeNull();
  });

  it('should throw an error when trying to get a client before connecting', async () => {
    try {
      await RedisConnection.getConnection('test-connection', {
        password: 'bad_password',
      });
    } catch (error) {
      return expect(error.message).not.toBeNull();
    }
    throw new Error('Expected an error to be thrown');
  });

  it('should disconnect from Redis', async () => {
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

  it('should set and get a value from Redis', async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection');
    const redisClient = await redisConnection.getClient();

    await redisClient.set('test-key', 'test-value');
    const val = await redisClient.get('test-key');
    expect(val).toBe('test-value');
  });

  it('publishes and subscribes', async () => {
    const publisher = await RedisConnection.getConnection('publisher');
    const subscriber = await RedisConnection.getConnection('subscriber');
    const publisherClient = await publisher.getClient();
    const subscriberClient = await subscriber.getClient();
    await subscriberClient.subscribe('article', (message) => {
      expect(message).toBe('message');
    });
    await publisherClient.publish('article', 'message');
  });
});
