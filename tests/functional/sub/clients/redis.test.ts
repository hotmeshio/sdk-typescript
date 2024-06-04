import { KeyType, HMNS } from '../../../../modules/key';
import { LoggerService } from '../../../../services/logger';
import { RedisStoreService } from '../../../../services/store/clients/redis';
import { RedisSubService } from '../../../../services/sub/clients/redis';
import { SubscriptionCallback } from '../../../../types/quorum';
import { RedisConnection, RedisClientType } from '../../../$setup/cache/redis';
import { sleepFor } from '../../../../modules/utils';

describe('FUNCTIONAL | RedisSubService', () => {
  const appConfig = { id: 'test-app', version: '1' };
  const engineId = '9876543210';
  let redisConnection: RedisConnection;
  let redisPublisherConnection: RedisConnection;
  let redisClient: RedisClientType;
  let redisPublisherClient: RedisClientType;
  let redisSubService: RedisSubService;
  let redisPubService: RedisStoreService;

  beforeEach(async () => {
    redisSubService = new RedisSubService(redisClient);
  });

  beforeAll(async () => {
    redisConnection = await RedisConnection.getConnection('test-connection-1');
    redisPublisherConnection =
      await RedisConnection.getConnection('test-publisher-1');
    redisClient = await redisConnection.getClient();
    redisPublisherClient = await redisPublisherConnection.getClient();
    await redisPublisherClient.flushDb();
    redisPubService = new RedisStoreService(redisPublisherClient);
    await redisPubService.init(HMNS, appConfig.id, new LoggerService());
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('subscribe/unsubscribe', () => {
    it('subscribes during initialization', async () => {
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = redisSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
      };
      await redisSubService.init(
        HMNS,
        appConfig.id,
        engineId,
        new LoggerService(),
      );
      const payload = { any: 'data' };
      await redisSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
      );
      await redisPubService.publish(KeyType.QUORUM, payload, appConfig.id);
    });

    it('unsubscribes and subscribes', async () => {
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = redisSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
      };
      await redisSubService.init(
        HMNS,
        appConfig.id,
        engineId,
        new LoggerService(),
      );
      const payload = { any: 'data' };
      await redisSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
      );
      const pub = await redisPubService.publish(
        KeyType.QUORUM,
        payload,
        appConfig.id,
      );
      expect(pub).toBeTruthy();
    });
  });

  describe('psubscribe/punsubscribe', () => {
    it('psubscribes and punsubscribes', async () => {
      const payload = { any: 'data' };
      let responded = false;
      const word = 'dog';
      const wild = 'd*g';
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = redisSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
          engineId: word,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
        responded = true;
      };
      await redisSubService.init(
        HMNS,
        appConfig.id,
        engineId,
        new LoggerService(),
      );
      await redisSubService.psubscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
        wild,
      );
      await redisPubService.publish(
        KeyType.QUORUM,
        payload,
        appConfig.id,
        word,
      );
      sleepFor(250); //give time to run
      await redisSubService.punsubscribe(KeyType.QUORUM, appConfig.id, wild);
      expect(responded).toBeTruthy();
    });
  });
});
