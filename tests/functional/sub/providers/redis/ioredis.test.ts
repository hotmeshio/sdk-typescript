import { KeyType, HMNS } from '../../../../../modules/key';
import { LoggerService } from '../../../../../services/logger';
import { IORedisSubService } from '../../../../../services/sub/providers/redis/ioredis';
import { SubscriptionCallback } from '../../../../../types/quorum';
import {
  RedisConnection,
  RedisClientType,
} from '../../../../$setup/cache/ioredis';
import { sleepFor } from '../../../../../modules/utils';

describe('FUNCTIONAL | IORedisSubService', () => {
  const appConfig = { id: 'test-app', version: '1' };
  const engineId = '9876543210';
  let redisConnection: RedisConnection;
  let redisStoreConnection: RedisConnection;
  let redisClient: RedisClientType;
  let redisStoreClient: RedisClientType;
  let redisSubService: IORedisSubService;

  beforeEach(async () => {
    redisSubService = new IORedisSubService(redisClient, redisStoreClient);
  });

  beforeAll(async () => {
    redisConnection = await RedisConnection.getConnection('ts-1');
    redisStoreConnection = await RedisConnection.getConnection('tp-1');
    redisClient = await redisConnection.getClient();
    redisStoreClient = await redisStoreConnection.getClient();
    await redisStoreClient.flushdb();
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('subscribe/unsubscribe', () => {
    it('subscribes and unsubscribes for an app', async () => {
      let responded = false;
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = redisSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
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
      const payload = { any: 'data' };
      await redisSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
      );
      await redisSubService.publish(KeyType.QUORUM, payload, appConfig.id);
      sleepFor(250); //give time to run
      await redisSubService.unsubscribe(KeyType.QUORUM, appConfig.id);
      expect(responded).toBeTruthy();
    });

    it('unsubscribes and subscribes for an app engine target', async () => {
      const engineId = 'cat';
      let responded = false;
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = redisSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
          engineId,
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
      const payload = { any: 'data' };
      await redisSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
        engineId,
      );
      await redisSubService.publish(
        KeyType.QUORUM,
        payload,
        appConfig.id,
        engineId,
      );
      sleepFor(250); //give time to run
      await redisSubService.unsubscribe(KeyType.QUORUM, appConfig.id, engineId);
      expect(responded).toBeTruthy();
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
      await redisSubService.publish(
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
