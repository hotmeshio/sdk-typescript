import { KeyType, HMNS } from '../../../../../modules/key';
import { LoggerService } from '../../../../../services/logger';
import { IORedisSubService } from '../../../../../services/sub/providers/redis/ioredis';
import { IORedisStoreService } from '../../../../../services/store/providers/redis/ioredis';
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
  let redisStoreService: IORedisStoreService;

  beforeEach(async () => {
    redisSubService = new IORedisSubService(redisClient, redisStoreClient);
    redisStoreService = new IORedisStoreService(redisStoreClient);
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
      await sleepFor(250); //give time to run
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
      await sleepFor(250); //give time to run
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
      await sleepFor(250); //give time to run
      await redisSubService.punsubscribe(KeyType.QUORUM, appConfig.id, wild);
      expect(responded).toBeTruthy();
    });
  });

  describe('publish with transaction', () => {
    it('publishes messages using a transaction', async () => {
      const txAppId = 'test-app-tx1';
      let responded = false;
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = redisSubService.mintKey(KeyType.QUORUM, {
          appId: txAppId,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
        responded = true;
      };
      
      await redisSubService.init(
        HMNS,
        txAppId,
        engineId,
        new LoggerService(),
      );
      await redisStoreService.init(
        HMNS,
        txAppId,
        new LoggerService(),
      );
      
      const payload = { any: 'data', timestamp: Date.now() };
      
      // Subscribe first
      await redisSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        txAppId,
      );
      
      // Create a transaction and publish within it
      const multi = redisStoreService.transact();
      await redisSubService.publish(
        KeyType.QUORUM,
        payload,
        txAppId,
        undefined,
        multi,
      );
      
      // Execute the transaction
      const results = await multi.exec();
      expect(results).toBeDefined();
      
      // Give time for message to be received
      await sleepFor(250);
      
      await redisSubService.unsubscribe(KeyType.QUORUM, txAppId);
      expect(responded).toBeTruthy();
    });

    it('publishes multiple messages in a single transaction', async () => {
      const txAppId = 'test-app-tx2';
      const receivedMessages: any[] = [];
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        receivedMessages.push(message);
      };
      
      await redisSubService.init(
        HMNS,
        txAppId,
        engineId,
        new LoggerService(),
      );
      await redisStoreService.init(
        HMNS,
        txAppId,
        new LoggerService(),
      );
      
      // Subscribe first
      await redisSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        txAppId,
      );
      
      // Create a transaction and publish multiple messages
      const multi = redisStoreService.transact();
      const payload1 = { message: 'first', timestamp: Date.now() };
      const payload2 = { message: 'second', timestamp: Date.now() + 1 };
      const payload3 = { message: 'third', timestamp: Date.now() + 2 };
      
      await redisSubService.publish(
        KeyType.QUORUM,
        payload1,
        txAppId,
        undefined,
        multi,
      );
      await redisSubService.publish(
        KeyType.QUORUM,
        payload2,
        txAppId,
        undefined,
        multi,
      );
      await redisSubService.publish(
        KeyType.QUORUM,
        payload3,
        txAppId,
        undefined,
        multi,
      );
      
      // Execute the transaction
      const results = await multi.exec();
      expect(results).toBeDefined();
      expect(results.length).toBeGreaterThanOrEqual(3);
      
      // Give time for messages to be received
      await sleepFor(500);
      
      await redisSubService.unsubscribe(KeyType.QUORUM, txAppId);
      
      // Should have received all 3 messages
      expect(receivedMessages.length).toBe(3);
      expect(receivedMessages).toContainEqual(payload1);
      expect(receivedMessages).toContainEqual(payload2);
      expect(receivedMessages).toContainEqual(payload3);
    });
  });
});
