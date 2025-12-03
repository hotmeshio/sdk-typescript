import { HMNS } from '../../../../../modules/key';
import { sleepFor } from '../../../../../modules/utils';
import { LoggerService } from '../../../../../services/logger';
import { IORedisStreamService } from '../../../../../services/stream/providers/redis/ioredis';
import {
  RedisConnection,
  RedisClientType,
} from '../../../../$setup/cache/ioredis';

describe('FUNCTIONAL | IORedisStreamService', () => {
  let redisClient: RedisClientType;
  let redisStoreClient: RedisClientType;
  let redisStreamService: IORedisStreamService;
  const TEST_STREAM = 'testStream';
  const TEST_GROUP = 'testGroup';
  const TEST_CONSUMER = 'testConsumer';

  const msg = (idx: number): string => {
    return JSON.stringify({ id: idx, data: `message${idx}` });
  };

  beforeEach(async () => {
    await redisClient.flushdb();
    redisStreamService = new IORedisStreamService(
      redisClient,
      redisStoreClient,
    );
    await redisStreamService.init(HMNS, 'APP_ID', new LoggerService());
  });

  beforeAll(async () => {
    const redisConnection =
      await RedisConnection.getConnection('test-connection-1');
    redisClient = await redisConnection.getClient();
    const redisStoreConnection =
      await RedisConnection.getConnection('test-connection-2');
    redisStoreClient = await redisStoreConnection.getClient();
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('Stream Operations', () => {
    it('should create and delete a stream', async () => {
      const created = await redisStreamService.createStream(TEST_STREAM);
      expect(created).toBe(true);

      const deleted = await redisStreamService.deleteStream(TEST_STREAM);
      expect(deleted).toBe(true);
    });

    it('should get stream stats', async () => {
      await redisStreamService.createStream(TEST_STREAM);
      const messages = [msg(1), msg(2), msg(3)];
      await redisStreamService.publishMessages(TEST_STREAM, messages);

      const stats = await redisStreamService.getStreamStats(TEST_STREAM);
      expect(stats.messageCount).toBe(3);
    });

    it('should get stream depth', async () => {
      await redisStreamService.createStream(TEST_STREAM);
      const messages = [msg(1), msg(2)];
      await redisStreamService.publishMessages(TEST_STREAM, messages);

      const depth = await redisStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(2);
    });

    it('should get stream depths', async () => {
      await redisStreamService.createStream(`${TEST_STREAM}1`);
      await redisStreamService.publishMessages(`${TEST_STREAM}1`, [
        msg(1),
        msg(2),
      ]);
      await redisStreamService.createStream(`${TEST_STREAM}2`);
      await redisStreamService.publishMessages(`${TEST_STREAM}2`, [msg(3)]);

      const depths = await redisStreamService.getStreamDepths([
        { stream: `${TEST_STREAM}1` },
        { stream: `${TEST_STREAM}2` },
        { stream: `${TEST_STREAM}2` }, //system de-dupes
      ]);
      expect(depths[0].depth).toBe(2);
      expect(depths[1].depth).toBe(1);
      expect(depths[2].depth).toBe(1);
    });
  });

  describe('Consumer Group Operations', () => {
    it('should create a consumer group', async () => {
      const created = await redisStreamService.createConsumerGroup(
        TEST_STREAM,
        TEST_GROUP,
      );
      expect(created).toBe(true);

      const groupInfo = await redisClient.xinfo('GROUPS', TEST_STREAM);
      expect(Array.isArray(groupInfo)).toBe(true);
      const createdGroup = (groupInfo as ['name', string][]).find(
        ([, name]) => name === TEST_GROUP,
      );
      expect(createdGroup).toBeDefined();
    });

    it('should delete a consumer group', async () => {
      await redisStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
      const deleted = await redisStreamService.deleteConsumerGroup(
        TEST_STREAM,
        TEST_GROUP,
      );
      expect(deleted).toBe(true);
    });
  });

  describe('Message Operations', () => {
    beforeEach(async () => {
      await redisStreamService.createStream(TEST_STREAM);
      await redisStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should publish messages to stream', async () => {
      const messages = [msg(1), msg(2)];
      const messageIds = await redisStreamService.publishMessages(
        TEST_STREAM,
        messages,
      );
      expect(messageIds).toHaveLength(2);
      expect(typeof messageIds[0]).toBe('string');
    });

    it('should consume messages from stream', async () => {
      const messages = [msg(1), msg(2)];
      await redisStreamService.publishMessages(TEST_STREAM, messages);

      const consumed = await redisStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { blockTimeout: 1000 },
      );

      expect(consumed).toHaveLength(2);
      expect(consumed[0]).toHaveProperty('id');
      expect(consumed[0]).toHaveProperty('data');
      expect(typeof consumed[0].data).toBe('object');
    });

    it('should acknowledge and delete messages', async () => {
      const messages = [msg(0)];
      const [messageId] = await redisStreamService.publishMessages(
        TEST_STREAM,
        messages,
      );

      await redisStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      const processedCount = await redisStreamService.ackAndDelete(
        TEST_STREAM,
        TEST_GROUP,
        [messageId],
      );
      expect(processedCount).toBe(1);

      // Verify message is no longer pending
      const pending = await redisStreamService.getPendingMessages(
        TEST_STREAM,
        TEST_GROUP,
        1,
        TEST_CONSUMER,
      );
      expect(pending).toHaveLength(0);
    });
  });

  describe('Message Retry Operations', () => {
    beforeEach(async () => {
      await redisStreamService.createStream(TEST_STREAM);
      await redisStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should retry pending messages', async () => {
      const message = [msg(0)];
      await redisStreamService.publishMessages(TEST_STREAM, message);

      // Consume but don't acknowledge
      await redisStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      // Wait for message to be idle
      await sleepFor(1100);

      const retriedMessages = await redisStreamService.retryMessages(
        TEST_STREAM,
        TEST_GROUP,
        {
          consumerName: 'retryConsumer',
          minIdleTime: 1000,
          limit: 10,
        },
      );

      expect(retriedMessages).toHaveLength(1);
      expect(Array.isArray(retriedMessages)).toBe(true);
      expect('id' in retriedMessages[0].data).toBe(true); //this is the message id (msg(0))
    });

    it('should claim specific messages', async () => {
      const message = [msg(0)];
      const [messageId] = await redisStreamService.publishMessages(
        TEST_STREAM,
        message,
      );

      // Consume but don't acknowledge
      await redisStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      await sleepFor(1100);

      const claimedMessage = await redisStreamService.claimMessage(
        TEST_STREAM,
        TEST_GROUP,
        'newConsumer',
        1000,
        messageId,
      );
      expect('id' in claimedMessage.data).toBe(true);
    });
  });

  describe('Provider Features', () => {
    it('should return correct provider-specific features', () => {
      const features = redisStreamService.getProviderSpecificFeatures();

      expect(features).toEqual({
        supportsBatching: true,
        supportsDeadLetterQueue: false,
        supportsNotifications: false,
        supportsOrdering: true,
        supportsTrimming: true,
        supportsRetry: true,
        maxMessageSize: 512 * 1024 * 1024,
        maxBatchSize: 1000,
      });
    });
  });
});
