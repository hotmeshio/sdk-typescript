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

  beforeEach(async () => {
    await redisClient.flushdb();
    redisStreamService = new IORedisStreamService(redisClient, redisStoreClient);
    const appConfig = { id: 'APP_ID', version: 'APP_VERSION' };
    await redisStreamService.init(HMNS, appConfig.id, new LoggerService());
  });

  beforeAll(async () => {
    const redisConnection = await RedisConnection.getConnection('test-connection-1');
    redisClient = await redisConnection.getClient();
    const redisStoreConnection = await RedisConnection.getConnection('test-connection-2');
    redisStoreClient = await redisStoreConnection.getClient();
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('createConsumerGroup', () => {
    it('should create a consumer group', async () => {
      const key = 'testKey';
      const groupName = 'testGroup';
      const created = await redisStreamService.createConsumerGroup(
        key,
        groupName,
      );
      expect(created).toBe(true);
      const groupInfo = await redisClient.xinfo('GROUPS', key);
      expect(Array.isArray(groupInfo)).toBe(true);
      const createdGroup = (groupInfo as ['name', string][]).find(
        ([, name]) => name === groupName,
      );
      expect(createdGroup).toBeDefined();
    });
  });

  describe('xadd', () => {
    it('should add data to stream', async () => {
      const key = 'testKey';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.publishMessage(key, msgId, field, value);
      const messages = await redisClient.xrange(key, '-', '+');
      const addedMessage = messages.find(
        ([_messageId, fields]) =>
          fields.includes(field) && fields.includes(value),
      );
      expect(addedMessage).toBeDefined();
    });

    it('should add data to stream using multi', async () => {
      const key = 'testKey';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      const multi = redisStreamService.getMulti();
      await redisStreamService.publishMessage(key, msgId, field, value, multi);
      await redisStreamService.publishMessage(key, msgId, field, value, multi);
      await redisStreamService.getMessageDepth(key, multi);
      const rslt = (await multi.exec()) as unknown as number[];
      expect(rslt[2][1]).toBe(2);
      const rslt2 = await redisStreamService.getMessageDepth(key);
      expect(rslt2).toBe(2);
      const messages = await redisClient.xrange(key, '-', '+');
      const addedMessages = messages.find(
        ([_messageId, fields]) =>
          fields.includes(field) && fields.includes(value),
      );
      expect(addedMessages?.length).toBe(2);
    });
  });

  describe('xreadgroup', () => {
    it('should read data from group in a stream', async () => {
      const key = 'testKey';
      const groupName = 'testGroup';
      const consumerName = 'testConsumer';
      const groupId = '0';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.createConsumerGroup(
        key,
        groupName,
      );
      const messageId = await redisStreamService.publishMessage(key, msgId, field, value);
      const messages = await redisStreamService.consumeMessages(
        groupName,
        consumerName,
        '1000',
        key,
      );
      const readMessage = (messages as string[][][])[0][1].find(
        ([readMessageId, _fields]) => readMessageId === messageId,
      );
      expect(readMessage).toBeDefined();
    });
  });

  describe('xack', () => {
    it('should acknowledge message in a group', async () => {
      const key = 'testKey';
      const groupName = 'testGroup';
      const groupId = '0';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.createConsumerGroup(
        key,
        groupName,
      );
      const messageId = (await redisStreamService.publishMessage(
        key,
        msgId,
        field,
        value,
      )) as string;
      await redisStreamService.consumeMessages(
        groupName,
        'testConsumer',
        '1000',
        key,
      );
      const ackCount = await redisStreamService.acknowledgeMessage(key, groupName, messageId);
      expect(ackCount).toBe(1);
    });
  });

  describe('xpending', () => {
    it('should retrieve pending messages for a group', async () => {
      const key = 'testKey';
      const consumerName = 'testConsumer';
      const groupName = 'testGroup';
      const groupId = '0';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.createConsumerGroup(
        key,
        groupName,
      );
      const messageId = await redisStreamService.publishMessage(key, msgId, field, value);
      await redisStreamService.consumeMessages(
        groupName,
        consumerName,
        '1000',
        key,
      );
      const pendingMessages = (await redisStreamService.getPendingMessages(
        key,
        groupName,
        1,
        consumerName,
      )) as [string][];
      const isPending = pendingMessages.some(([id, , ,]) => id === messageId);
      expect(isPending).toBe(true);
    });
  });

  describe('xclaim', () => {
    it('should claim a pending message in a group', async () => {
      const key = 'testKey';
      const initialConsumer = 'testConsumer1';
      const claimantConsumer = 'testConsumer2';
      const groupName = 'testGroup';
      const groupId = '0';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      // First, create a group and add a message to the stream
      await redisStreamService.createConsumerGroup(
        key,
        groupName,
      );
      const messageId = (await redisStreamService.publishMessage(
        key,
        msgId,
        field,
        value,
      )) as string;
      // Then, read the message from the group
      await redisStreamService.consumeMessages(
        groupName,
        initialConsumer,
        '1000',
        key,
      );
      //count pending messages
      await sleepFor(1000);
      const pendingMessageCount = (await redisStreamService.getPendingMessages(
        key,
        groupName,
        1,
      )) as [string, string, number, any][];
      //[[ '1688768134881-0', 'testConsumer1', 1017, 1 ]] //id, consumer, delay
      expect(pendingMessageCount[0][1]).toBe(initialConsumer);
      expect(pendingMessageCount[0][2]).toBeGreaterThan(1000);
      expect(pendingMessageCount[0][3]).toBe(1);
      // Retrieve pending messages for the initial consumer
      let pendingMessages = (await redisStreamService.getPendingMessages(
        key,
        groupName,
        1,
        initialConsumer,
      )) as [string, string, number, any][];
      let claimedMessage = pendingMessages.find(
        ([id, consumer, ,]) => id === messageId && consumer === initialConsumer,
      );
      expect(claimedMessage).toBeDefined();
      // Claim the message by another consumer using sendCommand
      const reclaimMessage = await redisStreamService.claimMessage(
        key,
        groupName,
        claimantConsumer,
        0,
        messageId,
      );
      const [messageField, messageValue] = reclaimMessage[0][1];
      expect(messageField).toBe(field);
      expect(messageValue).toBe(value);
      //check for race (did another consumer claim this message?)
      const failedReclaimAttempt = await redisStreamService.claimMessage(
        key,
        groupName,
        claimantConsumer,
        1000,
        messageId,
      );
      expect(failedReclaimAttempt.length).toBe(0);
      // Retrieve pending messages for the claimant consumer
      pendingMessages = (await redisStreamService.getPendingMessages(
        key,
        groupName,
        1,
        claimantConsumer,
      )) as [string, string, number, any][];
      claimedMessage = pendingMessages.find(
        ([id, consumer, ,]) =>
          id === messageId && consumer === claimantConsumer,
      );
      expect(claimedMessage).toBeDefined();
    });
  });

  describe('xdel', () => {
    it('should delete a message from a stream', async () => {
      const key = 'testKey';
      const groupName = 'testGroup';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.createConsumerGroup(
        key,
        groupName,
      );
      const messageId = (await redisStreamService.publishMessage(
        key,
        msgId,
        field,
        value,
      )) as string;
      const delCount = await redisStreamService.deleteMessage(key, messageId);
      expect(delCount).toBe(1);
      const messages = await redisStreamService.consumeMessages(
        groupName,
        'testConsumer',
        '1000',
        key,
      );
      const deletedMessage = (messages as string[][][])?.[0]?.[1]?.find(
        ([readMessageId, _fields]) => readMessageId === messageId,
      );
      expect(deletedMessage).toBeUndefined();
    });
  });
});
