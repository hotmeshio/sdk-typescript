import { HMNS } from '../../../../modules/key';
import { sleepFor } from '../../../../modules/utils';
import { LoggerService } from '../../../../services/logger';
import { RedisStreamService } from '../../../../services/stream/clients/redis';
import { RedisConnection, RedisClientType } from '../../../$setup/cache/redis';

describe('FUNCTIONAL | RedisStreamService', () => {
  let redisConnection: RedisConnection;
  let redisClient: RedisClientType;
  let redisStreamService: RedisStreamService;

  beforeEach(async () => {
    await redisClient.flushDb();
    redisStreamService = new RedisStreamService(redisClient);
    const appConfig = { id: 'APP_ID', version: 'APP_VERSION' };
    await redisStreamService.init(HMNS, appConfig.id, new LoggerService());
  });

  beforeAll(async () => {
    redisConnection = await RedisConnection.getConnection('test-connection-1');
    redisClient = await redisConnection.getClient();
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('xgroup', () => {
    it('should create a consumer group', async () => {
      const key = 'testKey';
      const groupName = 'testGroup';
      const groupId = '0';
      await redisStreamService.xgroup('CREATE', key, groupName, groupId, 'MKSTREAM');
      const groupInfo = await redisClient.sendCommand(['XINFO', 'GROUPS', key]);
      expect(Array.isArray(groupInfo)).toBe(true);
      const createdGroup = (groupInfo as [string, string][]).find(([, name]) => name === groupName);
      expect(createdGroup).toBeDefined();
    });
  });

  describe('xadd', () => {
    it('should add data to stream', async () => {
      const key = 'testKey';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.xadd(key, msgId, field, value);
      const messages = await redisClient.sendCommand(['XRANGE', key, '-', '+']) as [string, string[]][];
      const addedMessage = messages.find(([messageId, fields]) => fields.includes(field) && fields.includes(value));
      expect(addedMessage).toBeDefined();
    });

    it('should add data to stream using multi', async () => {
      const key = 'testKey';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      const multi = redisStreamService.getMulti();
      await redisStreamService.xadd(key, msgId, field, value, multi);
      await redisStreamService.xadd(key, msgId, field, value, multi);
      await redisStreamService.xlen(key, multi);
      const rslt = await multi.exec();
      expect(rslt[2]).toBe(2);
      const messages = await redisClient.sendCommand(['XRANGE', key, '-', '+']) as [string, string[]][];
      const addedMessages = messages.find(([messageId, fields]) => fields.includes(field) && fields.includes(value));
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
      await redisStreamService.xgroup('CREATE', key, groupName, groupId, 'MKSTREAM');
      const messageId = await redisStreamService.xadd(key, msgId, field, value);
      const messages = await redisStreamService.xreadgroup(
        'GROUP',
        groupName,
        consumerName,
        'BLOCK',
        1000,
        'STREAMS',
        key,
        '>'
      );
      const readMessage = (messages as string[][][])[0][1].find(([readMessageId, fields]) => readMessageId === messageId);
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
      await redisStreamService.xgroup('CREATE', key, groupName, groupId, 'MKSTREAM');
      const messageId = await redisStreamService.xadd(key, msgId, field, value) as string;
      await redisStreamService.xreadgroup('GROUP', groupName, 'testConsumer', 'BLOCK', 1000, 'STREAMS', key, '>');
      const ackCount = await redisStreamService.xack(key, groupName, messageId);
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
      await redisStreamService.xgroup('CREATE', key, groupName, groupId, 'MKSTREAM');
      const messageId = await redisStreamService.xadd(key, msgId, field, value);
      await redisStreamService.xreadgroup('GROUP', groupName, consumerName, 'BLOCK', 1000, 'STREAMS', key, '>');
      const pendingMessages = (await redisStreamService.xpending(key, groupName, '-', '+', 1, consumerName)) as unknown as [string][];
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
      await redisStreamService.xgroup('CREATE', key, groupName, groupId, 'MKSTREAM');
      const messageId = await redisStreamService.xadd(key, msgId, field, value) as string;
      // Then, read the message from the group
      await redisStreamService.xreadgroup('GROUP', groupName, initialConsumer, 'BLOCK', 1000, 'STREAMS', key, '>');
      //count pending messages
      await sleepFor(1000);
      const pendingMessageCount = await redisStreamService.xpending(key, groupName, '-', '+', 1) as [string, string, number, any][];
      //[[ '1688768134881-0', 'testConsumer1', 1017, 1 ]] //id, consumer, delay
      expect(pendingMessageCount[0][1]).toBe(initialConsumer);
      expect(pendingMessageCount[0][2]).toBeGreaterThan(1000);
      expect(pendingMessageCount[0][3]).toBe(1);
      // Retrieve pending messages for the initial consumer
      let pendingMessages = await redisStreamService.xpending(key, groupName, '-', '+', 1, initialConsumer) as [string, string, number, any][];
      const toBeClaimedMessage = pendingMessages.find(([id, consumer, ,]) => id === messageId && consumer === initialConsumer);
      expect(toBeClaimedMessage).toBeDefined();
      // Claim the message by another consumer using sendCommand
      const reclaimMessage = await redisStreamService.xclaim(key, groupName, claimantConsumer, 0, messageId);
      const [messageField, messageValue] = reclaimMessage[0][1];
      expect(messageField).toBe(field);
      expect(messageValue).toBe(value);
      //check for race (did another consumer claim this message?)
      const failedReclaimAttempt = await redisStreamService.xclaim(key, groupName, claimantConsumer, 1000, messageId);
      expect(failedReclaimAttempt.length).toBe(0);
      // Retrieve pending messages for the claimant consumer
      pendingMessages = await redisStreamService.xpending(key, groupName, '-', '+', 1, claimantConsumer) as [string, string, number, any][];
      const claimedMessage = pendingMessages.find(([id, consumer, ,]) => id === messageId && consumer === claimantConsumer);
      expect(claimedMessage).toBeDefined();
    });
  });

  describe('xdel', () => {
    it('should delete a message from a stream', async () => {
      const key = 'testKey';
      const groupName = 'testGroup';
      const groupId = '0';
      const msgId = '*';
      const field = 'testField';
      const value = 'testValue';
      await redisStreamService.xgroup('CREATE', key, groupName, groupId, 'MKSTREAM');
      const messageId = await redisStreamService.xadd(key, msgId, field, value) as string;
      const delCount = await redisStreamService.xdel(key, messageId);
      expect(delCount).toBe(1);
      const messages = await redisStreamService.xreadgroup('GROUP', groupName, 'testConsumer', 'BLOCK', '1000', 'STREAMS', key, messageId);
      const deletedMessage = (messages as string[][][])[0][1].find(([readMessageId, fields]) => readMessageId === messageId);
      expect(deletedMessage).toBeUndefined();
    });
  });
});
