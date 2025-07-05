// /app/tests/functional/stream/providers/nats/nats.test.ts
import { connect } from 'nats';

import { HMNS } from '../../../../../modules/key';
import { sleepFor } from '../../../../../modules/utils';
import { NatsConnection } from '../../../../../services/connector/providers/nats';
import { LoggerService } from '../../../../../services/logger';
import { NatsStreamService } from '../../../../../services/stream/providers/nats/nats';
import {
  NatsClientType,
  NatsConsumerInfo,
  NatsStreamInfo,
} from '../../../../../types/nats';
import { ProviderClient } from '../../../../../types/provider';

describe('FUNCTIONAL | NatsStreamService', () => {
  let natsClient: NatsClientType;
  let natsStreamService: NatsStreamService;
  const TEST_STREAM = 'testStream';
  const TEST_GROUP = 'testGroup';
  const TEST_CONSUMER = 'testConsumer';

  const msg = (idx: number): string => {
    return JSON.stringify({ id: idx, data: `message${idx}` });
  };

  beforeAll(async () => {
    // Initialize NATS connection
    const natsConnection = await NatsConnection.connect(
      'test-connection-1',
      connect,
      { servers: ['nats:4222'] },
    );
    natsClient = natsConnection.getClient();
  });

  beforeEach(async () => {
    // Clean up existing streams and consumers
    natsStreamService = new NatsStreamService(natsClient, {} as ProviderClient);
    await natsStreamService.init(HMNS, 'APP_ID', new LoggerService());

    // Delete test stream if it exists
    try {
      await natsStreamService.deleteStream(TEST_STREAM);
      await natsStreamService.deleteStream(`${TEST_STREAM}1`);
      await natsStreamService.deleteStream(`${TEST_STREAM}2`);
    } catch (error) {
      // Stream might not exist, ignore error
    }
  });

  afterAll(async () => {
    await natsClient.close();
  });

  describe('Stream Operations', () => {
    it('should create and delete a stream', async () => {
      const created = await natsStreamService.createStream(TEST_STREAM);
      expect(created).toBe(true);

      const streams: NatsStreamInfo[] = [];
      for await (const stream of natsStreamService.jsm.streams.list()) {
        streams.push(stream);
      }
      const streamNames = streams.map((stream) => stream.config.name);
      expect(streamNames).toContain(TEST_STREAM);

      const deleted = await natsStreamService.deleteStream(TEST_STREAM);
      expect(deleted).toBe(true);

      const streamsAfterDelete: NatsStreamInfo[] = [];
      for await (const stream of natsStreamService.jsm.streams.list()) {
        streamsAfterDelete.push(stream);
      }
      const streamNamesAfterDelete = streamsAfterDelete.map(
        (stream) => stream.config.name,
      );
      expect(streamNamesAfterDelete).not.toContain(TEST_STREAM);
    });

    it('should get stream stats', async () => {
      await natsStreamService.createStream(TEST_STREAM);
      const messages = [msg(1), msg(2), msg(3)];
      await natsStreamService.publishMessages(TEST_STREAM, messages);

      const stats = await natsStreamService.getStreamStats(TEST_STREAM);
      expect(stats.messageCount).toBe(3);
    });

    it('should get stream depth', async () => {
      await natsStreamService.createStream(TEST_STREAM);
      const messages = [msg(1), msg(2)];
      await natsStreamService.publishMessages(TEST_STREAM, messages);

      const stats = await natsStreamService.getStreamStats(TEST_STREAM);
      expect(stats.messageCount).toBe(2);

      const depth = await natsStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(2);
    });

    it('should get stream depths', async () => {
      await natsStreamService.createStream(`${TEST_STREAM}1`);
      await natsStreamService.publishMessages(`${TEST_STREAM}1`, [
        msg(1),
        msg(2),
      ]);
      await natsStreamService.createStream(`${TEST_STREAM}2`);
      await natsStreamService.publishMessages(`${TEST_STREAM}2`, [msg(3)]);

      const stats = await natsStreamService.getStreamStats(`${TEST_STREAM}1`);
      expect(stats.messageCount).toBe(2);

      const depths = await natsStreamService.getStreamDepths([
        { stream: `${TEST_STREAM}1` },
        { stream: `${TEST_STREAM}2` },
      ]);

      expect(depths).toEqual([
        { stream: `${TEST_STREAM}1`, depth: 2 },
        { stream: `${TEST_STREAM}2`, depth: 1 },
      ]);
    });
  });

  describe('Consumer Group Operations', () => {
    beforeEach(async () => {
      await natsStreamService.createStream(TEST_STREAM);
    });

    it('should create a consumer group', async () => {
      const created = await natsStreamService.createConsumerGroup(
        TEST_STREAM,
        TEST_GROUP,
      );
      expect(created).toBe(true);

      const consumers: NatsConsumerInfo[] = [];
      for await (const consumer of natsStreamService.jsm.consumers.list(
        TEST_STREAM,
      )) {
        consumers.push(consumer);
      }
      const consumerNames = consumers.map((consumer) => consumer.name);
      expect(consumerNames).toContain(TEST_GROUP);
    });

    it('should delete a consumer group', async () => {
      await natsStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
      const deleted = await natsStreamService.deleteConsumerGroup(
        TEST_STREAM,
        TEST_GROUP,
      );
      expect(deleted).toBe(true);

      const consumersAfterDelete: NatsConsumerInfo[] = [];
      for await (const consumer of natsStreamService.jsm.consumers.list(
        TEST_STREAM,
      )) {
        consumersAfterDelete.push(consumer);
      }
      const consumerNamesAfterDelete = consumersAfterDelete.map(
        (consumer) => consumer.name,
      );
      expect(consumerNamesAfterDelete).not.toContain(TEST_GROUP);
    });
  });

  describe('Message Operations', () => {
    beforeEach(async () => {
      await natsStreamService.createStream(TEST_STREAM);
      await natsStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should publish messages to stream', async () => {
      const messages = [msg(1), msg(2)];
      const messageIds = await natsStreamService.publishMessages(
        TEST_STREAM,
        messages,
      );
      expect(messageIds).toHaveLength(2);
      expect(typeof messageIds[0]).toBe('string');
    });

    it('should consume messages from stream', async () => {
      const messages = [msg(1), msg(2)];
      await natsStreamService.publishMessages(TEST_STREAM, messages);

      const consumed = await natsStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 2, blockTimeout: 1000 },
      );

      expect(consumed).toHaveLength(2);
      expect(consumed[0]).toHaveProperty('id');
      expect(consumed[0]).toHaveProperty('data');
      expect(typeof consumed[0].data).toBe('object');
    });

    it('should acknowledge and delete messages', async () => {
      const messages = [msg(0)];
      const messageIds = await natsStreamService.publishMessages(
        TEST_STREAM,
        messages,
      );

      const consumedMessages = await natsStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      const processedCount = await natsStreamService.ackAndDelete(
        TEST_STREAM,
        TEST_GROUP,
        messageIds as string[],
      );
      expect(processedCount).toBe(1);
    });
  });

  describe.skip('Message Retry Operations', () => {
    beforeEach(async () => {
      await natsStreamService.createStream(TEST_STREAM);
      await natsStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should handle message retries (not supported)', async () => {
      const messages = [msg(0)];
      await natsStreamService.publishMessages(TEST_STREAM, messages);

      // Consume but don't acknowledge
      const consumedMessages = await natsStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { autoAck: false },
      );

      // Wait for ack_wait to expire
      await sleepFor(31_000); // Assuming ack_wait is set to 30 seconds

      // Try consuming again
      const retriedMessages = await natsStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { autoAck: false },
      );

      expect(retriedMessages).toHaveLength(1);
      expect(retriedMessages[0].id).toBe(consumedMessages[0].id);
    }, 35_000);
  });

  describe('Provider Features', () => {
    it('should return correct provider-specific features', () => {
      const features = natsStreamService.getProviderSpecificFeatures();

      expect(features).toEqual({
        supportsBatching: true,
        supportsDeadLetterQueue: true,
        supportsNotifications: false,
        supportsOrdering: true,
        supportsTrimming: true,
        supportsRetry: false,
        maxMessageSize: 1024 * 1024,
        maxBatchSize: 256,
      });
    });
  });
});
