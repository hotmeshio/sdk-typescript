import { Client } from 'pg';

import { HMNS } from '../../../../../modules/key';
import { parseStreamMessage, sleepFor } from '../../../../../modules/utils';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { LoggerService } from '../../../../../services/logger';
import { PostgresStreamService } from '../../../../../services/stream/providers/postgres/postgres';
import { PostgresClientType } from '../../../../../types/postgres';
import {
  ProviderNativeClient,
  ProviderClient,
} from '../../../../../types/provider';
import { dropTables } from '../../../../$setup/postgres';

describe('FUNCTIONAL | PostgresStreamService', () => {
  let postgresClient: ProviderNativeClient;
  let postgresStreamService: PostgresStreamService;
  const TEST_STREAM = 'testStream';
  const TEST_GROUP = 'WORKER';
  const TEST_CONSUMER = 'testConsumer';

  const msg = (idx: number): string => {
    return JSON.stringify({ id: idx, data: `message${idx}` });
  };

  beforeAll(async () => {
    // Initialize PostgreSQL connection
    postgresClient = (
      await PostgresConnection.connect('test', Client, {
        user: 'postgres',
        host: 'postgres',
        database: 'hotmesh',
        password: 'password',
        port: 5432,
      })
    ).getClient();

    dropTables(postgresClient);
  });

  beforeEach(async () => {
    // Initialize PostgresStreamService
    postgresStreamService = new PostgresStreamService(
      postgresClient as PostgresClientType & ProviderClient,
      {} as ProviderClient,
    );
    await postgresStreamService.init(HMNS, 'mytestapp', new LoggerService());

    // Clean up existing streams
    try {
      // Delete all streams
      await postgresStreamService.deleteStream('*');
    } catch (error) {
      // Stream might not exist; ignore error
    }
  });

  afterAll(async () => {
    await postgresClient.end();
  });

  describe('Stream Operations', () => {
    it('should create and delete a stream', async () => {
      const created = await postgresStreamService.createStream(TEST_STREAM);
      expect(created).toBe(true);

      // Verify stream exists by checking stats
      const stats = await postgresStreamService.getStreamStats(TEST_STREAM);
      expect(stats.messageCount).toBe(0);

      const deleted = await postgresStreamService.deleteStream(TEST_STREAM);
      expect(deleted).toBe(true);

      // After deletion, the stream stats should show zero messages
      const statsAfterDelete =
        await postgresStreamService.getStreamStats(TEST_STREAM);
      expect(statsAfterDelete.messageCount).toBe(0);
    });

    it('should get stream stats', async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      const messages = [msg(1), msg(2), msg(3)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      const stats = await postgresStreamService.getStreamStats(TEST_STREAM);
      expect(stats.messageCount).toBe(3);
    });

    it('should get stream depth', async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      const messages = [msg(1), msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(2);
    });

    it('should get stream depths', async () => {
      await postgresStreamService.createStream(`${TEST_STREAM}1`);
      await postgresStreamService.publishMessages(`${TEST_STREAM}1`, [
        msg(1),
        msg(2),
      ]);
      await postgresStreamService.createStream(`${TEST_STREAM}2`);
      await postgresStreamService.publishMessages(`${TEST_STREAM}2`, [msg(3)]);

      const depths = await postgresStreamService.getStreamDepths([
        { stream: `${TEST_STREAM}1` },
        { stream: `${TEST_STREAM}2` },
      ]);

      // find depth for 2nd stream
      const d1 = depths.find((d) => d.stream === `${TEST_STREAM}1`);
      const d2 = depths.find((d) => d.stream === `${TEST_STREAM}2`);
      expect(d1?.depth).toBe(2);
      expect(d2?.depth).toBe(1);
    });
  });

  describe('Consumer Group Operations', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
    });

    it('should create a consumer group', async () => {
      const created = await postgresStreamService.createConsumerGroup(
        TEST_STREAM,
        TEST_GROUP,
      );
      expect(created).toBe(true);

      // Verify consumer group by attempting to consume messages
      const messages = [msg(1)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );
      expect(consumed).toHaveLength(1);
    });

    it('should delete a consumer group', async () => {
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
      const deleted = await postgresStreamService.deleteConsumerGroup(
        TEST_STREAM,
        TEST_GROUP,
      );
      expect(deleted).toBe(true);

      // After deletion, consuming messages with the group should yield no results
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );
      expect(consumed).toHaveLength(0);
    });
  });

  describe('Message Operations', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should publish messages to stream', async () => {
      const messages = [msg(1), msg(2)];
      const messageIds = await postgresStreamService.publishMessages(
        TEST_STREAM,
        messages,
      );
      expect(messageIds).toHaveLength(2);
      expect(typeof messageIds[0]).toBe('string');
    });

    it('should consume messages from stream', async () => {
      const messages = [msg(1), msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 2 },
      );

      expect(consumed).toHaveLength(2);
      expect(consumed[0]).toHaveProperty('id');
      expect(consumed[0]).toHaveProperty('data');
      expect(typeof consumed[0].data).toBe('object');
    });

    it('should acknowledge and delete messages', async () => {
      const messages = [msg(0)];
      const messageIds = await postgresStreamService.publishMessages(
        TEST_STREAM,
        messages,
      );

      await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      const processedCount = await postgresStreamService.ackAndDelete(
        TEST_STREAM,
        TEST_GROUP,
        messageIds as string[],
      );
      expect(processedCount).toBe(1);

      // Verify that the message has been deleted
      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(0);
    });
  });

  describe('consumeMessages with Backoff', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });
  
    it('should consume messages without triggering backoff', async () => {
      const messages = [msg(1), msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);
  
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 2, enableBackoff: false },
      );
  
      expect(consumed).toHaveLength(2);
      expect(consumed[0]).toHaveProperty('id');
      expect(consumed[0]).toHaveProperty('data');
    });
  
    it('should apply backoff when stream is empty', async () => {
      const start = Date.now();
  
      // Attempt to consume messages with backoff enabled
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { enableBackoff: true, initialBackoff: 100, maxBackoff: 500 },
      );
  
      const elapsed = Date.now() - start;
  
      // Ensure no messages are consumed
      expect(consumed).toHaveLength(0);
  
      // Verify that backoff delay occurred (at least 100ms + 200ms + 400ms = 700ms total)
      expect(elapsed).toBeGreaterThanOrEqual(700);
    });
  
    it('should reset backoff after messages are consumed', async () => {
      const initialBackoff = 100;
  
      // First call with empty stream to trigger backoff
      const consumeSpy = jest.spyOn(postgresStreamService, 'consumeMessages');
      const firstAttempt = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { enableBackoff: true, initialBackoff, maxBackoff: 300 },
      );
      expect(firstAttempt).toHaveLength(0);
      expect(consumeSpy).toHaveBeenCalledTimes(1);
  
      // Publish messages to reset backoff
      const messages = [msg(1)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);
  
      // Consume the published messages
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { enableBackoff: true, initialBackoff, maxBackoff: 300 },
      );
  
      expect(consumed).toHaveLength(1);
      expect(consumed[0]).toHaveProperty('id');
      expect(consumed[0]).toHaveProperty('data');
    });
  
    it('should not exceed maximum backoff', async () => {
      const initialBackoff = 100;
      const maxBackoff = 500;
    
      const consumeSpy = jest.spyOn(postgresStreamService, 'consumeMessages');
      const querySpy = jest.spyOn(postgresStreamService['streamClient'], 'query');
    
      const start = Date.now();
    
      await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { enableBackoff: true, initialBackoff, maxBackoff, maxRetries: 3 }
      );
    
      const elapsed = Date.now() - start;
    
      // Ensure the backoff time does not exceed the expected maximum total time.
      // For 3 retries, each potentially at maxBackoff, that would be (assuming exponential):
      // initial: 100ms, second: 200ms, third: 400ms but capped at maxBackoff: 500ms
      // So total delay ~100ms + 200ms + 500ms = 800ms (approx)
      expect(elapsed).toBeLessThanOrEqual(maxBackoff * 3);
    
      // consumeMessages itself is only called once externally
      expect(consumeSpy).toHaveBeenCalledTimes(1);
    
      // Check internal retries by counting how many queries were executed.
      // With no messages, we expect multiple queries (e.g., 3 queries for 3 retries).
      expect(querySpy.mock.calls.length).toBeGreaterThan(1);
    });
  });

  describe('Message Retry Operations', () => {
    it('should reclaim messages after reservation timeout', async () => {
      const messages = [msg(1), msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);
    
      const consumerA = 'consumerA';
      const consumerB = 'consumerB';
    
      // Consumer A consumes but does not acknowledge
      const consumedByA = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        consumerA,
        { batchSize: 1, reservationTimeout: 2 },
      );
    
      expect(consumedByA).toHaveLength(1);
    
      // Wait for reservation timeout
      await sleepFor(3000);
    
      // Consumer B reclaims the message
      const consumedByB = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        consumerB,
        { batchSize: 1, reservationTimeout: 2 },
      );
    
      expect(consumedByB).toHaveLength(1);
      expect(consumedByB[0].id).toBe(consumedByA[0].id);
    }); 
  
    it('should implicitly consume stale messages without calling retryMessages', async () => {
      const message = [msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, message);

      // Consumer A consumes the message but doesn't acknowledge it
      const consumedMessagesA = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        'consumerA',
        { reservationTimeout: 1 }, // 1 second reservation timeout
      );
      expect(consumedMessagesA).toHaveLength(1);

      // Wait for the reservation timeout to expire
      await sleepFor(1500); // Wait for 1.5 seconds

      // Consumer B attempts to consume messages
      const consumedMessagesB = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        'consumerB',
        { reservationTimeout: 1 },
      );

      // The stale message should be picked up by Consumer B
      expect(consumedMessagesB).toHaveLength(1);
      expect(consumedMessagesB[0].id).toBe(consumedMessagesA[0].id);
      expect(consumedMessagesB[0].data).toEqual(parseStreamMessage(message[0]));
    });
  });

  describe('Provider Features', () => {
    it('should return correct provider-specific features', () => {
      const features = postgresStreamService.getProviderSpecificFeatures();

      expect(features).toEqual({
        supportsBatching: true,
        supportsDeadLetterQueue: false,
        supportsNotifications: true,
        supportsOrdering: true,
        supportsTrimming: true,
        supportsRetry: false,
        maxMessageSize: 1024 * 1024,
        maxBatchSize: 256,
      });
    });
  });
});
