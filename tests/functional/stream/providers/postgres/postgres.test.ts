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

    await dropTables(postgresClient);
  });

  beforeEach(async () => {
    // Clean up the previous service instance if it exists
    if (postgresStreamService) {
      try {
        await postgresStreamService.cleanup();
      } catch (error) {
        // Ignore cleanup errors
      }
    }

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

  afterEach(async () => {
    // Ensure cleanup happens after each test
    if (postgresStreamService) {
      try {
        await postgresStreamService.cleanup();
      } catch (error) {
        // Ignore cleanup errors
      }
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
      const consumeSpy = vi.spyOn(postgresStreamService, 'consumeMessages');
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
    
      const consumeSpy = vi.spyOn(postgresStreamService, 'consumeMessages');
      const querySpy = vi.spyOn(postgresStreamService['streamClient'], 'query');
    
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
      // Account for _retryAttempt field injected by postgres stream service
      const expectedData = { ...parseStreamMessage(message[0]), _retryAttempt: 0 };
      expect(consumedMessagesB[0].data).toEqual(expectedData);
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

  describe('Visibility Timeout/Delay', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should not consume messages with future visibility delay', async () => {
      // Create message with visibility delay (internal field)
      const messageData = {
        id: 1,
        data: 'delayed-message',
        _visibilityDelayMs: 2000, // 2 second delay
      };
      const messages = [JSON.stringify(messageData)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Try to consume immediately - should get nothing
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 10 },
      );

      expect(consumed).toHaveLength(0);
    });

    it('should consume messages after visibility delay expires', async () => {
      // Create message with short visibility delay
      const messageData = {
        id: 2,
        data: 'delayed-message-2',
        _visibilityDelayMs: 1000, // 1 second delay
      };
      const messages = [JSON.stringify(messageData)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Wait for visibility delay to expire
      await sleepFor(1500);

      // Now should be able to consume
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 10 },
      );

      expect(consumed).toHaveLength(1);
      expect(consumed[0].data.data).toBe('delayed-message-2');
    });

    it('should handle mixed visibility delays correctly', async () => {
      // Publish messages with different visibility delays
      const messages = [
        JSON.stringify({ id: 1, data: 'immediate' }), // No delay
        JSON.stringify({ id: 2, data: 'delayed-3s', _visibilityDelayMs: 3000 }), // 3 second delay
        JSON.stringify({ id: 3, data: 'delayed-1s', _visibilityDelayMs: 1000 }), // 1 second delay
      ];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Immediate consumption - should only get immediate message
      const firstBatch = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 10 },
      );
      expect(firstBatch).toHaveLength(1);
      expect(firstBatch[0].data.data).toBe('immediate');
      await postgresStreamService.ackAndDelete(TEST_STREAM, TEST_GROUP, [firstBatch[0].id]);

      // Wait 1.5 seconds - should get 1s delayed message
      await sleepFor(1500);
      const secondBatch = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 10 },
      );
      expect(secondBatch).toHaveLength(1);
      expect(secondBatch[0].data.data).toBe('delayed-1s');
      await postgresStreamService.ackAndDelete(TEST_STREAM, TEST_GROUP, [secondBatch[0].id]);

      // Wait another 2 seconds - should get 3s delayed message
      await sleepFor(2000);
      const thirdBatch = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 10 },
      );
      expect(thirdBatch).toHaveLength(1);
      expect(thirdBatch[0].data.data).toBe('delayed-3s');
    });
  });

  describe('Retry Policy Configuration', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should publish and consume message with custom retry policy', async () => {
      // Create message with custom retry policy
      const messageData = {
        id: 1,
        data: 'message-with-retry',
        _streamRetryConfig: {
          max_retry_attempts: 5,
          backoff_coefficient: 2.0,
          maximum_interval_seconds: 60,
        },
      };
      const messages = [JSON.stringify(messageData)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Consume the message
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(consumed).toHaveLength(1);
      expect(consumed[0].retryPolicy).toBeDefined();
      expect(consumed[0].retryPolicy?.maximumAttempts).toBe(5);
      expect(consumed[0].retryPolicy?.backoffCoefficient).toBe(2.0);
      expect(consumed[0].retryPolicy?.maximumInterval).toBe(60);
    });

    it('should not inject retry policy for default values', async () => {
      // Create message without custom retry policy (uses defaults)
      const messageData = { id: 1, data: 'message-default-retry' };
      const messages = [JSON.stringify(messageData)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Consume the message
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(consumed).toHaveLength(1);
      expect(consumed[0].retryPolicy).toBeUndefined();
      expect(consumed[0].data._streamRetryConfig).toBeUndefined();
    });

    it('should handle retry policy via publishMessages options', async () => {
      const messages = [JSON.stringify({ id: 1, data: 'test' })];
      await postgresStreamService.publishMessages(TEST_STREAM, messages, {
        retryPolicy: {
          maximumAttempts: 7,
          backoffCoefficient: 1.5,
          maximumInterval: 90,
        },
      });

      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(consumed).toHaveLength(1);
      expect(consumed[0].retryPolicy).toBeDefined();
      expect(consumed[0].retryPolicy?.maximumAttempts).toBe(7);
      expect(consumed[0].retryPolicy?.backoffCoefficient).toBe(1.5);
      expect(consumed[0].retryPolicy?.maximumInterval).toBe(90);
    });
  });

  describe('Notification-Based Consumption', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should receive notifications when messages are published', async () => {
      const receivedMessages: any[] = [];
      let resolveTest: () => void;
      const testPromise = new Promise<void>((resolve) => {
        resolveTest = resolve;
      });

      // Set up notification consumer
      await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        {
          enableNotifications: true,
          notificationCallback: (messages) => {
            receivedMessages.push(...messages);
            if (receivedMessages.length >= 2) {
              resolveTest();
            }
          },
        },
      );

      // Give setup time to complete
      await sleepFor(100);

      // Publish messages - should trigger notifications
      const messages = [msg(1), msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Wait for callback to be triggered
      await Promise.race([
        testPromise,
        sleepFor(5000).then(() => {
          throw new Error('Timeout waiting for notifications');
        }),
      ]);

      expect(receivedMessages).toHaveLength(2);
      // Check that we received both messages (order may vary due to initial fetch)
      const ids = receivedMessages.map(m => (m.data as any).id).sort();
      expect(ids).toEqual([1, 2]);
    }, 10000);

    it('should stop receiving notifications after cleanup', async () => {
      const receivedMessages: any[] = [];

      // Set up notification consumer
      await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        {
          enableNotifications: true,
          notificationCallback: (messages) => {
            receivedMessages.push(...messages);
          },
        },
      );

      await sleepFor(100);

      // Publish first message
      await postgresStreamService.publishMessages(TEST_STREAM, [msg(1)]);
      await sleepFor(200);

      // Stop the consumer
      await postgresStreamService.stopNotificationConsumer(TEST_STREAM, TEST_GROUP);
      await sleepFor(100);

      const countBefore = receivedMessages.length;

      // Publish another message - should NOT trigger callback
      await postgresStreamService.publishMessages(TEST_STREAM, [msg(2)]);
      await sleepFor(200);

      // Count should not have increased
      expect(receivedMessages.length).toBe(countBefore);
    }, 10000);
  });

  describe('Stream Trimming Operations', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should trim stream by max length', async () => {
      // Publish 5 messages
      const messages = [msg(1), msg(2), msg(3), msg(4), msg(5)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Verify all messages exist
      let depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(5);

      // Trim to keep only 3 messages (expire oldest 2)
      const trimmed = await postgresStreamService.trimStream(TEST_STREAM, {
        maxLen: 3,
      });

      expect(trimmed).toBe(2);

      // Verify only 3 messages remain (getStreamDepth excludes expired)
      depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(3);
    });

    it('should trim stream by max age', async () => {
      // Publish messages
      const messages = [msg(1), msg(2)];
      await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Wait 1 second
      await sleepFor(1000);

      // Publish more messages
      await postgresStreamService.publishMessages(TEST_STREAM, [msg(3)]);

      // Trim messages older than 500ms
      const trimmed = await postgresStreamService.trimStream(TEST_STREAM, {
        maxAge: 500,
      });

      // First 2 messages should be trimmed
      expect(trimmed).toBeGreaterThanOrEqual(2);

      // Verify only newest message remains (getStreamDepth excludes expired)
      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(1);
    });

    it('should trim stream by both maxLen and maxAge', async () => {
      // Publish 3 messages
      await postgresStreamService.publishMessages(TEST_STREAM, [msg(1), msg(2), msg(3)]);

      // Wait
      await sleepFor(1000);

      // Publish 2 more messages
      await postgresStreamService.publishMessages(TEST_STREAM, [msg(4), msg(5)]);

      // Trim: keep max 4 messages AND expire messages older than 500ms
      // Note: Both operations run, so old messages are trimmed, then if >4 remain, oldest are trimmed
      const trimmed = await postgresStreamService.trimStream(TEST_STREAM, {
        maxLen: 4,
        maxAge: 500,
      });

      // Should trim at least the 3 old messages
      expect(trimmed).toBeGreaterThanOrEqual(3);

      // After maxAge trim: 2 messages remain (the new ones)
      // maxLen=4 won't trim anything since we only have 2
      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBeLessThanOrEqual(2);
    });
  });

  describe('Message Deletion Operations', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    it('should soft delete messages', async () => {
      // Publish messages
      const messages = [msg(1), msg(2), msg(3)];
      const messageIds = await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Verify messages exist
      let depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(3);

      // Delete specific messages
      const deletedCount = await postgresStreamService.deleteMessages(
        TEST_STREAM,
        TEST_GROUP,
        messageIds.slice(0, 2) as string[],
      );

      expect(deletedCount).toBe(2);

      // Verify depth decreased (getStreamDepth excludes expired)
      depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(1);
    });

    it('should not consume soft-deleted messages', async () => {
      const messages = [msg(1)];
      const messageIds = await postgresStreamService.publishMessages(TEST_STREAM, messages);

      // Delete the message immediately (before consuming)
      await postgresStreamService.deleteMessages(
        TEST_STREAM,
        TEST_GROUP,
        messageIds as string[],
      );

      // Try to consume - should get nothing since message is deleted
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );
      expect(consumed).toHaveLength(0);

      // Verify depth is 0
      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(0);
    });
  });

  describe('Cleanup and Resource Management', () => {
    it('should properly cleanup resources', async () => {
      const testStream = 'cleanup-test-stream';
      await postgresStreamService.createStream(testStream);
      await postgresStreamService.createConsumerGroup(testStream, TEST_GROUP);

      // Set up notification consumer
      const receivedMessages: any[] = [];
      await postgresStreamService.consumeMessages(
        testStream,
        TEST_GROUP,
        TEST_CONSUMER,
        {
          enableNotifications: true,
          notificationCallback: (messages) => {
            receivedMessages.push(...messages);
          },
        },
      );

      await sleepFor(100);

      // Cleanup
      await postgresStreamService.cleanup();

      // Verify cleanup occurred (no errors thrown)
      expect(true).toBe(true);
    });
  });

  describe('Retry Attempt Tracking', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createConsumerGroup(TEST_STREAM, TEST_GROUP);
    });

    afterEach(async () => {
      // Clean up any remaining messages
      try {
        await postgresStreamService.deleteStream(TEST_STREAM);
      } catch (error) {
        // Ignore
      }
    });

    it('should track retry attempts in messages', async () => {
      // Publish message with retry attempt and a minimal visibility delay
      // (retry_attempt column only inserted when visibilityDelayMs > 0)
      const messageData = {
        id: 1,
        data: 'retry-test',
        _retryAttempt: 2,
        _visibilityDelayMs: 1, // Must be > 0 to trigger retry_attempt column
      };
      const messages = [JSON.stringify(messageData)];
      const publishedIds = await postgresStreamService.publishMessages(TEST_STREAM, messages);
      
      expect(publishedIds).toHaveLength(1);

      // Small delay to ensure message is visible
      await sleepFor(10);

      // Verify message exists before consuming
      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(1);

      // Consume the message
      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(consumed).toHaveLength(1);
      expect(consumed[0].data._retryAttempt).toBe(2);
    });

    it('should default retry attempt to 0 for new messages', async () => {
      // Simple message without retry config - uses DB default (0)
      const messages = [msg(1)];
      const publishedIds = await postgresStreamService.publishMessages(TEST_STREAM, messages);
      
      expect(publishedIds).toHaveLength(1);

      // Verify message exists
      const depth = await postgresStreamService.getStreamDepth(TEST_STREAM);
      expect(depth).toBe(1);

      const consumed = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(consumed).toHaveLength(1);
      // When no retry config/visibility delay, retry_attempt defaults to 0 in DB
      // and gets injected into the message data on consumption
      expect(consumed[0].data._retryAttempt).toBe(0);
    });
  });

  describe('Multiple Consumer Groups', () => {
    beforeEach(async () => {
      await postgresStreamService.createStream(TEST_STREAM);
      await postgresStreamService.createStream(TEST_STREAM + ':');
    });

    afterEach(async () => {
      // Clean up
      try {
        await postgresStreamService.deleteStream(TEST_STREAM);
        await postgresStreamService.deleteStream(TEST_STREAM + ':');
      } catch (error) {
        // Ignore
      }
    });

    it('should isolate messages between different consumer groups', async () => {
      const GROUP_A = 'WORKER';
      const GROUP_B = 'ENGINE';

      // Publish messages to stream with WORKER group (default)
      const messagesWorker = [msg(1), msg(2)];
      const publishedWorker = await postgresStreamService.publishMessages(TEST_STREAM, messagesWorker);
      expect(publishedWorker).toHaveLength(2);

      // Publish messages to stream with ENGINE group (stream name ending with ':')
      const messagesEngine = [msg(3), msg(4)];
      const publishedEngine = await postgresStreamService.publishMessages(TEST_STREAM + ':', messagesEngine);
      expect(publishedEngine).toHaveLength(2);

      // Verify depths
      const depthWorker = await postgresStreamService.getStreamDepth(TEST_STREAM);
      const depthEngine = await postgresStreamService.getStreamDepth(TEST_STREAM + ':');
      expect(depthWorker).toBe(2);
      expect(depthEngine).toBe(2);

      // Consume from WORKER group
      const consumedWorker = await postgresStreamService.consumeMessages(
        TEST_STREAM,
        GROUP_A,
        'consumerA',
        { batchSize: 10 },
      );

      // Consume from ENGINE group
      const consumedEngine = await postgresStreamService.consumeMessages(
        TEST_STREAM + ':',
        GROUP_B,
        'consumerB',
        { batchSize: 10 },
      );

      // Each group should only see its own messages
      expect(consumedWorker).toHaveLength(2);
      expect(consumedEngine).toHaveLength(2);
      expect((consumedWorker[0].data as any).id).toBe(1);
      expect((consumedEngine[0].data as any).id).toBe(3);
    });
  });
});
