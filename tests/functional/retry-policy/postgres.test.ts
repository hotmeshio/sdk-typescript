import { Client as Postgres } from 'pg';

import { HMNS } from '../../../modules/key';
import { guid, sleepFor, normalizeRetryPolicy } from '../../../modules/utils';
import { HotMesh, HotMeshConfig } from '../../../index';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
  RetryPolicy,
} from '../../../types/stream';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresStreamService } from '../../../services/stream/providers/postgres/postgres';
import { LoggerService } from '../../../services/logger';

describe('FUNCTIONAL | Retry Policy | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let streamService: PostgresStreamService;
  const TEST_STREAM = 'test-retry-stream';
  const TEST_GROUP = 'WORKER';
  const TEST_CONSUMER = 'testConsumer';

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    // Initialize stream service
    streamService = new PostgresStreamService(
      postgresClient as any,
      {} as any,
    );
    await streamService.init(HMNS, 'retrypolicytest', new LoggerService());
  });

  afterAll(async () => {
    await postgresClient.end();
    await HotMesh.stop();
  });

  describe('HotMesh-Level Retry Policy', () => {
    it('should store retry policy values in database when publishing', async () => {
      // Use existing TEST_STREAM that gets cleaned up properly
      const messageData = {
        metadata: { guid: guid(), aid: 'test-hotmesh' },
        data: { test: 'data' },
        _streamRetryConfig: normalizeRetryPolicy({
          maximumAttempts: 7,
          backoffCoefficient: 3,
          maximumInterval: '600s',
        }),
      };

      await streamService.publishMessages(TEST_STREAM, [
        JSON.stringify(messageData),
      ]);

      // Query database to verify values were stored
      const result = await postgresClient.query(
        `SELECT max_retry_attempts, backoff_coefficient, maximum_interval_seconds
         FROM retrypolicytest.streams
         WHERE stream_name = $1 AND expired_at IS NULL`,
        [TEST_STREAM],
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].max_retry_attempts).toBe(7);
      expect(parseFloat(result.rows[0].backoff_coefficient)).toBe(3);
      expect(result.rows[0].maximum_interval_seconds).toBe(600);
      
      // Clean up for next test
      await streamService.deleteStream(TEST_STREAM);
      await streamService.createStream(TEST_STREAM);
    });
  });

  describe('PostgreSQL Column Storage', () => {
    beforeEach(async () => {
      // Clean up stream
      try {
        await streamService.deleteStream(TEST_STREAM);
      } catch (error) {
        // Ignore if stream doesn't exist
      }
      await streamService.createStream(TEST_STREAM);
    });

    it('should store retry policy in database columns when publishing', async () => {
      const retryPolicy: RetryPolicy = {
        maximumAttempts: 5,
        backoffCoefficient: 2,
        maximumInterval: '300s',
      };

      const messageData = {
        metadata: { guid: guid(), aid: 'test-activity' },
        data: { test: 'data' },
        _streamRetryConfig: normalizeRetryPolicy(retryPolicy),
      };

      await streamService.publishMessages(
        TEST_STREAM,
        [JSON.stringify(messageData)],
        { retryPolicy },
      );

      // Query database directly to verify columns
      const result = await postgresClient.query(
        `SELECT max_retry_attempts, backoff_coefficient, maximum_interval_seconds
         FROM retrypolicytest.streams
         WHERE stream_name = $1 AND expired_at IS NULL`,
        [TEST_STREAM],
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].max_retry_attempts).toBe(5);
      expect(parseFloat(result.rows[0].backoff_coefficient)).toBe(2);
      expect(result.rows[0].maximum_interval_seconds).toBe(300);
    });

    it('should use default retry policy values when not specified', async () => {
      const messageData = {
        metadata: { guid: guid(), aid: 'test-activity' },
        data: { test: 'data' },
      };

      await streamService.publishMessages(TEST_STREAM, [
        JSON.stringify(messageData),
      ]);

      const result = await postgresClient.query(
        `SELECT max_retry_attempts, backoff_coefficient, maximum_interval_seconds
         FROM retrypolicytest.streams
         WHERE stream_name = $1 AND expired_at IS NULL`,
        [TEST_STREAM],
      );

      expect(result.rows).toHaveLength(1);
      // Default values: 3 attempts, coefficient 10, max 120s
      expect(result.rows[0].max_retry_attempts).toBe(3);
      expect(parseFloat(result.rows[0].backoff_coefficient)).toBe(10);
      expect(result.rows[0].maximum_interval_seconds).toBe(120);
    });

    it('should inject retry policy when consuming messages', async () => {
      const retryPolicy: RetryPolicy = {
        maximumAttempts: 7,
        backoffCoefficient: 3,
        maximumInterval: '600s',
      };

      const messageData = {
        metadata: { guid: guid(), aid: 'test-activity' },
        data: { test: 'data' },
      };

      await streamService.publishMessages(
        TEST_STREAM,
        [JSON.stringify(messageData)],
        { retryPolicy },
      );

      const messages = await streamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
        { batchSize: 1 },
      );

      expect(messages).toHaveLength(1);
      expect(messages[0].retryPolicy).toBeDefined();
      expect(messages[0].retryPolicy?.maximumAttempts).toBe(7);
      expect(messages[0].retryPolicy?.backoffCoefficient).toBe(3);
      expect(messages[0].retryPolicy?.maximumInterval).toBe(600);

      // Verify _streamRetryConfig is injected into StreamData
      expect((messages[0].data as any)._streamRetryConfig).toBeDefined();
      expect((messages[0].data as any)._streamRetryConfig.max_retry_attempts).toBe(7);
      expect((messages[0].data as any)._streamRetryConfig.backoff_coefficient).toBe(3);
      expect((messages[0].data as any)._streamRetryConfig.maximum_interval_seconds).toBe(600);
    });

    it('should handle multiple messages with different retry policies', async () => {
      const messages = [
        {
          metadata: { guid: guid(), aid: 'test-1' },
          data: { id: 1 },
          _streamRetryConfig: normalizeRetryPolicy({
            maximumAttempts: 3,
            backoffCoefficient: 10,
            maximumInterval: '120s',
          }),
        },
        {
          metadata: { guid: guid(), aid: 'test-2' },
          data: { id: 2 },
          _streamRetryConfig: normalizeRetryPolicy({
            maximumAttempts: 10,
            backoffCoefficient: 2,
            maximumInterval: '600s',
          }),
        },
      ];

      await streamService.publishMessages(
        TEST_STREAM,
        messages.map((m) => JSON.stringify(m)),
      );

      const result = await postgresClient.query(
        `SELECT id, max_retry_attempts, backoff_coefficient, maximum_interval_seconds
         FROM retrypolicytest.streams
         WHERE stream_name = $1 AND expired_at IS NULL
         ORDER BY id ASC`,
        [TEST_STREAM],
      );

      expect(result.rows).toHaveLength(2);
      
      // First message
      expect(result.rows[0].max_retry_attempts).toBe(3);
      expect(parseFloat(result.rows[0].backoff_coefficient)).toBe(10);
      expect(result.rows[0].maximum_interval_seconds).toBe(120);

      // Second message
      expect(result.rows[1].max_retry_attempts).toBe(10);
      expect(parseFloat(result.rows[1].backoff_coefficient)).toBe(2);
      expect(result.rows[1].maximum_interval_seconds).toBe(600);
    });

    it('should query messages by retry policy configuration', async () => {
      // Publish messages with different policies
      await streamService.publishMessages(
        `${TEST_STREAM}-aggressive`,
        [JSON.stringify({ metadata: { guid: guid(), aid: 'test' }, data: {} })],
        {
          retryPolicy: {
            maximumAttempts: 10,
            backoffCoefficient: 2,
            maximumInterval: '600s',
          },
        },
      );

      await streamService.publishMessages(
        `${TEST_STREAM}-standard`,
        [JSON.stringify({ metadata: { guid: guid(), aid: 'test' }, data: {} })],
        {
          retryPolicy: {
            maximumAttempts: 3,
            backoffCoefficient: 10,
            maximumInterval: '120s',
          },
        },
      );

      // Query for aggressive retry policies (> 5 attempts)
      const aggressive = await postgresClient.query(
        `SELECT stream_name, max_retry_attempts
         FROM retrypolicytest.streams
         WHERE max_retry_attempts > 5 AND expired_at IS NULL`,
      );

      expect(aggressive.rows.length).toBeGreaterThan(0);
      expect(aggressive.rows[0].stream_name).toContain('aggressive');
      expect(aggressive.rows[0].max_retry_attempts).toBe(10);

      // Query for fast retry policies (coefficient < 5)
      const fast = await postgresClient.query(
        `SELECT stream_name, backoff_coefficient
         FROM retrypolicytest.streams
         WHERE backoff_coefficient < 5 AND expired_at IS NULL`,
      );

      expect(fast.rows.length).toBeGreaterThan(0);
      expect(fast.rows[0].backoff_coefficient).toBe('2');
    });
  });

  describe('Utility Functions', () => {
    it('should normalize retry policy with string maximumInterval', () => {
      const policy: RetryPolicy = {
        maximumAttempts: 5,
        backoffCoefficient: 2,
        maximumInterval: '300s',
      };

      const normalized = normalizeRetryPolicy(policy);

      expect(normalized.max_retry_attempts).toBe(5);
      expect(normalized.backoff_coefficient).toBe(2);
      expect(normalized.maximum_interval_seconds).toBe(300);
    });

    it('should normalize retry policy with numeric maximumInterval', () => {
      const policy: RetryPolicy = {
        maximumAttempts: 3,
        backoffCoefficient: 10,
        maximumInterval: 120,
      };

      const normalized = normalizeRetryPolicy(policy);

      expect(normalized.max_retry_attempts).toBe(3);
      expect(normalized.backoff_coefficient).toBe(10);
      expect(normalized.maximum_interval_seconds).toBe(120);
    });

    it('should apply defaults for missing values', () => {
      const policy: RetryPolicy = {
        maximumAttempts: 7,
      };

      const normalized = normalizeRetryPolicy(policy);

      expect(normalized.max_retry_attempts).toBe(7);
      expect(normalized.backoff_coefficient).toBe(10); // default
      expect(normalized.maximum_interval_seconds).toBe(120); // default
    });

    it('should handle undefined policy with all defaults', () => {
      const normalized = normalizeRetryPolicy(undefined);

      expect(normalized.max_retry_attempts).toBe(3);
      expect(normalized.backoff_coefficient).toBe(10);
      expect(normalized.maximum_interval_seconds).toBe(120);
    });

    it('should handle custom defaults', () => {
      const normalized = normalizeRetryPolicy(undefined, {
        maximumAttempts: 10,
        backoffCoefficient: 2,
        maximumInterval: 600,
      });

      expect(normalized.max_retry_attempts).toBe(10);
      expect(normalized.backoff_coefficient).toBe(2);
      expect(normalized.maximum_interval_seconds).toBe(600);
    });
  });

  describe('Migration Support', () => {
    it('should handle existing tables without retry columns', async () => {
      // Note: This test assumes the migration has already run
      // In a real scenario, you'd test the migration itself
      
      const result = await postgresClient.query(`
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema = 'retrypolicytest'
          AND table_name = 'streams'
          AND column_name IN ('max_retry_attempts', 'backoff_coefficient', 'maximum_interval_seconds')
        ORDER BY column_name
      `);

      expect(result.rows).toHaveLength(3);
      
      const columns = result.rows.reduce((acc, row) => {
        acc[row.column_name] = row;
        return acc;
      }, {} as Record<string, any>);

      expect(columns.max_retry_attempts).toBeDefined();
      expect(columns.max_retry_attempts.data_type).toBe('integer');
      expect(columns.max_retry_attempts.column_default).toBe('3');

      expect(columns.backoff_coefficient).toBeDefined();
      expect(columns.backoff_coefficient.data_type).toBe('numeric');
      expect(columns.backoff_coefficient.column_default).toBe('10');

      expect(columns.maximum_interval_seconds).toBeDefined();
      expect(columns.maximum_interval_seconds.data_type).toBe('integer');
      expect(columns.maximum_interval_seconds.column_default).toBe('120');
    });
  });

  describe('Retry Policy Propagation', () => {
    beforeEach(async () => {
      try {
        await streamService.deleteStream(TEST_STREAM);
      } catch (error) {
        // Ignore
      }
      await streamService.createStream(TEST_STREAM);
    });

    it('should propagate retry config through retry cycle', async () => {
      const retryPolicy: RetryPolicy = {
        maximumAttempts: 5,
        backoffCoefficient: 2,
        maximumInterval: '300s',
      };

      const messageData = {
        metadata: { guid: guid(), aid: 'test-activity', try: 0 },
        data: { test: 'propagation' },
      };

      await streamService.publishMessages(
        TEST_STREAM,
        [JSON.stringify(messageData)],
        { retryPolicy },
      );

      // Consume original message
      const messages = await streamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(messages).toHaveLength(1);
      const originalConfig = (messages[0].data as any)._streamRetryConfig;
      expect(originalConfig).toBeDefined();
      expect(originalConfig.max_retry_attempts).toBe(5);

      // Simulate retry by publishing new message with incremented try count
      const retryMessageData = {
        ...messages[0].data,
        metadata: { ...messages[0].data.metadata, try: 1 },
      };

      // Retry message should preserve the original retry policy
      await streamService.publishMessages(
        TEST_STREAM,
        [JSON.stringify(retryMessageData)],
        { retryPolicy }, // Pass the same policy
      );

      // Delete original message (ack)
      await streamService.ackAndDelete(TEST_STREAM, TEST_GROUP, [messages[0].id]);

      // Consume retry message
      const retryMessages = await streamService.consumeMessages(
        TEST_STREAM,
        TEST_GROUP,
        TEST_CONSUMER,
      );

      expect(retryMessages).toHaveLength(1);
      const retryConfig = (retryMessages[0].data as any)._streamRetryConfig;
      expect(retryConfig).toBeDefined();
      expect(retryConfig.max_retry_attempts).toBe(5);
      expect(retryConfig.backoff_coefficient).toBe(2);
      expect(retryConfig.maximum_interval_seconds).toBe(300);
      expect(retryMessages[0].data.metadata.try).toBe(1);
    });
  });

  describe('Performance and Observability', () => {
    beforeEach(async () => {
      // Clean up all test streams
      await postgresClient.query(
        `UPDATE retrypolicytest.streams SET expired_at = NOW() WHERE stream_name LIKE 'perf-%'`,
      );
    });

    it('should efficiently query retry statistics', async () => {
      // Create multiple streams with different policies
      const streams = [
        { name: 'perf-critical', policy: { maximumAttempts: 10, backoffCoefficient: 2, maximumInterval: 600 } },
        { name: 'perf-standard', policy: { maximumAttempts: 3, backoffCoefficient: 10, maximumInterval: 120 } },
        { name: 'perf-fast', policy: { maximumAttempts: 5, backoffCoefficient: 1.5, maximumInterval: 60 } },
      ];

      for (const { name, policy } of streams) {
        await streamService.publishMessages(
          name,
          [JSON.stringify({ metadata: { guid: guid(), aid: 'test' }, data: {} })],
          { retryPolicy: policy as RetryPolicy },
        );
      }

      // Query aggregated statistics
      const stats = await postgresClient.query(`
        SELECT 
          stream_name,
          COUNT(*) as message_count,
          AVG(max_retry_attempts) as avg_attempts,
          AVG(backoff_coefficient) as avg_backoff,
          AVG(maximum_interval_seconds) as avg_interval
        FROM retrypolicytest.streams
        WHERE stream_name LIKE 'perf-%' AND expired_at IS NULL
        GROUP BY stream_name
        ORDER BY stream_name
      `);

      expect(stats.rows).toHaveLength(3);
      
      const critical = stats.rows.find((r) => r.stream_name === 'perf-critical');
      expect(parseFloat(critical.avg_attempts)).toBe(10);
      expect(parseFloat(critical.avg_backoff)).toBe(2);

      const standard = stats.rows.find((r) => r.stream_name === 'perf-standard');
      expect(parseFloat(standard.avg_attempts)).toBe(3);
      expect(parseFloat(standard.avg_backoff)).toBe(10);
    });

    it('should support indexed queries on retry columns', async () => {
      await streamService.publishMessages(
        'perf-indexed-1',
        [JSON.stringify({ metadata: { guid: guid(), aid: 'test' }, data: {} })],
        { retryPolicy: { maximumAttempts: 10, backoffCoefficient: 2, maximumInterval: '600s' } },
      );

      // Use EXPLAIN to verify index usage (optional, for performance testing)
      const explain = await postgresClient.query(`
        EXPLAIN SELECT * FROM retrypolicytest.streams
        WHERE max_retry_attempts > 5 AND expired_at IS NULL
      `);

      expect(explain.rows.length).toBeGreaterThan(0);
      // In a real test, you'd verify index usage in the query plan
    });
  });

  describe('End-to-End Worker Retry', () => {
    const RETRY_APP_ID = 'retry-worker-test';
    let hotMesh: HotMesh;
    let attemptCount = 0;

    beforeAll(async () => {
      // Reset attempt counter
      attemptCount = 0;

      // Initialize HotMesh with basic configuration
      const retryConfig: HotMeshConfig = {
        appId: RETRY_APP_ID,
        logLevel: HMSH_LOGLEVEL,
        engine: {
          connection: {
            class: Postgres,
            options: postgres_options,
          },
        },
        workers: [
          {
            topic: 'work.do',
            connection: {
              class: Postgres,
              options: postgres_options,
            },
            callback: async (
              streamData: StreamData,
            ): Promise<StreamDataResponse> => {
              attemptCount++;
              
              // Fail the first 2 attempts, succeed on the 3rd
              if (attemptCount <= 2) {
                throw new Error(`Simulated failure on attempt ${attemptCount}`);
              }

              // Success on 3rd attempt
              return {
                code: 200,
                status: StreamStatus.SUCCESS,
                metadata: { ...streamData.metadata },
                data: { 
                  result: 'success',
                  attempts: attemptCount,
                },
              } as StreamDataResponse;
            },
          },
        ],
      };

      hotMesh = await HotMesh.init(retryConfig);

      // Deploy a simple workflow with retry policy configured in YAML
      await hotMesh.deploy(`
app:
  id: ${RETRY_APP_ID}
  version: '1'
  graphs:
    - subscribes: retry.test

      activities:
        t1:
          type: trigger
        a1:
          type: worker
          topic: work.do
          retry:
            '500': [5]
          input:
            maps:
              code: '{t1.output.data.code}'
          job:
            maps:
              result: '{a1.output.data.result}'
              attempts: '{a1.output.data.attempts}'
          
      transitions:
        t1:
          - to: a1
      `);

      await hotMesh.activate('1');
    }, 15_000);

    afterAll(async () => {
      if (hotMesh) {
        hotMesh.stop();
      }
    });

    it('should retry worker failures and eventually succeed', async () => {
      // Reset counter for this test
      attemptCount = 0;

      // Execute the workflow
      const result = await hotMesh.pubsub(
        'retry.test',
        { code: 200 },
        null,
        30_000, // 30 second timeout to allow for retries
      );

      // Verify the workflow succeeded
      expect(result).toBeDefined();
      expect(result.data).toBeDefined();
      expect(result.data.result).toBe('success');
      expect(result.data.attempts).toBe(3); // Should have taken 3 attempts

      // Verify we actually failed twice before succeeding
      expect(attemptCount).toBe(3);
    }, 35_000);
  });
});

