import { Client } from 'pg';

import { HMNS, KeyType } from '../../../../../modules/key';
import { LoggerService } from '../../../../../services/logger';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { PostgresSubService } from '../../../../../services/sub/providers/postgres/postgres';
import { ProviderClient } from '../../../../../types/provider';
import { dropTables, postgres_options } from '../../../../$setup/postgres';
import { guid, sleepFor } from '../../../../../modules/utils';
import { PostgresClientType } from '../../../../../types';

describe('FUNCTIONAL | PostgresSubService', () => {
  let postgresClient: PostgresClientType & ProviderClient;
  let postgresStoreClient: PostgresClientType & ProviderClient;
  let postgresSubService: PostgresSubService;
  const TEST_APP = 'testApp';
  const TEST_MESSAGE = { id: 1, data: 'test message' };

  const logger = new LoggerService();

  beforeAll(async () => {
    // Initialize PostgreSQL connection
    postgresClient = (
      await PostgresConnection.connect(guid(), Client, postgres_options)
    ).getClient() as PostgresClientType & ProviderClient;

    postgresStoreClient = (
      await PostgresConnection.connect(guid(), Client, postgres_options)
    ).getClient() as PostgresClientType & ProviderClient;

    await dropTables(postgresClient);

    postgresSubService = new PostgresSubService(
      postgresClient,
      postgresStoreClient,
    );
    await postgresSubService.init(HMNS, TEST_APP, 'engine1', logger);
  });

  afterAll(async () => {
    await postgresClient.end();
  });

  describe('Pub/Sub Operations', () => {
    it('should publish and receive a message', async () => {
      const receivedMessages: any[] = [];

      // Subscribe to the topic
      await postgresSubService.subscribe(
        KeyType.QUORUM,
        (topic, payload) => {
          receivedMessages.push({ topic, payload });
        },
        TEST_APP,
      );

      // Publish a message
      await postgresSubService.publish(KeyType.QUORUM, TEST_MESSAGE, TEST_APP);

      // Wait for the message to be received
      await sleepFor(100); // Allow time for async delivery

      // Assertions
      expect(receivedMessages.length).toBe(1);
      expect(receivedMessages[0].topic).toBe(`${HMNS}:${TEST_APP}:q:`);
      expect(receivedMessages[0].payload).toEqual(TEST_MESSAGE);

      // Unsubscribe
      await postgresSubService.unsubscribe(KeyType.QUORUM, TEST_APP);
    });

    it('should handle multiple subscribers', async () => {
      const subscriber1Messages: any[] = [];
      const subscriber2Messages: any[] = [];

      // First subscriber
      await postgresSubService.subscribe(
        KeyType.QUORUM,
        (topic, payload) => {
          subscriber1Messages.push({ topic, payload });
        },
        TEST_APP,
      );

      // Second subscriber
      await postgresSubService.subscribe(
        KeyType.QUORUM,
        (topic, payload) => {
          subscriber2Messages.push({ topic, payload });
        },
        TEST_APP,
      );

      // Publish a message
      await postgresSubService.publish(KeyType.QUORUM, TEST_MESSAGE, TEST_APP);

      // Wait for the messages to be received
      await sleepFor(100); // Allow time for async delivery

      // Assertions
      expect(subscriber1Messages.length).toBe(1);
      expect(subscriber1Messages[0].payload).toEqual(TEST_MESSAGE);

      expect(subscriber2Messages.length).toBe(1);
      expect(subscriber2Messages[0].payload).toEqual(TEST_MESSAGE);

      // Unsubscribe
      await postgresSubService.unsubscribe(KeyType.QUORUM, TEST_APP);
    });

    it('should not receive messages after unsubscribing', async () => {
      const receivedMessages: any[] = [];

      // Subscribe to the topic
      await postgresSubService.subscribe(
        KeyType.QUORUM,
        (topic, payload) => {
          receivedMessages.push({ topic, payload });
        },
        TEST_APP,
      );

      // Unsubscribe
      await postgresSubService.unsubscribe(KeyType.QUORUM, TEST_APP);

      // Publish a message
      await postgresSubService.publish(KeyType.QUORUM, TEST_MESSAGE, TEST_APP);

      await sleepFor(100);
      expect(receivedMessages.length).toBe(0);
    });
  });
});
