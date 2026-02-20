import { Client } from 'pg';

import config from '../../../../$setup/config';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { PostgresSubService } from '../../../../../services/sub/providers/postgres/postgres';
import { PostgresStreamService } from '../../../../../services/stream/providers/postgres/postgres';
import {
  PostgresClientOptions,
  PostgresClientType,
} from '../../../../../types/postgres';

describe('PostgresConnection', () => {
  let postgresConnection: PostgresConnection;
  let postgresClient: PostgresClientType;

  const options: PostgresClientOptions = {
    host: config.POSTGRES_HOST,
    port: config.POSTGRES_PORT,
    user: config.POSTGRES_USER,
    password: config.POSTGRES_PASSWORD,
    database: config.POSTGRES_DB,
    max: 20,
    idleTimeoutMillis: 30000,
  };

  beforeAll(async () => {
    postgresConnection = await PostgresConnection.connect(
      'testId',
      Client,
      options,
    );
    postgresClient = postgresConnection.getClient();
  });

  afterAll(async () => {
    await PostgresConnection.disconnectAll();
  });

  it('should connect to Postgres', () => {
    expect(postgresClient).toBeDefined();
  });

  it('should execute a query successfully', async () => {
    const result = await postgresClient.query('SELECT 1 as number');
    expect(result.rows[0].number).toBe(1);
  });

  it('should throw an error if getClient is called without a connection', async () => {
    await postgresConnection.disconnect();
    expect(() => postgresConnection.getClient()).toThrow(
      'Postgres client is not connected',
    );
  });

  it('should handle connection errors gracefully', async () => {
    const invalidOptions = { ...options, port: 1234 };
    await expect(
      PostgresConnection.connect('testId', Client, invalidOptions),
    ).rejects.toThrow('postgres-provider-connection-failed');
  });

  it('should handle closing non-existent connection without error', async () => {
    const connection = new PostgresConnection();
    await expect(
      connection.closeConnection(postgresClient),
    ).resolves.not.toThrow();
  });

  it('should reuse connections when same taskQueue is used', async () => {
    const taskQueue = 'test-task-queue';
    const options: PostgresClientOptions = {
      host: config.POSTGRES_HOST,
      port: config.POSTGRES_PORT,
      user: config.POSTGRES_USER,
      password: config.POSTGRES_PASSWORD,
      database: config.POSTGRES_DB,
      max: 20,
      idleTimeoutMillis: 30000,
    };

    // Create first connection with taskQueue
    const connection1 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId1',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // Create second connection with same taskQueue
    const connection2 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId2', 
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // They should be the same connection instance
    expect(connection1).toBe(connection2);

    // Create third connection with different taskQueue
    const connection3 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId3',
      'different-task-queue',
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // This should be a different connection
    expect(connection1).not.toBe(connection3);
  });

  it('should track connection statistics and demonstrate taskQueue pooling effectiveness', async () => {
    // Clear any existing connections
    await PostgresConnection.disconnectAll();
    
    // Get baseline stats
    const initialStats = PostgresConnection.getConnectionStats();
    expect(initialStats.totalConnections).toBe(0);
    expect(initialStats.taskQueueConnections).toBe(0);

    const taskQueue = 'monitoring-test-queue';
    const options: PostgresClientOptions = {
      host: config.POSTGRES_HOST,
      port: config.POSTGRES_PORT,
      user: config.POSTGRES_USER,
      password: config.POSTGRES_PASSWORD,
      database: config.POSTGRES_DB,
      max: 20,
      idleTimeoutMillis: 30000,
    };

    // Create first connection with taskQueue
    const connection1 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId1',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // Check stats after first connection
    const afterFirstStats = PostgresConnection.getConnectionStats();
    expect(afterFirstStats.totalConnections).toBe(1);
    expect(afterFirstStats.taskQueueConnections).toBe(1);
    expect(afterFirstStats.taskQueueDetails).toHaveLength(1);
    expect(afterFirstStats.taskQueueDetails[0].reusedCount).toBe(0);

    // Create second connection with same taskQueue (should reuse)
    const connection2 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId2',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // Should be the same connection instance
    expect(connection2).toBe(connection1);

    // Check stats after reuse
    const afterReuseStats = PostgresConnection.getConnectionStats();
    expect(afterReuseStats.totalConnections).toBe(1); // Still only 1 connection
    expect(afterReuseStats.taskQueueConnections).toBe(1);
    expect(afterReuseStats.taskQueueDetails[0].reusedCount).toBe(1); // Reused once

    // Create third connection with different taskQueue (should create new)
    const connection3 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId3',
      'different-queue',
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // Should be different connection
    expect(connection3).not.toBe(connection1);

    // Final stats check
    const finalStats = PostgresConnection.getConnectionStats();
    expect(finalStats.totalConnections).toBe(2); // Now 2 connections
    expect(finalStats.taskQueueConnections).toBe(2); // 2 taskQueue pools

    // Log stats for demonstration
    PostgresConnection.logConnectionStats();

    await PostgresConnection.disconnectAll();
  });

  it('should not use taskQueue pooling when taskQueue is undefined', async () => {
    const options: PostgresClientOptions = {
      host: config.POSTGRES_HOST,
      port: config.POSTGRES_PORT,
      user: config.POSTGRES_USER,
      password: config.POSTGRES_PASSWORD,
      database: config.POSTGRES_DB,
      max: 20,
      idleTimeoutMillis: 30000,
    };

    // Create connections without taskQueue
    const connection1 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId1',
      undefined,
      Client as any,
      options,
      { provider: 'postgres' },
    );

    const connection2 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId2',
      undefined,
      Client as any,
      options,
      { provider: 'postgres' },
    );

    // They should be different connections (no pooling without taskQueue)
    expect(connection1).not.toBe(connection2);
  });

  it('should handle multiple SubService instances with shared taskQueue connection correctly', async () => {
    // Import the SubService here to test subscription handling
    // PostgresSubService imported at top of file
    
    const taskQueue = 'shared-task-queue';
    const options: PostgresClientOptions = {
      host: config.POSTGRES_HOST,
      port: config.POSTGRES_PORT,
      user: config.POSTGRES_USER,
      password: config.POSTGRES_PASSWORD,
      database: config.POSTGRES_DB,
      max: 20,
      idleTimeoutMillis: 30000,
    };

    // Create two connections with the same taskQueue (should be reused)
    const connection1 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId1',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres', connect: true },
    );

    const connection2 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId2',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres', connect: true },
    );

    // They should be the same connection
    expect(connection1).toBe(connection2);

    // Create two SubService instances using the shared connections
    const client1 = connection1.getClient();
    const client2 = connection2.getClient();
    
    // They should be the same client instance
    expect(client1).toBe(client2);

    const subService1 = new PostgresSubService(client1, client1);
    const subService2 = new PostgresSubService(client2, client2);

    // Initialize both services
    await subService1.init('test', 'testApp', 'engine1', { debug: () => {}, error: () => {} } as any);
    await subService2.init('test', 'testApp', 'engine2', { debug: () => {}, error: () => {} } as any);

    // Both should be able to set up subscriptions without conflict
    expect(subService1).toBeDefined();
    expect(subService2).toBeDefined();

    // Cleanup both services
    await subService1.cleanup();
    await subService2.cleanup();
  });

  it('should handle multiple StreamService instances with shared taskQueue connection correctly', async () => {
    // Import the StreamService here to test notification handling
    // PostgresStreamService imported at top of file
    
    const taskQueue = 'shared-stream-task-queue';
    const options: PostgresClientOptions = {
      host: config.POSTGRES_HOST,
      port: config.POSTGRES_PORT,
      user: config.POSTGRES_USER,
      password: config.POSTGRES_PASSWORD,
      database: config.POSTGRES_DB,
      max: 20,
      idleTimeoutMillis: 30000,
    };

    // Create two connections with the same taskQueue (should be reused)
    const connection1 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId1',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres', connect: true },
    );

    const connection2 = await PostgresConnection.getOrCreateTaskQueueConnection(
      'testId2',
      taskQueue,
      Client as any,
      options,
      { provider: 'postgres', connect: true },
    );

    // They should be the same connection
    expect(connection1).toBe(connection2);

    // Create two StreamService instances using the shared connections
    const client1 = connection1.getClient();
    const client2 = connection2.getClient();
    
    // They should be the same client instance
    expect(client1).toBe(client2);

    const streamService1 = new PostgresStreamService(client1, client1, { postgres: { enableNotifications: false } });
    const streamService2 = new PostgresStreamService(client2, client2, { postgres: { enableNotifications: false } });

    // Initialize both services
    await streamService1.init('test', 'testApp', { debug: () => {}, error: () => {} } as any);
    await streamService2.init('test', 'testApp', { debug: () => {}, error: () => {} } as any);

    // Both should be able to set up streams without conflict
    expect(streamService1).toBeDefined();
    expect(streamService2).toBeDefined();

    // Test basic stream operations
    await streamService1.createStream('test-stream');
    await streamService2.createConsumerGroup('test-stream', 'test-group');

    // Cleanup both services
    await streamService1.cleanup();
    await streamService2.cleanup();
  });
});
