import { Client } from 'pg';
import config from '../../../../$setup/config';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { PostgresClientOptions, PostgresClientType } from '../../../../../types/postgres';

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
    postgresConnection = await PostgresConnection.connect('testId', Client, options);
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
      'Postgres client is not connected'
    );
  });

  it('should handle connection errors gracefully', async () => {
    const invalidOptions = { ...options, port: 1234 };
    await expect(
      PostgresConnection.connect('testId', Client, invalidOptions)
    ).rejects.toThrow('postgres-provider-connection-failed');
  });

  it('should handle closing non-existent connection without error', async () => {
    const connection = new PostgresConnection();
    await expect(connection.closeConnection(postgresClient)).resolves.not.toThrow();
  });
});
