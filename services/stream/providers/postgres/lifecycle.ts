import { ILogger } from '../../../logger';
import { PostgresClientType } from '../../../../types/postgres';
import { ProviderClient } from '../../../../types/provider';

/**
 * Create a stream (no-op for PostgreSQL - streams are created implicitly).
 */
export async function createStream(streamName: string): Promise<boolean> {
  return true;
}

/**
 * Delete a stream or all streams from a specific table.
 */
export async function deleteStream(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  logger: ILogger,
): Promise<boolean> {
  try {
    if (streamName === '*') {
      await client.query(`DELETE FROM ${tableName}`);
    } else {
      await client.query(`DELETE FROM ${tableName} WHERE stream_name = $1`, [
        streamName,
      ]);
    }
    return true;
  } catch (error) {
    logger.error(`postgres-stream-delete-error-${streamName}`, {
      error,
    });
    throw error;
  }
}

/**
 * Create a consumer group (no-op for PostgreSQL - groups are created implicitly).
 */
export async function createConsumerGroup(
  streamName: string,
  groupName: string,
): Promise<boolean> {
  return true;
}

/**
 * Delete messages for a stream from a specific table.
 * No group_name needed since engine and worker are separate tables.
 */
export async function deleteConsumerGroup(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  logger: ILogger,
): Promise<boolean> {
  try {
    await client.query(
      `DELETE FROM ${tableName} WHERE stream_name = $1`,
      [streamName],
    );
    return true;
  } catch (error) {
    logger.error(
      `postgres-stream-delete-group-error-${streamName}`,
      { error },
    );
    throw error;
  }
}
