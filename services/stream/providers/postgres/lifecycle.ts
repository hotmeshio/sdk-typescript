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
 * Delete a stream or all streams.
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
 * Delete a consumer group (removes all messages for that group).
 */
export async function deleteConsumerGroup(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  groupName: string,
  logger: ILogger,
): Promise<boolean> {
  try {
    await client.query(
      `DELETE FROM ${tableName} WHERE stream_name = $1 AND group_name = $2`,
      [streamName, groupName],
    );
    return true;
  } catch (error) {
    logger.error(
      `postgres-stream-delete-group-error-${streamName}.${groupName}`,
      { error },
    );
    throw error;
  }
}

