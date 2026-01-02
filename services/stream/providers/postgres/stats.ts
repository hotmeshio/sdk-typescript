import { ILogger } from '../../../logger';
import { PostgresClientType } from '../../../../types/postgres';
import { StreamConfig, StreamStats } from '../../../../types/stream';
import { ProviderClient } from '../../../../types/provider';

/**
 * Get statistics for a specific stream.
 * Returns message count and other metrics.
 */
export async function getStreamStats(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  logger: ILogger,
): Promise<StreamStats> {
  try {
    const res = await client.query(
      `SELECT COUNT(*) AS available_count 
      FROM ${tableName} 
      WHERE stream_name = $1 AND expired_at IS NULL`,
      [streamName],
    );
    return {
      messageCount: parseInt(res.rows[0].available_count, 10),
    };
  } catch (error) {
    logger.error(`postgres-stream-stats-error-${streamName}`, { error });
    throw error;
  }
}

/**
 * Get the depth (message count) for a specific stream.
 */
export async function getStreamDepth(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  logger: ILogger,
): Promise<number> {
  const stats = await getStreamStats(client, tableName, streamName, logger);
  return stats.messageCount;
}

/**
 * Get depths for multiple streams in a single query.
 * More efficient than calling getStreamDepth multiple times.
 */
export async function getStreamDepths(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamNames: { stream: string }[],
  logger: ILogger,
): Promise<{ stream: string; depth: number }[]> {
  try {
    const streams = streamNames.map((s) => s.stream);

    const res = await client.query(
      `SELECT stream_name, COUNT(*) AS count
       FROM ${tableName}
       WHERE stream_name = ANY($1::text[])
       GROUP BY stream_name`,
      [streams],
    );

    const result = res.rows.map((row: any) => ({
      stream: row.stream_name,
      depth: parseInt(row.count, 10),
    }));

    return result;
  } catch (error) {
    logger.error('postgres-stream-depth-error', { error });
    throw error;
  }
}

/**
 * Trim (soft delete) messages from a stream based on age or count.
 * Returns the number of messages expired.
 */
export async function trimStream(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  options: {
    maxLen?: number;
    maxAge?: number;
    exactLimit?: boolean;
  },
  logger: ILogger,
): Promise<number> {
  try {
    let expiredCount = 0;

    if (options.maxLen !== undefined) {
      const res = await client.query(
        `WITH to_expire AS (
          SELECT id FROM ${tableName}
          WHERE stream_name = $1
          ORDER BY id ASC
          OFFSET $2
        )
        UPDATE ${tableName}
        SET expired_at = NOW()
        WHERE id IN (SELECT id FROM to_expire)`,
        [streamName, options.maxLen],
      );
      expiredCount += res.rowCount;
    }

    if (options.maxAge !== undefined) {
      const res = await client.query(
        `UPDATE ${tableName}
        SET expired_at = NOW()
        WHERE stream_name = $1 AND created_at < NOW() - INTERVAL '${options.maxAge} milliseconds'`,
        [streamName],
      );
      expiredCount += res.rowCount;
    }

    return expiredCount;
  } catch (error) {
    logger.error(`postgres-stream-trim-error-${streamName}`, { error });
    throw error;
  }
}

/**
 * Check if notifications are enabled for the provider.
 */
export function isNotificationsEnabled(config: StreamConfig = {}): boolean {
  // Allow override via environment variable
  if (process.env.HOTMESH_POSTGRES_DISABLE_NOTIFICATIONS === 'true') {
    return false;
  }
  return config?.postgres?.enableNotifications !== false; // Default: true
}

/**
 * Get provider-specific feature flags and capabilities.
 */
export function getProviderSpecificFeatures(config: StreamConfig = {}) {
  return {
    supportsBatching: true,
    supportsDeadLetterQueue: false,
    supportsOrdering: true,
    supportsTrimming: true,
    supportsRetry: false,
    supportsNotifications: isNotificationsEnabled(config),
    maxMessageSize: 1024 * 1024,
    maxBatchSize: 256,
  };
}

