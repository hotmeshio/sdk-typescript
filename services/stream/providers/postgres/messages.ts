import { ILogger } from '../../../logger';
import { parseStreamMessage, sleepFor, normalizeRetryPolicy } from '../../../../modules/utils';
import { StringAnyType } from '../../../../types';
import { PostgresClientType } from '../../../../types/postgres';
import {
  PublishMessageConfig,
  StreamMessage,
} from '../../../../types/stream';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';

/**
 * Publish messages to a stream. Can be used within a transaction.
 * 
 * When a transaction is provided, the SQL is added to the transaction
 * and executed atomically with other operations.
 */
export async function publishMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  messages: string[],
  options: PublishMessageConfig | undefined,
  logger: ILogger,
): Promise<string[] | ProviderTransaction> {
  const { sql, params } = buildPublishSQL(tableName, streamName, messages, options);
  
  if (
    options?.transaction &&
    typeof options.transaction.addCommand === 'function'
  ) {
    // Add to transaction and return the transaction object
    options.transaction.addCommand(
      sql,
      params,
      'array',
      (rows: { id: number }[]) => rows.map((row) => row.id.toString()),
    );
    return options.transaction as ProviderTransaction;
  } else {
    try {
      const ids: string[] = [];
      const res = await client.query(sql, params);
      for (const row of res.rows) {
        ids.push(row.id.toString());
      }
      return ids;
    } catch (error) {
      logger.error(`postgres-stream-publish-error-${streamName}`, {
        error,
      });
      throw error;
    }
  }
}

/**
 * Build SQL for publishing messages with retry policies and visibility delays.
 * Optimizes the INSERT statement based on whether retry config is present.
 */
export function buildPublishSQL(
  tableName: string,
  streamName: string,
  messages: string[],
  options?: PublishMessageConfig,
): { sql: string; params: any[] } {
  const groupName = streamName.endsWith(':') ? 'ENGINE' : 'WORKER';
  
  // Parse messages to extract retry config and visibility options
  const parsedMessages = messages.map(msg => {
    const data = JSON.parse(msg);
    const retryConfig = data._streamRetryConfig;
    const visibilityDelayMs = data._visibilityDelayMs;
    const retryAttempt = data._retryAttempt;
    
    // Remove internal fields from message payload
    delete data._streamRetryConfig;
    delete data._visibilityDelayMs;
    delete data._retryAttempt;
    
    // Determine if this message has explicit retry config
    const hasExplicitConfig = (retryConfig && 'max_retry_attempts' in retryConfig) || options?.retryPolicy;
    
    let normalizedPolicy = null;
    if (retryConfig && 'max_retry_attempts' in retryConfig) {
      normalizedPolicy = retryConfig;
    } else if (options?.retryPolicy) {
      normalizedPolicy = normalizeRetryPolicy(options.retryPolicy, {
        maximumAttempts: 3,
        backoffCoefficient: 10,
        maximumInterval: 120,
      });
    }
    
    return {
      message: JSON.stringify(data),
      hasExplicitConfig,
      retryPolicy: normalizedPolicy,
      visibilityDelayMs: visibilityDelayMs || 0,
      retryAttempt: retryAttempt || 0,
    };
  });
  
  const params: any[] = [streamName, groupName];
  let valuesClauses: string[] = [];
  let insertColumns: string;
  
  // Check if ALL messages have explicit config or ALL don't
  const allHaveConfig = parsedMessages.every(pm => pm.hasExplicitConfig);
  const noneHaveConfig = parsedMessages.every(pm => !pm.hasExplicitConfig);
  const hasVisibilityDelays = parsedMessages.some(pm => pm.visibilityDelayMs > 0);
  
  if (noneHaveConfig && !hasVisibilityDelays) {
    // Omit retry columns entirely - let DB defaults apply
    insertColumns = '(stream_name, group_name, message)';
    parsedMessages.forEach((pm, idx) => {
      const base = idx * 1;
      valuesClauses.push(`($1, $2, $${base + 3})`);
      params.push(pm.message);
    });
  } else if (noneHaveConfig && hasVisibilityDelays) {
    // Only visibility delays, no retry config
    insertColumns = '(stream_name, group_name, message, visible_at, retry_attempt)';
    parsedMessages.forEach((pm, idx) => {
      const base = idx * 2;
      if (pm.visibilityDelayMs > 0) {
        const visibleAtSQL = `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`;
        valuesClauses.push(`($1, $2, $${base + 3}, ${visibleAtSQL}, $${base + 4})`);
        params.push(pm.message, pm.retryAttempt);
      } else {
        valuesClauses.push(`($1, $2, $${base + 3}, DEFAULT, $${base + 4})`);
        params.push(pm.message, pm.retryAttempt);
      }
    });
  } else {
    // Include retry columns and optionally visibility
    insertColumns = '(stream_name, group_name, message, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, visible_at, retry_attempt)';
    parsedMessages.forEach((pm, idx) => {
      const visibleAtClause = pm.visibilityDelayMs > 0 
        ? `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`
        : 'DEFAULT';
      
      if (pm.hasExplicitConfig) {
        const paramOffset = params.length + 1; // Current param count + 1 for next param
        valuesClauses.push(`($1, $2, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, ${visibleAtClause}, $${paramOffset + 4})`);
        params.push(
          pm.message,
          pm.retryPolicy.max_retry_attempts,
          pm.retryPolicy.backoff_coefficient,
          pm.retryPolicy.maximum_interval_seconds,
          pm.retryAttempt
        );
      } else {
        // This message doesn't have config but others do - use DEFAULT keyword
        const paramOffset = params.length + 1;
        valuesClauses.push(`($1, $2, $${paramOffset}, DEFAULT, DEFAULT, DEFAULT, ${visibleAtClause}, $${paramOffset + 1})`);
        params.push(pm.message, pm.retryAttempt);
      }
    });
  }

  return {
    sql: `INSERT INTO ${tableName} ${insertColumns}
      VALUES ${valuesClauses.join(', ')} 
      RETURNING id`,
    params,
  };
}

/**
 * Fetch messages from the stream with optional exponential backoff.
 * Uses SKIP LOCKED for high-concurrency consumption.
 */
export async function fetchMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  groupName: string,
  consumerName: string,
  options: {
    batchSize?: number;
    blockTimeout?: number;
    autoAck?: boolean;
    reservationTimeout?: number; // in seconds
    enableBackoff?: boolean; // enable backoff
    initialBackoff?: number; // Initial backoff in ms
    maxBackoff?: number; // Maximum backoff in ms
    maxRetries?: number; // Maximum retries before giving up
  } = {},
  logger: ILogger,
): Promise<StreamMessage[]> {
  const enableBackoff = options?.enableBackoff ?? false;
  const initialBackoff = options?.initialBackoff ?? 100; // Default initial backoff: 100ms
  const maxBackoff = options?.maxBackoff ?? 3000; // Default max backoff: 3 seconds
  const maxRetries = options?.maxRetries ?? 3; // Set a finite default, e.g., 3 retries

  let backoff = initialBackoff;
  let retries = 0;

  try {
    while (retries < maxRetries) {
      retries++;
      const batchSize = options?.batchSize || 1;
      const reservationTimeout = options?.reservationTimeout || 30;

      // Simplified query for better performance - especially for notification-triggered fetches
      const res = await client.query(
        `UPDATE ${tableName} 
         SET reserved_at = NOW(), reserved_by = $4
         WHERE id IN (
           SELECT id FROM ${tableName}
           WHERE stream_name = $1 
             AND group_name = $2
             AND (reserved_at IS NULL OR reserved_at < NOW() - INTERVAL '${reservationTimeout} seconds')
             AND expired_at IS NULL
             AND visible_at <= NOW()
           ORDER BY id
           LIMIT $3
           FOR UPDATE SKIP LOCKED
         )
         RETURNING id, message, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, retry_attempt`,
        [streamName, groupName, batchSize, consumerName],
      );

      const messages: StreamMessage[] = res.rows.map((row: any) => {
        const data = parseStreamMessage(row.message);
        
        // Inject retry policy only if not using default values
        // Default values indicate old retry mechanism should be used (policies.retry)
        const hasDefaultRetryPolicy = 
          (row.max_retry_attempts === 3 || row.max_retry_attempts === 5) &&
          parseFloat(row.backoff_coefficient) === 10 &&
          row.maximum_interval_seconds === 120;
        
        if (row.max_retry_attempts !== null && !hasDefaultRetryPolicy) {
          data._streamRetryConfig = {
            max_retry_attempts: row.max_retry_attempts,
            backoff_coefficient: parseFloat(row.backoff_coefficient),
            maximum_interval_seconds: row.maximum_interval_seconds,
          };
        }
        
        // Inject retry_attempt from database
        if (row.retry_attempt !== undefined && row.retry_attempt !== null) {
          data._retryAttempt = row.retry_attempt;
        }
        
        return {
          id: row.id.toString(),
          data,
          retryPolicy: (row.max_retry_attempts !== null && !hasDefaultRetryPolicy) ? {
            maximumAttempts: row.max_retry_attempts,
            backoffCoefficient: parseFloat(row.backoff_coefficient),
            maximumInterval: row.maximum_interval_seconds,
          } : undefined,
        };
      });

      if (messages.length > 0 || !enableBackoff) {
        return messages;
      }

      // Apply backoff if enabled and no messages found
      await sleepFor(backoff);
      backoff = Math.min(backoff * 2, maxBackoff); // Exponential backoff
    }

    // Return empty array if maxRetries is reached and still no messages
    return [];
  } catch (error) {
    logger.error(`postgres-stream-consumer-error-${streamName}`, {
      error,
    });
    throw error;
  }
}

/**
 * Acknowledge messages (no-op for PostgreSQL - uses soft delete pattern).
 */
export async function acknowledgeMessages(
  messageIds: string[],
): Promise<number> {
  // No-op for this implementation
  return messageIds.length;
}

/**
 * Delete messages by soft-deleting them (setting expired_at).
 */
export async function deleteMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  groupName: string,
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  try {
    const ids = messageIds.map((id) => parseInt(id));

    // Perform a soft delete by setting `expired_at` to the current timestamp
    await client.query(
      `UPDATE ${tableName}
       SET expired_at = NOW()
       WHERE stream_name = $1 AND id = ANY($2::bigint[]) AND group_name = $3`,
      [streamName, ids, groupName],
    );

    return messageIds.length;
  } catch (error) {
    logger.error(`postgres-stream-delete-error-${streamName}`, {
      error,
    });
    throw error;
  }
}

/**
 * Acknowledge and delete messages in one operation.
 */
export async function ackAndDelete(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  groupName: string,
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  return await deleteMessages(client, tableName, streamName, groupName, messageIds, logger);
}

/**
 * Retry messages (placeholder for future implementation).
 */
export async function retryMessages(
  streamName: string,
  groupName: string,
  options?: {
    consumerName?: string;
    minIdleTime?: number;
    messageIds?: string[];
    delay?: number;
    maxRetries?: number;
    limit?: number;
  },
): Promise<StreamMessage[]> {
  // Implement retry logic if needed
  return [];
}

