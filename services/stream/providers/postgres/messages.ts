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
  isEngine: boolean,
  messages: string[],
  options: PublishMessageConfig | undefined,
  logger: ILogger,
): Promise<string[] | ProviderTransaction> {
  const { sql, params } = buildPublishSQL(tableName, streamName, isEngine, messages, options);

  if (
    options?.transaction &&
    typeof options.transaction.addCommand === 'function'
  ) {
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
 * Routes to engine_streams or worker_streams based on isEngine flag.
 * Worker messages include a workflow_name column extracted from metadata.wfn.
 */
export function buildPublishSQL(
  tableName: string,
  streamName: string,
  isEngine: boolean,
  messages: string[],
  options?: PublishMessageConfig,
): { sql: string; params: any[] } {
  // Parse messages to extract retry config, visibility options, and workflow name
  const parsedMessages = messages.map(msg => {
    const data = JSON.parse(msg);
    const retryConfig = data._streamRetryConfig;
    const visibilityDelayMs = data._visibilityDelayMs;
    const retryAttempt = data._retryAttempt;

    // Remove internal fields from message payload
    delete data._streamRetryConfig;
    delete data._visibilityDelayMs;
    delete data._retryAttempt;

    // Extract metadata for worker stream columns
    const workflowName = data.metadata?.wfn || '';
    const jid = data.metadata?.jid || '';
    const aid = data.metadata?.aid || '';
    const dad = data.metadata?.dad || '';
    const msgType = data.type || '';
    const topic = data.metadata?.topic || '';

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
      workflowName,
      jid,
      aid,
      dad,
      msgType,
      topic,
    };
  });

  const params: any[] = [streamName];
  let valuesClauses: string[] = [];
  let insertColumns: string;

  const allHaveConfig = parsedMessages.every(pm => pm.hasExplicitConfig);
  const noneHaveConfig = parsedMessages.every(pm => !pm.hasExplicitConfig);
  const hasVisibilityDelays = parsedMessages.some(pm => pm.visibilityDelayMs > 0);

  if (isEngine) {
    // Engine table: no group_name, no workflow_name
    if (noneHaveConfig && !hasVisibilityDelays) {
      insertColumns = '(stream_name, message)';
      parsedMessages.forEach((pm, idx) => {
        const base = idx * 1;
        valuesClauses.push(`($1, $${base + 2})`);
        params.push(pm.message);
      });
    } else if (noneHaveConfig && hasVisibilityDelays) {
      insertColumns = '(stream_name, message, visible_at, retry_attempt)';
      parsedMessages.forEach((pm, idx) => {
        const base = idx * 2;
        if (pm.visibilityDelayMs > 0) {
          const visibleAtSQL = `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`;
          valuesClauses.push(`($1, $${base + 2}, ${visibleAtSQL}, $${base + 3})`);
          params.push(pm.message, pm.retryAttempt);
        } else {
          valuesClauses.push(`($1, $${base + 2}, DEFAULT, $${base + 3})`);
          params.push(pm.message, pm.retryAttempt);
        }
      });
    } else {
      insertColumns = '(stream_name, message, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const visibleAtClause = pm.visibilityDelayMs > 0
          ? `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`
          : 'DEFAULT';

        if (pm.hasExplicitConfig) {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, ${visibleAtClause}, $${paramOffset + 4})`);
          params.push(
            pm.message,
            pm.retryPolicy.max_retry_attempts,
            pm.retryPolicy.backoff_coefficient,
            pm.retryPolicy.maximum_interval_seconds,
            pm.retryAttempt
          );
        } else {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, DEFAULT, DEFAULT, DEFAULT, ${visibleAtClause}, $${paramOffset + 1})`);
          params.push(pm.message, pm.retryAttempt);
        }
      });
    }
  } else {
    // Worker table: includes workflow_name + export fidelity columns, no group_name
    if (noneHaveConfig && !hasVisibilityDelays) {
      insertColumns = '(stream_name, workflow_name, jid, aid, dad, msg_type, topic, message)';
      parsedMessages.forEach((pm) => {
        const paramOffset = params.length + 1;
        valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6})`);
        params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message);
      });
    } else if (noneHaveConfig && hasVisibilityDelays) {
      insertColumns = '(stream_name, workflow_name, jid, aid, dad, msg_type, topic, message, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const paramOffset = params.length + 1;
        if (pm.visibilityDelayMs > 0) {
          const visibleAtSQL = `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, ${visibleAtSQL}, $${paramOffset + 7})`);
          params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.retryAttempt);
        } else {
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, DEFAULT, $${paramOffset + 7})`);
          params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.retryAttempt);
        }
      });
    } else {
      insertColumns = '(stream_name, workflow_name, jid, aid, dad, msg_type, topic, message, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const visibleAtClause = pm.visibilityDelayMs > 0
          ? `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`
          : 'DEFAULT';

        if (pm.hasExplicitConfig) {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7}, $${paramOffset + 8}, $${paramOffset + 9}, ${visibleAtClause}, $${paramOffset + 10})`);
          params.push(
            pm.workflowName,
            pm.jid,
            pm.aid,
            pm.dad,
            pm.msgType,
            pm.topic,
            pm.message,
            pm.retryPolicy.max_retry_attempts,
            pm.retryPolicy.backoff_coefficient,
            pm.retryPolicy.maximum_interval_seconds,
            pm.retryAttempt
          );
        } else {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, DEFAULT, DEFAULT, DEFAULT, ${visibleAtClause}, $${paramOffset + 7})`);
          params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.retryAttempt);
        }
      });
    }
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
 * No group_name filter needed - the table itself determines engine vs worker.
 */
export async function fetchMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  isEngine: boolean,
  consumerName: string,
  options: {
    batchSize?: number;
    blockTimeout?: number;
    autoAck?: boolean;
    reservationTimeout?: number;
    enableBackoff?: boolean;
    initialBackoff?: number;
    maxBackoff?: number;
    maxRetries?: number;
  } = {},
  logger: ILogger,
): Promise<StreamMessage[]> {
  const enableBackoff = options?.enableBackoff ?? false;
  const initialBackoff = options?.initialBackoff ?? 100;
  const maxBackoff = options?.maxBackoff ?? 3000;
  const maxRetries = options?.maxRetries ?? 3;

  let backoff = initialBackoff;
  let retries = 0;

  // Include workflow_name in RETURNING for worker streams
  const returningClause = isEngine
    ? 'id, message, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, retry_attempt'
    : 'id, message, workflow_name, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, retry_attempt';

  try {
    while (retries < maxRetries) {
      retries++;
      const batchSize = options?.batchSize || 1;
      const reservationTimeout = options?.reservationTimeout || 30;

      const res = await client.query(
        `UPDATE ${tableName}
         SET reserved_at = NOW(), reserved_by = $3
         WHERE id IN (
           SELECT id FROM ${tableName}
           WHERE stream_name = $1
             AND (reserved_at IS NULL OR reserved_at < NOW() - INTERVAL '${reservationTimeout} seconds')
             AND expired_at IS NULL
             AND visible_at <= NOW()
           ORDER BY id
           LIMIT $2
           FOR UPDATE SKIP LOCKED
         )
         RETURNING ${returningClause}`,
        [streamName, batchSize, consumerName],
      );

      const messages: StreamMessage[] = res.rows.map((row: any) => {
        const data = parseStreamMessage(row.message);

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

        if (row.retry_attempt !== undefined && row.retry_attempt !== null) {
          data._retryAttempt = row.retry_attempt;
        }

        // Inject workflow_name from DB column into metadata for dispatch routing
        if (!isEngine && row.workflow_name) {
          if (data.metadata) {
            data.metadata.wfn = row.workflow_name;
          }
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

      await sleepFor(backoff);
      backoff = Math.min(backoff * 2, maxBackoff);
    }

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
  return messageIds.length;
}

/**
 * Delete messages by soft-deleting them (setting expired_at).
 * No group_name needed - stream_name + table is sufficient.
 */
export async function deleteMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  try {
    const ids = messageIds.map((id) => parseInt(id));

    await client.query(
      `UPDATE ${tableName}
       SET expired_at = NOW()
       WHERE stream_name = $1 AND id = ANY($2::bigint[])`,
      [streamName, ids],
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
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  return await deleteMessages(client, tableName, streamName, messageIds, logger);
}

/**
 * Move messages to the dead-letter state by setting dead_lettered_at
 * and expired_at. The message payload is preserved for inspection.
 */
export async function deadLetterMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  try {
    const ids = messageIds.map((id) => parseInt(id));
    const res = await client.query(
      `UPDATE ${tableName}
       SET dead_lettered_at = NOW(), expired_at = NOW()
       WHERE stream_name = $1 AND id = ANY($2::bigint[])`,
      [streamName, ids],
    );
    return res.rowCount;
  } catch (error) {
    logger.error(`postgres-stream-dead-letter-error-${streamName}`, {
      error,
      messageIds,
    });
    throw error;
  }
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
  return [];
}
