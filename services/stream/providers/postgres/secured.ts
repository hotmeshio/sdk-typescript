/**
 * Secured SQL adapter for worker routers.
 *
 * Replaces raw SQL with calls to SECURITY DEFINER stored procedures.
 * Workers using this adapter can only access their allowed streams —
 * the stored procedures validate `app.allowed_streams` before executing.
 */

import { sleepFor } from '../../../../modules/utils';
import { ILogger } from '../../../logger';
import { parseStreamMessage } from '../../../../modules/utils';
import { PostgresClientType } from '../../../../types/postgres';
import { ProviderClient } from '../../../../types/provider';
import { StreamMessage } from '../../../../types/stream';

/**
 * Dequeue messages from worker_streams via stored procedure.
 */
export async function fetchMessagesSecured(
  client: PostgresClientType & ProviderClient,
  schema: string,
  streamName: string,
  consumerName: string,
  options: {
    batchSize?: number;
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
  const batchSize = options?.batchSize || 1;
  const reservationTimeout = options?.reservationTimeout || 30;

  let backoff = initialBackoff;
  let retries = 0;

  try {
    while (retries < maxRetries) {
      retries++;

      const res = await client.query(
        `SELECT * FROM ${schema}.worker_dequeue($1, $2, $3, $4)`,
        [streamName, batchSize, consumerName, reservationTimeout],
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

        if (row.workflow_name) {
          if (data.metadata) {
            data.metadata.wfn = row.workflow_name;
          }
        }

        return {
          id: row.id.toString(),
          data,
          retry:
            row.max_retry_attempts !== null && !hasDefaultRetryPolicy
              ? {
                  maximumAttempts: row.max_retry_attempts,
                  backoffCoefficient: parseFloat(row.backoff_coefficient),
                  maximumInterval: row.maximum_interval_seconds,
                }
              : undefined,
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
    logger.error(`postgres-secured-dequeue-error-${streamName}`, { error });
    throw error;
  }
}

/**
 * Ack/delete messages via stored procedure.
 */
export async function ackAndDeleteSecured(
  client: PostgresClientType & ProviderClient,
  schema: string,
  streamName: string,
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  try {
    const ids = messageIds.map((id) => parseInt(id));
    const res = await client.query(
      `SELECT ${schema}.worker_ack($1, $2)`,
      [streamName, ids],
    );
    return res.rows[0]?.worker_ack ?? messageIds.length;
  } catch (error) {
    logger.error(`postgres-secured-ack-error-${streamName}`, { error });
    throw error;
  }
}

/**
 * Dead-letter messages via stored procedure.
 */
export async function deadLetterMessagesSecured(
  client: PostgresClientType & ProviderClient,
  schema: string,
  streamName: string,
  messageIds: string[],
  logger: ILogger,
): Promise<number> {
  try {
    const ids = messageIds.map((id) => parseInt(id));
    const res = await client.query(
      `SELECT ${schema}.worker_dead_letter($1, $2)`,
      [streamName, ids],
    );
    return res.rows[0]?.worker_dead_letter ?? 0;
  } catch (error) {
    logger.error(`postgres-secured-dead-letter-error-${streamName}`, {
      error,
      messageIds,
    });
    throw error;
  }
}

/**
 * Publish a response to engine_streams via stored procedure.
 */
export async function publishMessagesSecured(
  client: PostgresClientType & ProviderClient,
  schema: string,
  streamName: string,
  messages: string[],
  logger: ILogger,
): Promise<string[]> {
  try {
    const ids: string[] = [];
    for (const msg of messages) {
      const data = JSON.parse(msg);
      const retryConfig = data._streamRetryConfig;
      const visibilityDelayMs = data._visibilityDelayMs;
      const retryAttempt = data._retryAttempt ?? 0;

      // Remove internal fields
      delete data._streamRetryConfig;
      delete data._visibilityDelayMs;
      delete data._retryAttempt;

      const cleanMessage = JSON.stringify(data);

      const visibleAt =
        visibilityDelayMs && visibilityDelayMs > 0
          ? new Date(Date.now() + visibilityDelayMs).toISOString()
          : new Date().toISOString();

      const hasRetryConfig =
        retryConfig && 'max_retry_attempts' in retryConfig;

      const res = await client.query(
        `SELECT ${schema}.worker_respond($1, $2, $3, $4, $5, $6, $7)`,
        [
          streamName,
          cleanMessage,
          hasRetryConfig ? retryConfig.max_retry_attempts : null,
          hasRetryConfig ? retryConfig.backoff_coefficient : null,
          hasRetryConfig ? retryConfig.maximum_interval_seconds : null,
          visibleAt,
          retryAttempt,
        ],
      );
      ids.push(res.rows[0]?.worker_respond?.toString() ?? '0');
    }
    return ids;
  } catch (error) {
    logger.error(`postgres-secured-respond-error-${streamName}`, { error });
    throw error;
  }
}
