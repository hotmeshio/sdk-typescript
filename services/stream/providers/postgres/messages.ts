import { ILogger } from '../../../logger';
import { HMSH_RESERVATION_TIMEOUT_S } from '../../../../modules/enums';
import { parseStreamMessage, sleepFor, normalizeRetryPolicy } from '../../../../modules/utils';
import { StringAnyType } from '../../../../types';
import { PostgresClientType } from '../../../../types/postgres';
import {
  PublishMessageConfig,
  StreamDataType,
  StreamMessage,
} from '../../../../types/stream';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';

const STREAM_PRIORITY_MAP: Record<string, number> = {
  [StreamDataType.INTERRUPT]: 6,
  [StreamDataType.TIMEHOOK]: 5,
  [StreamDataType.WEBHOOK]: 5,
  [StreamDataType.SIGNAL]: 5,
  [StreamDataType.TRANSITION]: 4,
  [StreamDataType.RESULT]: 3,
  [StreamDataType.RESPONSE]: 2,
  [StreamDataType.WORKER]: 1,
  [StreamDataType.AWAIT]: 0,
};

export function getMessagePriority(msgType: string | undefined): number {
  return STREAM_PRIORITY_MAP[msgType ?? ''] ?? 0;
}

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
    const hasExplicitConfig = (retryConfig && 'max_retry_attempts' in retryConfig) || options?.retry;

    let normalizedPolicy = null;
    if (retryConfig && 'max_retry_attempts' in retryConfig) {
      normalizedPolicy = retryConfig;
    } else if (options?.retry) {
      normalizedPolicy = normalizeRetryPolicy(options.retry, {
        maximumAttempts: 3,
        backoffCoefficient: 10,
        maximumInterval: 120,
      });
    }

    return {
      message: JSON.stringify(data),
      hasExplicitConfig,
      retry: normalizedPolicy,
      visibilityDelayMs: visibilityDelayMs || 0,
      retryAttempt: retryAttempt || 0,
      workflowName,
      jid,
      aid,
      dad,
      msgType,
      topic,
      priority: getMessagePriority(data.type),
    };
  });

  const params: any[] = [streamName];
  let valuesClauses: string[] = [];
  let insertColumns: string;

  const allHaveConfig = parsedMessages.every(pm => pm.hasExplicitConfig);
  const noneHaveConfig = parsedMessages.every(pm => !pm.hasExplicitConfig);
  const hasVisibilityDelays = parsedMessages.some(pm => pm.visibilityDelayMs > 0);

  if (isEngine) {
    // Engine table: includes jid for job tracing
    if (noneHaveConfig && !hasVisibilityDelays) {
      insertColumns = '(stream_name, jid, message, priority)';
      parsedMessages.forEach((pm) => {
        const paramOffset = params.length + 1;
        valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2})`);
        params.push(pm.jid, pm.message, pm.priority);
      });
    } else if (noneHaveConfig && hasVisibilityDelays) {
      insertColumns = '(stream_name, jid, message, priority, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const paramOffset = params.length + 1;
        if (pm.visibilityDelayMs > 0) {
          const visibleAtSQL = `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, ${visibleAtSQL}, $${paramOffset + 3})`);
          params.push(pm.jid, pm.message, pm.priority, pm.retryAttempt);
        } else {
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, DEFAULT, $${paramOffset + 3})`);
          params.push(pm.jid, pm.message, pm.priority, pm.retryAttempt);
        }
      });
    } else {
      insertColumns = '(stream_name, jid, message, priority, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const visibleAtClause = pm.visibilityDelayMs > 0
          ? `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`
          : 'DEFAULT';

        if (pm.hasExplicitConfig) {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, ${visibleAtClause}, $${paramOffset + 6})`);
          params.push(
            pm.jid,
            pm.message,
            pm.priority,
            pm.retry.max_retry_attempts,
            pm.retry.backoff_coefficient,
            pm.retry.maximum_interval_seconds,
            pm.retryAttempt
          );
        } else {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, DEFAULT, DEFAULT, DEFAULT, ${visibleAtClause}, $${paramOffset + 3})`);
          params.push(pm.jid, pm.message, pm.priority, pm.retryAttempt);
        }
      });
    }
  } else {
    // Worker table: includes workflow_name + export fidelity columns, no group_name
    if (noneHaveConfig && !hasVisibilityDelays) {
      insertColumns = '(stream_name, workflow_name, jid, aid, dad, msg_type, topic, message, priority)';
      parsedMessages.forEach((pm) => {
        const paramOffset = params.length + 1;
        valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7})`);
        params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.priority);
      });
    } else if (noneHaveConfig && hasVisibilityDelays) {
      insertColumns = '(stream_name, workflow_name, jid, aid, dad, msg_type, topic, message, priority, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const paramOffset = params.length + 1;
        if (pm.visibilityDelayMs > 0) {
          const visibleAtSQL = `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7}, ${visibleAtSQL}, $${paramOffset + 8})`);
          params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.priority, pm.retryAttempt);
        } else {
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7}, DEFAULT, $${paramOffset + 8})`);
          params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.priority, pm.retryAttempt);
        }
      });
    } else {
      insertColumns = '(stream_name, workflow_name, jid, aid, dad, msg_type, topic, message, priority, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, visible_at, retry_attempt)';
      parsedMessages.forEach((pm) => {
        const visibleAtClause = pm.visibilityDelayMs > 0
          ? `NOW() + INTERVAL '${pm.visibilityDelayMs} milliseconds'`
          : 'DEFAULT';

        if (pm.hasExplicitConfig) {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7}, $${paramOffset + 8}, $${paramOffset + 9}, $${paramOffset + 10}, ${visibleAtClause}, $${paramOffset + 11})`);
          params.push(
            pm.workflowName,
            pm.jid,
            pm.aid,
            pm.dad,
            pm.msgType,
            pm.topic,
            pm.message,
            pm.priority,
            pm.retry.max_retry_attempts,
            pm.retry.backoff_coefficient,
            pm.retry.maximum_interval_seconds,
            pm.retryAttempt
          );
        } else {
          const paramOffset = params.length + 1;
          valuesClauses.push(`($1, $${paramOffset}, $${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7}, DEFAULT, DEFAULT, DEFAULT, ${visibleAtClause}, $${paramOffset + 8})`);
          params.push(pm.workflowName, pm.jid, pm.aid, pm.dad, pm.msgType, pm.topic, pm.message, pm.priority, pm.retryAttempt);
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
 * Job-liveness context for the delivery guard. `keyPrefix` is the minted
 * job-key prefix (`hmsh:<app>:j:`) so `keyPrefix || jid` addresses the
 * jobs row. `enabled` is mutated to false (self-disable) when the jobs
 * table is not visible from the stream connection.
 */
export interface JobLivenessContext {
  jobsTable: string;
  keyPrefix: string;
  enabled: boolean;
}

/**
 * Soft-delete every live stream row that belongs to a job. Called when a
 * job is interrupted so its queued, reserved, and scheduled-retry
 * messages are never delivered again. Uses the partial jid indexes
 * (idx_*_streams_jid_created); runs once per interrupt.
 */
export async function expireJobMessages(
  client: PostgresClientType & ProviderClient,
  tableNames: string[],
  jid: string,
  logger: ILogger,
): Promise<number> {
  if (!jid) return 0;
  let total = 0;
  for (const tableName of tableNames) {
    try {
      const res = await client.query(
        `UPDATE ${tableName}
         SET expired_at = NOW()
         WHERE jid = $1 AND expired_at IS NULL`,
        [jid],
      );
      total += res.rowCount ?? 0;
    } catch (error) {
      logger.error(`postgres-stream-expire-job-error`, {
        tableName,
        jid,
        error,
      });
      throw error;
    }
  }
  return total;
}

/**
 * Refresh an owned reservation (heartbeat). Scoped to the owning
 * consumer and to live rows: a message that was reclaimed by another
 * consumer, acked, or expired (job interrupted) reports 0 so the
 * stale consumer can abandon its execution.
 */
export async function extendReservation(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  messageId: string,
  consumerName: string,
  logger: ILogger,
): Promise<number> {
  try {
    const res = await client.query(
      `UPDATE ${tableName}
       SET reserved_at = NOW()
       WHERE stream_name = $1 AND id = $2
         AND reserved_by = $3 AND expired_at IS NULL`,
      [streamName, parseInt(messageId), consumerName],
    );
    return res.rowCount ?? 0;
  } catch (error) {
    logger.error(`postgres-stream-extend-error-${streamName}`, {
      messageId,
      error,
    });
    throw error;
  }
}

/**
 * Delivery liveness guard. Scoped to messages that are REDELIVERIES
 * (a prior reservation lapsed) or RETRIES (retry_attempt > 0) — zombie
 * messages of interrupted jobs only resurface through those paths, so
 * first deliveries (the steady-state hot path) pay zero extra cost.
 * A suspect whose job exists, is live, and has status <= 0 is expired
 * in place and dropped from the batch. A missing job row still delivers
 * (job-creating messages precede the row). Fails open on any error;
 * self-disables when the jobs table is not visible (42P01).
 */
async function dropDeadJobMessages(
  client: PostgresClientType & ProviderClient,
  tableName: string,
  streamName: string,
  rows: any[],
  liveness: JobLivenessContext,
  logger: ILogger,
): Promise<Set<string>> {
  const deadIds = new Set<string>();
  const suspects = rows.filter(
    (row) => row.jid && (row.redelivered || row.retry_attempt > 0),
  );
  if (suspects.length === 0) {
    return deadIds;
  }
  try {
    const res = await client.query(
      `UPDATE ${tableName} t
       SET expired_at = NOW()
       FROM ${liveness.jobsTable} j
       WHERE t.stream_name = $1
         AND t.id = ANY($2::bigint[])
         AND j.key = $3 || t.jid
         AND j.is_live
         AND j.status <= 0
       RETURNING t.id`,
      [streamName, suspects.map((row) => row.id), liveness.keyPrefix],
    );
    for (const row of res.rows) {
      deadIds.add(row.id.toString());
    }
    if (deadIds.size > 0) {
      logger.warn(`postgres-stream-zombie-dropped-${streamName}`, {
        count: deadIds.size,
        jids: [
          ...new Set(
            suspects
              .filter((row) => deadIds.has(row.id.toString()))
              .map((row) => row.jid),
          ),
        ],
      });
    }
  } catch (error) {
    if (error?.code === '42P01' || error?.code === '42501') {
      //jobs table is not visible (42P01) or not readable (42501) from
      //this connection; the guard cannot run here — interrupt-time
      //purging (expireJobMessages) still applies. Self-disable so the
      //hot path stops issuing a failing cross-table query per fetch.
      liveness.enabled = false;
      logger.info('postgres-stream-liveness-guard-disabled', {
        jobsTable: liveness.jobsTable,
        code: error.code,
      });
    } else {
      logger.error(`postgres-stream-liveness-guard-error-${streamName}`, {
        error,
      });
    }
  }
  return deadIds;
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
  liveness?: JobLivenessContext,
): Promise<StreamMessage[]> {
  const enableBackoff = options?.enableBackoff ?? false;
  const initialBackoff = options?.initialBackoff ?? 100;
  const maxBackoff = options?.maxBackoff ?? 3000;
  const maxRetries = options?.maxRetries ?? 3;

  let backoff = initialBackoff;
  let retries = 0;

  // Include workflow_name in RETURNING for worker streams. Columns are
  // qualified with the update target's alias because the claim UPDATE
  // joins a CTE that also exposes an id column. Worker streams also
  // return jid and the pre-claim redelivery flag for the liveness guard.
  const returningClause = isEngine
    ? 't.id, t.message, t.max_retry_attempts, t.backoff_coefficient, t.maximum_interval_seconds, t.retry_attempt'
    : 't.id, t.message, t.workflow_name, t.max_retry_attempts, t.backoff_coefficient, t.maximum_interval_seconds, t.retry_attempt, t.jid, candidates.redelivered';

  try {
    while (retries < maxRetries) {
      retries++;
      const batchSize = options?.batchSize || 1;
      const reservationTimeout = options?.reservationTimeout || (HMSH_RESERVATION_TIMEOUT_S + 5);

      // The locking SELECT must live in a MATERIALIZED CTE: as a plain IN
      // subquery the planner may re-execute it per outer row (rows updated
      // earlier in the same command are skipped as lock candidates), which
      // reserves MORE rows than LIMIT. The UPDATE repeats stream_name so
      // the planner prunes to a single hash partition and joins on the
      // (stream_name, id) primary key.
      // candidates exposes the PRE-claim reservation state: a non-null
      // reserved_at at claim time means a prior reservation lapsed
      // (redelivery) — the trigger condition for the liveness guard.
      const res = await client.query(
        `WITH candidates AS MATERIALIZED (
           SELECT id, (reserved_at IS NOT NULL) AS redelivered FROM ${tableName}
           WHERE stream_name = $1
             AND (reserved_at IS NULL OR reserved_at < NOW() - INTERVAL '${reservationTimeout} seconds')
             AND expired_at IS NULL
             AND visible_at <= NOW()
           ORDER BY priority DESC, id
           LIMIT $2
           FOR UPDATE SKIP LOCKED
         )
         UPDATE ${tableName} t
         SET reserved_at = NOW(), reserved_by = $3
         FROM candidates
         WHERE t.stream_name = $1 AND t.id = candidates.id
         RETURNING ${returningClause}`,
        [streamName, batchSize, consumerName],
      );

      let rows = res.rows;
      if (!isEngine && liveness?.enabled && rows.length > 0) {
        const deadIds = await dropDeadJobMessages(
          client,
          tableName,
          streamName,
          rows,
          liveness,
          logger,
        );
        if (deadIds.size > 0) {
          rows = rows.filter((row: any) => !deadIds.has(row.id.toString()));
        }
      }

      const messages: StreamMessage[] = rows.map((row: any) => {
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
          retry: (row.max_retry_attempts !== null && !hasDefaultRetryPolicy) ? {
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
