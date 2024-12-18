import { ILogger } from '../../../logger';
import { KeyService, KeyType } from '../../../../modules/key';
import { parseStreamMessage, sleepFor } from '../../../../modules/utils';
import { StreamService } from '../../index';
import { KeyStoreParams, StringAnyType } from '../../../../types';
import { PostgresClientType } from '../../../../types/postgres';
import {
  PublishMessageConfig,
  StreamConfig,
  StreamMessage,
  StreamStats,
} from '../../../../types/stream';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';

import { deploySchema } from './kvtables';

class PostgresStreamService extends StreamService<
  PostgresClientType & ProviderClient,
  any
> {
  namespace: string;
  appId: string;
  logger: ILogger;

  constructor(
    streamClient: PostgresClientType & ProviderClient,
    storeClient: ProviderClient,
    config: StreamConfig = {},
  ) {
    super(streamClient, storeClient, config);
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.appId = appId;
    this.logger = logger;
    await deploySchema(this.streamClient, this.appId, this.logger);
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, {
      ...params,
      appId: this.appId,
    });
  }

  transact(): ProviderTransaction {
    return {} as ProviderTransaction;
  }

  getTableName(): string {
    return `${this.safeName(this.appId)}.streams`;
  }

  safeName(appId: string): string {
    return appId.replace(/[^a-zA-Z0-9_]/g, '_');
  }

  async createStream(streamName: string): Promise<boolean> {
    return true;
  }

  async deleteStream(streamName: string): Promise<boolean> {
    const client = this.streamClient;
    const tableName = this.getTableName();
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
      this.logger.error(`postgres-stream-delete-error-${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    return true;
  }

  async deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      await client.query(
        `DELETE FROM ${tableName} WHERE stream_name = $1 AND group_name = $2`,
        [streamName, groupName],
      );
      return true;
    } catch (error) {
      this.logger.error(
        `postgres-stream-delete-group-error-${streamName}.${groupName}`,
        { error },
      );
      throw error;
    }
  }

  /**
   * `publishMessages` can be roped into a transaction by the `store`
   * service. If so, it will add the SQL and params to the
   * transaction. [Process Overview]: The engine keeps a reference
   * to the `store` and `stream` providers; it asks the `store` to
   * create a transaction and then starts adding store commands to the
   * transaction. The engine then calls the router to publish a
   * message using the `stream` provider (which the router keeps
   * a reference to), and provides the transaction object.
   * The `stream` provider then calls this method to generate
   * the SQL and params for the transaction (but, of course, the sql
   * is not executed until the engine calls the `exec` method on
   * the transaction object provided by `store`).
   *
   * NOTE: this strategy keeps `stream` and `store` operations separate but
   * allows calls to the stream to be roped into a single SQL transaction.
   */
  async publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): Promise<string[] | ProviderTransaction> {
    const { sql, params } = this._publishMessages(streamName, messages);
    if (
      options?.transaction &&
      typeof options.transaction.addCommand === 'function'
    ) {
      //call addCommand and return the transaction object
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
        const res = await this.streamClient.query(sql, params);
        for (const row of res.rows) {
          ids.push(row.id.toString());
        }
        return ids;
      } catch (error) {
        this.logger.error(`postgres-stream-publish-error-${streamName}`, {
          error,
        });
        throw error;
      }
    }
  }

  _publishMessages(
    streamName: string,
    messages: string[],
  ): { sql: string; params: any[] } {
    const tableName = this.getTableName();
    const groupName = streamName.endsWith(':') ? 'ENGINE' : 'WORKER';
    const insertValues = messages
      .map((_, idx) => `($1, $2, $${idx + 3})`)
      .join(', ');

    return {
      sql: `INSERT INTO ${tableName} (stream_name, group_name, message) VALUES ${insertValues} RETURNING id`,
      params: [streamName, groupName, ...messages],
    };
  }
  async consumeMessages(
    streamName: string,
    groupName: string,
    consumerName: string,
    options?: {
      batchSize?: number;
      blockTimeout?: number;
      autoAck?: boolean;
      reservationTimeout?: number; // in seconds
      enableBackoff?: boolean;     // enable backoff
      initialBackoff?: number;     // Initial backoff in ms
      maxBackoff?: number;         // Maximum backoff in ms
      maxRetries?: number;         // Maximum retries before giving up
    },
  ): Promise<StreamMessage[]> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    const enableBackoff = options?.enableBackoff ?? false;
    const initialBackoff = options?.initialBackoff ?? 100;   // Default initial backoff: 100ms
    const maxBackoff = options?.maxBackoff ?? 3000;          // Default max backoff: 3 seconds
    const maxRetries = options?.maxRetries ?? 3;             // Set a finite default, e.g., 3 retries
    
    let backoff = initialBackoff;
    let retries = 0;
  
    try {
      while (retries < maxRetries) {
        retries++;
        const batchSize = options?.batchSize || 1;
        const reservationTimeout = options?.reservationTimeout || 30;
  
        const res = await client.query(
          `WITH selected_messages AS (
            SELECT id, message
            FROM ${tableName}
            WHERE stream_name = $1
              AND group_name = $2
              AND COALESCE(reserved_at, '1970-01-01') < NOW() - INTERVAL '${reservationTimeout} seconds'
              AND expired_at IS NULL
            ORDER BY id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
          ),
          update_reservation AS (
            UPDATE ${tableName} t
            SET 
              reserved_at = NOW(),
              reserved_by = $4
            FROM selected_messages s
            WHERE t.stream_name = $1 AND t.id = s.id
            RETURNING t.id, t.message
          )
          SELECT * FROM update_reservation`,
          [streamName, groupName, batchSize, consumerName],
        );
  
        const messages: StreamMessage[] = res.rows.map((row: any) => ({
          id: row.id.toString(),
          data: parseStreamMessage(row.message),
        }));
  
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
      this.logger.error(`postgres-stream-consumer-error-${streamName}`, {
        error,
      });
      throw error;
    }
  }
  
  async ackAndDelete(
    streamName: string,
    groupName: string,
    messageIds: string[],
  ): Promise<number> {
    return await this.deleteMessages(streamName, groupName, messageIds);
  }

  async acknowledgeMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    // No-op for this implementation
    return messageIds.length;
  }

  async deleteMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    const client = this.streamClient;
    const tableName = this.getTableName();
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
      this.logger.error(`postgres-stream-delete-error-${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async retryMessages(
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

  async getStreamStats(streamName: string): Promise<StreamStats> {
    const client = this.streamClient;
    const tableName = this.getTableName();
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
      this.logger.error(`postgres-stream-stats-error-${streamName}`, { error });
      throw error;
    }
  }

  async getStreamDepth(streamName: string): Promise<number> {
    const stats = await this.getStreamStats(streamName);
    return stats.messageCount;
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    const client = this.streamClient;
    const tableName = this.getTableName();
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
      this.logger.error('postgres-stream-depth-error', { error });
      throw error;
    }
  }

  async trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number> {
    const client = this.streamClient;
    const tableName = this.getTableName();
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
      this.logger.error(`postgres-stream-trim-error-${streamName}`, { error });
      throw error;
    }
  }

  getProviderSpecificFeatures() {
    return {
      supportsBatching: true,
      supportsDeadLetterQueue: false,
      supportsOrdering: true,
      supportsTrimming: true,
      supportsRetry: false,
      maxMessageSize: 1024 * 1024,
      maxBatchSize: 256,
    };
  }
}

export { PostgresStreamService };
