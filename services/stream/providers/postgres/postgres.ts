// /app/services/stream/providers/postgres/postgres.ts
import { ILogger } from '../../../logger';
import { KeyService, KeyType } from '../../../../modules/key';
import { parseStreamMessage } from '../../../../modules/utils';
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
    await this.deploy();
  }

  async deploy(): Promise<void> {
    const client = this.streamClient;

    try {
      // Acquire advisory lock to prevent race conditions
      const lockId = this.getAdvisoryLockId();
      const lockResult = await client.query(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [lockId],
      );

      if (lockResult.rows[0].locked) {
        // Begin transaction
        await client.query('BEGIN');

        // Check if tables exist
        const tableExists = await this.checkIfTablesExist(client);
        if (!tableExists) {
          await this.createTables(client);
        }

        // Commit transaction
        await client.query('COMMIT');

        // Release the lock
        await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
      } else {
        // Wait for the deploy process to complete
        let retries = 0;
        const maxRetries = 20; // Wait up to 2 seconds
        while (retries < maxRetries) {
          await this.delay(100); // Wait 100 ms
          const lockCheck = await client.query(
            "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint) AS unlocked",
            [lockId],
          );
          if (lockCheck.rows[0].unlocked) {
            // Lock has been released, table should exist now
            const tableExists = await this.checkIfTablesExist(client);
            if (tableExists) {
              break;
            }
          }
          retries++;
        }
        if (retries === maxRetries) {
          throw new Error('Timeout waiting for table creation');
        }
      }
    } catch (error) {
      this.logger.error('Error deploying tables', { error });
      throw error;
    }
  }

  getAdvisoryLockId(): number {
    return this.hashStringToInt(this.appId);
  }

  hashStringToInt(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0; // Convert to 32-bit integer
    }
    return Math.abs(hash);
  }

  async checkIfTablesExist(client: PostgresClientType): Promise<boolean> {
    const res = await client.query(
      `SELECT to_regclass('public.${this.getTableName()}') AS table`,
    );
    return res.rows[0].table !== null;
  }

  getTableName(): string {
    // Use namespace and appId to generate table name
    return `stream_${this.namespace.replace(
      /[^a-zA-Z0-9_]/g,
      '_',
    )}_${this.appId.replace(/[^a-zA-Z0-9_]/g, '_')}`;
  }

  async createTables(client: PostgresClientType): Promise<void> {
    const tableName = this.getTableName();
    // Start transaction
    await client.query('BEGIN');

    // Create main table with partitioning
    await client.query(
      `CREATE TABLE ${tableName} (
        id BIGSERIAL,
        stream_name TEXT NOT NULL,
        group_name TEXT NOT NULL DEFAULT 'ENGINE',
        message TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        reserved_at TIMESTAMPTZ,
        reserved_by TEXT,
        PRIMARY KEY (stream_name, id)
      ) PARTITION BY HASH (stream_name)`,
    );

    // Create partitions
    for (let i = 0; i < 8; i++) {
      await client.query(
        `CREATE TABLE ${tableName}_part_${i} PARTITION OF ${tableName}
         FOR VALUES WITH (modulus 8, remainder ${i})`,
      );
    }

    // Create index on parent table (will be applied to partitions)
    await client.query(
      `CREATE INDEX idx_${tableName}_group_stream_reserved_at_id 
       ON ${tableName} (group_name, stream_name, reserved_at, id)`,
    );

    // Commit transaction
    await client.query('COMMIT');
  }

  delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
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

  async createStream(streamName: string): Promise<boolean> {
    // Streams are managed within the table; no action needed
    return true;
  }

  async deleteStream(streamName: string): Promise<boolean> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      await client.query('BEGIN');
      if (streamName === '*') {
        await client.query(`DELETE FROM ${tableName}`);
      } else {
        await client.query(`DELETE FROM ${tableName} WHERE stream_name = $1`, [
          streamName,
        ]);
      }
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(`Error deleting stream ${streamName}`, { error });
      throw error;
    }
  }

  async createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    // Consumer groups are managed via group_name; no action needed
    return true;
  }

  async deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      await client.query('BEGIN');
      await client.query(
        `DELETE FROM ${tableName} WHERE stream_name = $1 AND group_name = $2`,
        [streamName, groupName],
      );
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(
        `Error deleting consumer group ${groupName} for stream ${streamName}`,
        { error },
      );
      throw error;
    }
  }

  async publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): Promise<string[] | ProviderTransaction> {
    const client = options?.transaction || this.streamClient;
    const tableName = this.getTableName();
    try {
      if (!options?.transaction) {
        await client.query('BEGIN');
      }
      const ids: string[] = [];

      const groupName = streamName.endsWith(':') ? 'ENGINE' : 'WORKER';

      const insertValues = messages
        .map((_, idx) => `($1, $2, $${idx + 3})`)
        .join(', ');

      const params = [streamName, groupName, ...messages];

      const res = await client.query(
        `INSERT INTO ${tableName} (stream_name, group_name, message) VALUES ${insertValues} RETURNING id`,
        params,
      );

      for (const row of res.rows) {
        ids.push(row.id.toString());
      }

      if (!options?.transaction) {
        await client.query('COMMIT');
      }
      return ids;
    } catch (error) {
      if (!options?.transaction) {
        await client.query('ROLLBACK');
      }
      this.logger.error(`Error publishing messages to ${streamName}`, {
        error,
      });
      throw error;
    }
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
    },
  ): Promise<StreamMessage[]> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      const batchSize = options?.batchSize || 1;
      const reservationTimeout = options?.reservationTimeout || 30;

      // Simplify the WHERE clause to avoid OR conditions
      const res = await client.query(
        `WITH selected_messages AS (
          SELECT id, message
          FROM ${tableName}
          WHERE stream_name = $1
            AND group_name = $2
            AND COALESCE(reserved_at, '1970-01-01') < NOW() - INTERVAL '${reservationTimeout} seconds'
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

      return messages;
    } catch (error) {
      this.logger.error(`Error consuming messages from ${streamName}`, {
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
      await client.query('BEGIN');
      const ids = messageIds.map((id) => parseInt(id));

      await client.query(
        `DELETE FROM ${tableName}
         WHERE stream_name = $1 AND id = ANY($2::bigint[]) AND group_name = $3`,
        [streamName, ids, groupName],
      );

      await client.query('COMMIT');
      return messageIds.length;
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(`Error deleting messages from ${streamName}`, {
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
        `SELECT COUNT(*) AS count FROM ${tableName} WHERE stream_name = $1`,
        [streamName],
      );
      return {
        messageCount: parseInt(res.rows[0].count, 10),
      };
    } catch (error) {
      this.logger.error(`Error getting stats for ${streamName}`, { error });
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
      this.logger.error('Error getting multiple stream depths', { error });
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
      await client.query('BEGIN');
      let deleted = 0;

      if (options.maxLen !== undefined) {
        const res = await client.query(
          `DELETE FROM ${tableName}
           WHERE stream_name = $1 AND id IN (
             SELECT id FROM ${tableName}
             WHERE stream_name = $1
             ORDER BY id ASC
             OFFSET $2
           )`,
          [streamName, options.maxLen],
        );
        deleted += res.rowCount;
      }

      if (options.maxAge !== undefined) {
        const res = await client.query(
          `DELETE FROM ${tableName}
           WHERE stream_name = $1 AND created_at < NOW() - INTERVAL '${options.maxAge} milliseconds'`,
          [streamName],
        );
        deleted += res.rowCount;
      }

      await client.query('COMMIT');
      return deleted;
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(`Error trimming stream ${streamName}`, { error });
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
