// /app/services/stream/providers/postgres/postgres.ts
import { ILogger } from '../../../logger';
import { KeyService, KeyType } from '../../../../modules/key';
import { parseStreamMessage } from '../../../../modules/utils';
import { StreamService } from '../../index';
import { KeyStoreParams, StringAnyType } from '../../../../types';
import {
  PostgresClientType,
  PostgresPoolClientType,
} from '../../../../types/postgres';
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
import {
  HMSH_DEPLOYMENT_DELAY,
  HMSH_DEPLOYMENT_PAUSE,
} from '../../../../modules/enums';

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
    const transactionClient = this.streamClient as
      | PostgresClientType
      | PostgresPoolClientType;

    let client: any;
    let releaseClient = false;

    if ('connect' in transactionClient && 'release' in transactionClient) {
      // It's a Pool, need to acquire a client
      client = await transactionClient.connect();
      releaseClient = true;
    } else {
      // Assume it's a connected Client
      client = transactionClient;
    }

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
        const maxRetries = Math.round(
          HMSH_DEPLOYMENT_DELAY / HMSH_DEPLOYMENT_PAUSE,
        );
        while (retries < maxRetries) {
          await this.delay(HMSH_DEPLOYMENT_PAUSE);
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
    } finally {
      if (releaseClient) {
        await client.release();
      }
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
      `SELECT to_regclass('${this.getTableName()}') AS table`,
    );
    return res.rows[0].table !== null;
  }

  safeName(appId: string) {
    return appId.replace(/[^a-zA-Z0-9_]/g, '_');
  }

  getTableName(): string {
    return `${this.safeName(this.appId)}.streams`;
  }

  async createTables(client: PostgresClientType): Promise<void> {
    const schemaName = this.safeName(this.appId);
    const tableName = this.getTableName();

    await client.query('BEGIN');

    // Create schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName};`);

    // Create main table (with partitioning)
    await client.query(
      `CREATE TABLE ${schemaName}.streams (
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
      const partitionTableName = `${schemaName}.streams_part_${i}`;
      await client.query(
        `CREATE TABLE ${partitionTableName} PARTITION OF ${tableName}
         FOR VALUES WITH (modulus 8, remainder ${i})`,
      );
    }

    // Create indexes
    await client.query(
      `CREATE INDEX idx_streams_group_stream_reserved_at_id 
       ON ${tableName} (group_name, stream_name, reserved_at, id)`,
    );

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
      if (streamName === '*') {
        await client.query(`DELETE FROM ${tableName}`);
      } else {
        await client.query(`DELETE FROM ${tableName} WHERE stream_name = $1`, [
          streamName,
        ]);
      }
      return true;
    } catch (error) {
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
      await client.query(
        `DELETE FROM ${tableName} WHERE stream_name = $1 AND group_name = $2`,
        [streamName, groupName],
      );
      return true;
    } catch (error) {
      this.logger.error(
        `Error deleting consumer group ${groupName} for stream ${streamName}`,
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
        this.logger.error(`Error publishing messages to ${streamName}`, {
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
    },
  ): Promise<StreamMessage[]> {
    const client = this.streamClient;
    const tableName = this.getTableName();
    try {
      const batchSize = options?.batchSize || 1;
      const reservationTimeout = options?.reservationTimeout || 30;

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
      const ids = messageIds.map((id) => parseInt(id));

      await client.query(
        `DELETE FROM ${tableName}
         WHERE stream_name = $1 AND id = ANY($2::bigint[]) AND group_name = $3`,
        [streamName, ids, groupName],
      );

      return messageIds.length;
    } catch (error) {
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

      return deleted;
    } catch (error) {
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
