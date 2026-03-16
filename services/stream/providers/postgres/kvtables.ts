import {
  HMSH_DEPLOYMENT_DELAY,
  HMSH_DEPLOYMENT_PAUSE,
} from '../../../../modules/enums';
import { sleepFor } from '../../../../modules/utils';
import { ILogger } from '../../../logger';

export async function deploySchema(
  streamClient: any,
  appId: string,
  logger: ILogger,
): Promise<void> {
  const isPool =
    streamClient?.totalCount !== undefined &&
    streamClient?.idleCount !== undefined;
  const client = isPool ? await streamClient.connect() : streamClient;
  const releaseClient = isPool;

  try {
    const schemaName = appId.replace(/[^a-zA-Z0-9_]/g, '_');

    // First, check if tables already exist (no lock needed)
    const tablesExist = await checkIfTablesExist(client, schemaName);
    if (tablesExist) {
      return;
    }

    // Tables don't exist, need to acquire lock and create them
    const lockId = getAdvisoryLockId(appId);
    const lockResult = await client.query(
      'SELECT pg_try_advisory_lock($1) AS locked',
      [lockId],
    );

    if (lockResult.rows[0].locked) {
      try {
        await client.query('BEGIN');

        // Double-check tables don't exist (race condition safety)
        const tablesStillMissing = !(await checkIfTablesExist(
          client,
          schemaName,
        ));
        if (tablesStillMissing) {
          await createTables(client, schemaName);
          await createNotificationTriggers(client, schemaName);
        }

        await client.query('COMMIT');
      } finally {
        await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
      }
    } else {
      // Release the client before waiting (if it's a pool connection)
      if (releaseClient && client.release) {
        await client.release();
      }

      // Wait for the deploy process to complete
      await waitForTablesCreation(
        streamClient,
        lockId,
        schemaName,
        logger,
      );
      return; // Already released client, don't release again in finally
    }
  } catch (error) {
    logger.error('Error deploying schema', { error });
    throw error;
  } finally {
    if (releaseClient && client.release) {
      try {
        await client.release();
      } catch {
        // Client may have been released already
      }
    }
  }
}

function getAdvisoryLockId(appId: string): number {
  return hashStringToInt(appId);
}

function hashStringToInt(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0; // Convert to 32-bit integer
  }
  return Math.abs(hash);
}

async function checkIfTablesExist(
  client: any,
  schemaName: string,
): Promise<boolean> {
  // Check engine_streams, worker_streams, AND roles table
  const result = await client.query(
    `SELECT
       to_regclass($1) AS engine_table,
       to_regclass($2) AS worker_table,
       to_regclass($3) AS roles_table`,
    [
      `${schemaName}.engine_streams`,
      `${schemaName}.worker_streams`,
      `${schemaName}.roles`,
    ]
  );
  return result.rows[0].engine_table !== null &&
         result.rows[0].worker_table !== null &&
         result.rows[0].roles_table !== null;
}

async function waitForTablesCreation(
  streamClient: any,
  lockId: number,
  schemaName: string,
  logger: ILogger,
): Promise<void> {
  let retries = 0;
  const maxRetries = Math.round(HMSH_DEPLOYMENT_DELAY / HMSH_DEPLOYMENT_PAUSE);

  while (retries < maxRetries) {
    await sleepFor(HMSH_DEPLOYMENT_PAUSE);

    const isPool =
      streamClient?.totalCount !== undefined &&
      streamClient?.idleCount !== undefined;
    const client = isPool ? await streamClient.connect() : streamClient;

    try {
      const tablesExist = await checkIfTablesExist(client, schemaName);
      if (tablesExist) {
        return;
      }

      // Fallback: check if the lock has been released (indicates completion)
      const lockCheck = await client.query(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint) AS unlocked",
        [lockId],
      );
      if (lockCheck.rows[0].unlocked) {
        const tablesExistAfterLock = await checkIfTablesExist(client, schemaName);
        if (tablesExistAfterLock) {
          return;
        }
      }
    } finally {
      if (isPool && client.release) {
        await client.release();
      }
    }

    retries++;
  }

  logger.error('stream-table-create-timeout', { schemaName });
  throw new Error('Timeout waiting for stream table creation');
}

async function createTables(
  client: any,
  schemaName: string,
): Promise<void> {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName};`);

  // ---- ENGINE_STREAMS table ----
  const engineTable = `${schemaName}.engine_streams`;
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${engineTable} (
      id BIGSERIAL,
      stream_name TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      reserved_at TIMESTAMPTZ,
      reserved_by TEXT,
      expired_at TIMESTAMPTZ,
      max_retry_attempts INT DEFAULT 3,
      backoff_coefficient NUMERIC DEFAULT 10,
      maximum_interval_seconds INT DEFAULT 120,
      visible_at TIMESTAMPTZ DEFAULT NOW(),
      retry_attempt INT DEFAULT 0,
      PRIMARY KEY (stream_name, id)
    ) PARTITION BY HASH (stream_name);
  `);

  for (let i = 0; i < 8; i++) {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${schemaName}.engine_streams_part_${i}
      PARTITION OF ${engineTable}
      FOR VALUES WITH (modulus 8, remainder ${i});
    `);
  }

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_active_messages
    ON ${engineTable} (stream_name, reserved_at, visible_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_message_fetch
    ON ${engineTable} (stream_name, visible_at, id)
    WHERE expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_expired_at
    ON ${engineTable} (expired_at);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_stream_name_expired_at
    ON ${engineTable} (stream_name)
    WHERE expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_processed_volume
    ON ${engineTable} (expired_at, stream_name)
    WHERE expired_at IS NOT NULL;
  `);

  // ---- WORKER_STREAMS table ----
  const workerTable = `${schemaName}.worker_streams`;
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workerTable} (
      id BIGSERIAL,
      stream_name TEXT NOT NULL,
      workflow_name TEXT NOT NULL DEFAULT '',
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      reserved_at TIMESTAMPTZ,
      reserved_by TEXT,
      expired_at TIMESTAMPTZ,
      max_retry_attempts INT DEFAULT 3,
      backoff_coefficient NUMERIC DEFAULT 10,
      maximum_interval_seconds INT DEFAULT 120,
      visible_at TIMESTAMPTZ DEFAULT NOW(),
      retry_attempt INT DEFAULT 0,
      PRIMARY KEY (stream_name, id)
    ) PARTITION BY HASH (stream_name);
  `);

  for (let i = 0; i < 8; i++) {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${schemaName}.worker_streams_part_${i}
      PARTITION OF ${workerTable}
      FOR VALUES WITH (modulus 8, remainder ${i});
    `);
  }

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_active_messages
    ON ${workerTable} (stream_name, reserved_at, visible_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_message_fetch
    ON ${workerTable} (stream_name, visible_at, id)
    WHERE expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_expired_at
    ON ${workerTable} (expired_at);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_stream_name_expired_at
    ON ${workerTable} (stream_name)
    WHERE expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_processed_volume
    ON ${workerTable} (expired_at, stream_name)
    WHERE expired_at IS NOT NULL;
  `);
}

async function createNotificationTriggers(
  client: any,
  schemaName: string,
): Promise<void> {
  const engineTable = `${schemaName}.engine_streams`;
  const workerTable = `${schemaName}.worker_streams`;

  // ---- ENGINE notification trigger ----
  await client.query(`
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_new_engine_stream_message()
    RETURNS TRIGGER AS $$
    DECLARE
      channel_name TEXT;
      payload JSON;
    BEGIN
      IF NEW.visible_at <= NOW() THEN
        channel_name := 'eng_' || NEW.stream_name;
        IF length(channel_name) > 63 THEN
          channel_name := left(channel_name, 63);
        END IF;

        payload := json_build_object(
          'stream_name', NEW.stream_name,
          'table_type', 'engine'
        );

        PERFORM pg_notify(channel_name, payload::text);
      END IF;

      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await client.query(`
    DROP TRIGGER IF EXISTS notify_engine_stream_insert ON ${engineTable};
    CREATE TRIGGER notify_engine_stream_insert
      AFTER INSERT ON ${engineTable}
      FOR EACH ROW
      EXECUTE FUNCTION ${schemaName}.notify_new_engine_stream_message();
  `);

  // ---- WORKER notification trigger ----
  await client.query(`
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_new_worker_stream_message()
    RETURNS TRIGGER AS $$
    DECLARE
      channel_name TEXT;
      payload JSON;
    BEGIN
      IF NEW.visible_at <= NOW() THEN
        channel_name := 'wrk_' || NEW.stream_name;
        IF length(channel_name) > 63 THEN
          channel_name := left(channel_name, 63);
        END IF;

        payload := json_build_object(
          'stream_name', NEW.stream_name,
          'table_type', 'worker'
        );

        PERFORM pg_notify(channel_name, payload::text);
      END IF;

      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await client.query(`
    DROP TRIGGER IF EXISTS notify_worker_stream_insert ON ${workerTable};
    CREATE TRIGGER notify_worker_stream_insert
      AFTER INSERT ON ${workerTable}
      FOR EACH ROW
      EXECUTE FUNCTION ${schemaName}.notify_new_worker_stream_message();
  `);

  // ---- Visibility timeout notification function (queries both tables) ----
  await client.query(`
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_visible_messages()
    RETURNS INTEGER AS $$
    DECLARE
      msg RECORD;
      channel_name TEXT;
      payload JSON;
      notification_count INTEGER := 0;
    BEGIN
      -- Engine streams
      FOR msg IN
        SELECT DISTINCT stream_name
        FROM ${engineTable}
        WHERE visible_at <= NOW()
          AND reserved_at IS NULL
          AND expired_at IS NULL
        LIMIT 50
      LOOP
        channel_name := 'eng_' || msg.stream_name;
        IF length(channel_name) > 63 THEN
          channel_name := left(channel_name, 63);
        END IF;

        payload := json_build_object(
          'stream_name', msg.stream_name,
          'table_type', 'engine'
        );

        PERFORM pg_notify(channel_name, payload::text);
        notification_count := notification_count + 1;
      END LOOP;

      -- Worker streams
      FOR msg IN
        SELECT DISTINCT stream_name
        FROM ${workerTable}
        WHERE visible_at <= NOW()
          AND reserved_at IS NULL
          AND expired_at IS NULL
        LIMIT 50
      LOOP
        channel_name := 'wrk_' || msg.stream_name;
        IF length(channel_name) > 63 THEN
          channel_name := left(channel_name, 63);
        END IF;

        payload := json_build_object(
          'stream_name', msg.stream_name,
          'table_type', 'worker'
        );

        PERFORM pg_notify(channel_name, payload::text);
        notification_count := notification_count + 1;
      END LOOP;

      RETURN notification_count;
    END;
    $$ LANGUAGE plpgsql;
  `);
}

export function getNotificationChannelName(
  streamName: string,
  isEngine: boolean,
): string {
  const prefix = isEngine ? 'eng_' : 'wrk_';
  const channelName = `${prefix}${streamName}`;
  // PostgreSQL channel names are limited to 63 characters
  return channelName.length > 63 ? channelName.substring(0, 63) : channelName;
}
