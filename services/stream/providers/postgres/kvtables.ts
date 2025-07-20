import { ILogger } from '../../../logger';

export async function deploySchema(
  streamClient: any,
  appId: string,
  logger: ILogger,
): Promise<void> {
  const client =
    'connect' in streamClient && 'release' in streamClient
      ? await streamClient.connect()
      : streamClient;
  const releaseClient = 'release' in streamClient;

  try {
    const lockId = getAdvisoryLockId(appId);
    const lockResult = await client.query(
      'SELECT pg_try_advisory_lock($1) AS locked',
      [lockId],
    );

    if (lockResult.rows[0].locked) {
      await client.query('BEGIN');
      const schemaName = appId.replace(/[^a-zA-Z0-9_]/g, '_');
      const tableName = `${schemaName}.streams`;

      await createTables(client, schemaName, tableName);
      await createNotificationTriggers(client, schemaName, tableName);

      await client.query('COMMIT');
      await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
    } else {
      throw new Error('Table deployment in progress by another process.');
    }
  } catch (error) {
    logger.error('Error deploying schema', { error });
    throw error;
  } finally {
    if (releaseClient) {
      await client.release();
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

async function createTables(
  client: any,
  schemaName: string,
  tableName: string,
): Promise<void> {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName};`);

  // Main table creation with partitions
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id BIGSERIAL,
      stream_name TEXT NOT NULL,
      group_name TEXT NOT NULL DEFAULT 'ENGINE',
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      reserved_at TIMESTAMPTZ,
      reserved_by TEXT,
      expired_at TIMESTAMPTZ,
      PRIMARY KEY (stream_name, id)
    ) PARTITION BY HASH (stream_name);
  `);

  for (let i = 0; i < 8; i++) {
    const partitionTableName = `${schemaName}.streams_part_${i}`;
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${partitionTableName}
      PARTITION OF ${tableName}
      FOR VALUES WITH (modulus 8, remainder ${i});
    `);
  }

  // Index for active messages
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_streams_active_messages 
    ON ${tableName} (group_name, stream_name, reserved_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);

  // Optimized index for the simplified fetchMessages query
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_streams_message_fetch 
    ON ${tableName} (stream_name, group_name, id)
    WHERE expired_at IS NULL;
  `);

  // Index for expired messages
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_streams_expired_at 
    ON ${tableName} (expired_at);
  `);

  // New index for stream stats optimization
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_name_expired_at
    ON ${tableName} (stream_name)
    WHERE expired_at IS NULL;
  `);

  // TODO: revisit this index when solving automated cleanup
  // Optional index for querying by creation time, if needed
  // await client.query(`
  //   CREATE INDEX IF NOT EXISTS idx_streams_created_at
  //   ON ${tableName} (created_at);
  // `);
}

async function createNotificationTriggers(
  client: any,
  schemaName: string,
  tableName: string,
): Promise<void> {
  // Create the notification function
  await client.query(`
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_new_stream_message()
    RETURNS TRIGGER AS $$
    DECLARE
      channel_name TEXT;
      payload JSON;
    BEGIN
      -- Create channel name: stream_{stream_name}_{group_name}
      -- Truncate if too long (PostgreSQL channel names limited to 63 chars)
      channel_name := 'stream_' || NEW.stream_name || '_' || NEW.group_name;
      IF length(channel_name) > 63 THEN
        channel_name := left(channel_name, 63);
      END IF;
      
      -- Create payload with message details
      payload := json_build_object(
        'id', NEW.id,
        'stream_name', NEW.stream_name,
        'group_name', NEW.group_name,
        'created_at', extract(epoch from NEW.created_at)
      );
      
      -- Send notification
      PERFORM pg_notify(channel_name, payload::text);
      
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  // Create trigger only on the main table - it will automatically apply to all partitions
  await client.query(`
    DROP TRIGGER IF EXISTS notify_stream_insert ON ${tableName};
    CREATE TRIGGER notify_stream_insert
      AFTER INSERT ON ${tableName}
      FOR EACH ROW
      EXECUTE FUNCTION ${schemaName}.notify_new_stream_message();
  `);
}

export function getNotificationChannelName(
  streamName: string,
  groupName: string,
): string {
  const channelName = `stream_${streamName}_${groupName}`;
  // PostgreSQL channel names are limited to 63 characters
  return channelName.length > 63 ? channelName.substring(0, 63) : channelName;
}
