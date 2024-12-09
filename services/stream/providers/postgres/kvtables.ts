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
