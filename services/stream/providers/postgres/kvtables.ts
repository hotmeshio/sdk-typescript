import {
  HMSH_DEPLOYMENT_DELAY,
  HMSH_DEPLOYMENT_PAUSE,
  HMSH_RESERVATION_TIMEOUT_S,
} from '../../../../modules/enums';
import { sleepFor } from '../../../../modules/utils';
import { ILogger } from '../../../logger';
import {
  getCreateProceduresSQL,
  getCreateWorkerCredentialsTableSQL,
} from './procedures';

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
    const tablesExist = await checkIfTablesExist(client, schemaName);

    // Acquire advisory lock for ALL DDL: table creation, index
    // migrations, and trigger setup. CREATE INDEX IF NOT EXISTS is
    // not atomic under concurrent transactions — two sessions can
    // both see the index as absent and both attempt creation,
    // causing a unique_violation on pg_class_relname_nsp_index.
    const lockId = getAdvisoryLockId(appId);
    const lockResult = await client.query(
      'SELECT pg_try_advisory_lock($1) AS locked',
      [lockId],
    );

    if (lockResult.rows[0].locked) {
      try {
        if (!tablesExist) {
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
        }

        // Always run index, procedure, and trigger migrations under the lock
        await ensureIndexes(client, schemaName);
        await ensureProcedures(client, schemaName);
        await ensureStatementLevelTriggers(client, schemaName);
        // Re-deploy the fallback poller's discovery function so existing
        // databases receive predicate changes (v0.25.6: stale-reservation
        // reclaim — see getNotifyVisibleMessagesSQL)
        await client.query(getNotifyVisibleMessagesSQL(schemaName));
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

/**
 * Ensure correct indexes exist on existing databases. Drops stale
 * PR #169 split indexes that can't serve the single-pass OR query,
 * then creates the correct indexes via IF NOT EXISTS.
 */
async function ensureIndexes(
  client: any,
  schemaName: string,
): Promise<void> {
  const engineTable = `${schemaName}.engine_streams`;
  const workerTable = `${schemaName}.worker_streams`;

  // Drop legacy indexes that don't include the priority column, plus
  // redundant ones: idx_*_expired_at duplicates the partial
  // idx_*_processed_volume for the retention purge, and
  // idx_*_stream_name_expired_at duplicates the leading column and
  // predicate of idx_*_message_fetch. Every index here is maintained on
  // each message's INSERT plus two non-HOT UPDATEs (reserve, ack).
  for (const idx of [
    'idx_engine_streams_dequeue',
    'idx_engine_streams_stale_reservations',
    'idx_worker_streams_dequeue',
    'idx_worker_streams_stale_reservations',
    'idx_engine_streams_active_messages',
    'idx_engine_streams_message_fetch',
    'idx_worker_streams_active_messages',
    'idx_worker_streams_message_fetch',
    'idx_engine_streams_expired_at',
    'idx_engine_stream_name_expired_at',
    'idx_worker_streams_expired_at',
    'idx_worker_stream_name_expired_at',
  ]) {
    await client.query(`DROP INDEX IF EXISTS ${schemaName}.${idx}`);
  }

  // Recreate with priority-aware ordering for priority DESC, id ASC dequeue
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_active_messages
    ON ${engineTable} (stream_name, priority DESC, visible_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);
  // message_fetch must match the dequeue ORDER BY (priority DESC, id)
  // exactly — placing visible_at between them forces the claim query to
  // fetch and sort the entire pending backlog instead of stopping at
  // LIMIT. visible_at and stale-reservation checks are scan filters.
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_message_fetch
    ON ${engineTable} (stream_name, priority DESC, id)
    WHERE expired_at IS NULL;
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_active_messages
    ON ${workerTable} (stream_name, priority DESC, visible_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_message_fetch
    ON ${workerTable} (stream_name, priority DESC, id)
    WHERE expired_at IS NULL;
  `);

  // v0.25.6: in-flight partial indexes — bounded to currently reserved
  // rows (≈ concurrent executions) — serve the fallback poller's
  // stale-reservation reclaim branch (see getNotifyVisibleMessagesSQL)
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_inflight
    ON ${engineTable} (stream_name, reserved_at)
    WHERE expired_at IS NULL AND reserved_at IS NOT NULL;
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_inflight
    ON ${workerTable} (stream_name, reserved_at)
    WHERE expired_at IS NULL AND reserved_at IS NOT NULL;
  `);

  // v0.18.0: add jid column to engine_streams for job tracing
  await client.query(
    `ALTER TABLE ${engineTable} ADD COLUMN IF NOT EXISTS jid TEXT NOT NULL DEFAULT ''`,
  );
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_jid_created
    ON ${engineTable} (jid, created_at)
    WHERE jid != '';
  `);
}

/**
 * Re-deploy the SECURITY DEFINER stored procedures on existing
 * databases so query changes (e.g., worker_dequeue) reach deployments
 * created before the change. CREATE OR REPLACE preserves grants.
 */
async function ensureProcedures(
  client: any,
  schemaName: string,
): Promise<void> {
  for (const sql of getCreateProceduresSQL(schemaName)) {
    await client.query(sql);
  }
}

/**
 * Migrate pre-existing row-level notification triggers to the
 * statement-level form. Recreating a trigger takes an ACCESS EXCLUSIVE
 * lock on the table, so only do it when the installed trigger is still
 * row-level (tgtype bit 0 set); subsequent boots are a no-op.
 *
 * The function replacement and trigger swap MUST commit atomically:
 * between them, the still-installed row-level trigger would invoke the
 * statement-level function body, whose transition-table reference
 * (new_rows) errors under FOR EACH ROW — failing every concurrent
 * INSERT until the swap lands (or indefinitely, if the migrating
 * process dies between the two statements).
 */
async function ensureStatementLevelTriggers(
  client: any,
  schemaName: string,
): Promise<void> {
  const result = await client.query(
    `SELECT count(*) AS row_level
     FROM pg_trigger t
     JOIN pg_class c ON c.oid = t.tgrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1
       AND c.relname IN ('engine_streams', 'worker_streams')
       AND t.tgname IN ('notify_engine_stream_insert', 'notify_worker_stream_insert')
       AND (t.tgtype & 1) = 1`,
    [schemaName],
  );
  if (parseInt(result.rows[0].row_level, 10) > 0) {
    await client.query('BEGIN');
    try {
      await createNotificationTriggers(client, schemaName);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }
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
      jid TEXT NOT NULL DEFAULT '',
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      reserved_at TIMESTAMPTZ,
      reserved_by TEXT,
      expired_at TIMESTAMPTZ,
      dead_lettered_at TIMESTAMPTZ,
      max_retry_attempts INT DEFAULT 3,
      backoff_coefficient NUMERIC DEFAULT 10,
      maximum_interval_seconds INT DEFAULT 120,
      visible_at TIMESTAMPTZ DEFAULT NOW(),
      retry_attempt INT DEFAULT 0,
      priority INT NOT NULL DEFAULT 0,
      PRIMARY KEY (stream_name, id)
    ) PARTITION BY HASH (stream_name);
  `);

  for (let i = 0; i < 8; i++) {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${schemaName}.engine_streams_part_${i}
      PARTITION OF ${engineTable}
      FOR VALUES WITH (modulus 8, remainder ${i})
      WITH (fillfactor = 70);
    `);
  }

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_active_messages
    ON ${engineTable} (stream_name, priority DESC, visible_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_message_fetch
    ON ${engineTable} (stream_name, priority DESC, id)
    WHERE expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_processed_volume
    ON ${engineTable} (expired_at, stream_name)
    WHERE expired_at IS NOT NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_dead_lettered
    ON ${engineTable} (dead_lettered_at, stream_name)
    WHERE dead_lettered_at IS NOT NULL;
  `);

  // All engine messages for a job, ordered by time
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_engine_streams_jid_created
    ON ${engineTable} (jid, created_at)
    WHERE jid != '';
  `);

  // ---- WORKER_STREAMS table ----
  const workerTable = `${schemaName}.worker_streams`;
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workerTable} (
      id BIGSERIAL,
      stream_name TEXT NOT NULL,
      workflow_name TEXT NOT NULL DEFAULT '',
      jid TEXT NOT NULL DEFAULT '',
      aid TEXT NOT NULL DEFAULT '',
      dad TEXT NOT NULL DEFAULT '',
      msg_type TEXT NOT NULL DEFAULT '',
      topic TEXT NOT NULL DEFAULT '',
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      reserved_at TIMESTAMPTZ,
      reserved_by TEXT,
      expired_at TIMESTAMPTZ,
      dead_lettered_at TIMESTAMPTZ,
      max_retry_attempts INT DEFAULT 3,
      backoff_coefficient NUMERIC DEFAULT 10,
      maximum_interval_seconds INT DEFAULT 120,
      visible_at TIMESTAMPTZ DEFAULT NOW(),
      retry_attempt INT DEFAULT 0,
      priority INT NOT NULL DEFAULT 0,
      PRIMARY KEY (stream_name, id)
    ) PARTITION BY HASH (stream_name);
  `);

  for (let i = 0; i < 8; i++) {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${schemaName}.worker_streams_part_${i}
      PARTITION OF ${workerTable}
      FOR VALUES WITH (modulus 8, remainder ${i})
      WITH (fillfactor = 70);
    `);
  }

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_active_messages
    ON ${workerTable} (stream_name, priority DESC, visible_at, id)
    WHERE reserved_at IS NULL AND expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_message_fetch
    ON ${workerTable} (stream_name, priority DESC, id)
    WHERE expired_at IS NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_processed_volume
    ON ${workerTable} (expired_at, stream_name)
    WHERE expired_at IS NOT NULL;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_dead_lettered
    ON ${workerTable} (dead_lettered_at, stream_name)
    WHERE dead_lettered_at IS NOT NULL;
  `);

  // All messages for a job, ordered by time
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_jid_created
    ON ${workerTable} (jid, created_at)
    WHERE jid != '';
  `);

  // Activity-specific lookups within a job
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_jid_aid
    ON ${workerTable} (jid, aid, created_at)
    WHERE jid != '';
  `);

  // Type-filtered queries (e.g., only worker invocations + responses)
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_worker_streams_jid_type
    ON ${workerTable} (jid, msg_type, created_at)
    WHERE jid != '';
  `);

  // ---- ROLES table (used by scout for distributed leader election) ----
  const rolesTable = `${schemaName}.roles`;
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${rolesTable} (
      key TEXT PRIMARY KEY,
      value TEXT,
      expiry TIMESTAMP WITH TIME ZONE
    );
  `);

  // ---- WORKER ACCESS CONTROL ----
  // SECURITY DEFINER stored procedures for scoped worker access
  const procedureStatements = getCreateProceduresSQL(schemaName);
  for (const sql of procedureStatements) {
    await client.query(sql);
  }

  // Worker credentials registry
  await client.query(getCreateWorkerCredentialsTableSQL(schemaName));
}

async function createNotificationTriggers(
  client: any,
  schemaName: string,
): Promise<void> {
  const engineTable = `${schemaName}.engine_streams`;
  const workerTable = `${schemaName}.worker_streams`;

  // ---- ENGINE notification trigger ----
  // Statement-level with a transition table: one pg_notify per distinct
  // stream_name per INSERT statement. Row-level triggers fire pg_notify
  // per message, which both multiplies trigger overhead and serializes
  // commits on the global notification queue lock at high insert rates.
  await client.query(`
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_new_engine_stream_message()
    RETURNS TRIGGER AS $$
    DECLARE
      rec RECORD;
      channel_name TEXT;
      payload JSON;
    BEGIN
      FOR rec IN
        SELECT DISTINCT stream_name FROM new_rows WHERE visible_at <= NOW()
      LOOP
        channel_name := 'eng_' || rec.stream_name;
        IF length(channel_name) > 63 THEN
          channel_name := left(channel_name, 63);
        END IF;

        payload := json_build_object(
          'stream_name', rec.stream_name,
          'table_type', 'engine'
        );

        PERFORM pg_notify(channel_name, payload::text);
      END LOOP;

      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await client.query(`
    DROP TRIGGER IF EXISTS notify_engine_stream_insert ON ${engineTable};
    CREATE TRIGGER notify_engine_stream_insert
      AFTER INSERT ON ${engineTable}
      REFERENCING NEW TABLE AS new_rows
      FOR EACH STATEMENT
      EXECUTE FUNCTION ${schemaName}.notify_new_engine_stream_message();
  `);

  // ---- WORKER notification trigger ----
  await client.query(`
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_new_worker_stream_message()
    RETURNS TRIGGER AS $$
    DECLARE
      rec RECORD;
      channel_name TEXT;
      payload JSON;
    BEGIN
      FOR rec IN
        SELECT DISTINCT stream_name FROM new_rows WHERE visible_at <= NOW()
      LOOP
        channel_name := 'wrk_' || rec.stream_name;
        IF length(channel_name) > 63 THEN
          channel_name := left(channel_name, 63);
        END IF;

        payload := json_build_object(
          'stream_name', rec.stream_name,
          'table_type', 'worker'
        );

        PERFORM pg_notify(channel_name, payload::text);
      END LOOP;

      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await client.query(`
    DROP TRIGGER IF EXISTS notify_worker_stream_insert ON ${workerTable};
    CREATE TRIGGER notify_worker_stream_insert
      AFTER INSERT ON ${workerTable}
      REFERENCING NEW TABLE AS new_rows
      FOR EACH STATEMENT
      EXECUTE FUNCTION ${schemaName}.notify_new_worker_stream_message();
  `);

  // ---- Visibility timeout notification function (queries both tables) ----
  await client.query(getNotifyVisibleMessagesSQL(schemaName));
}

/**
 * The fallback poller's discovery function. Surfaces streams that have
 * deliverable work: unreserved visible messages AND stale reservations
 * whose holder died (heartbeats refresh reserved_at every ~15s while an
 * activity runs, so any reservation older than 60s has a dead holder or
 * is already claimable). Stale-reservation discovery is the ONLY path
 * that reclaims orphaned in-flight work on a quiet stream — without it,
 * a process death wedges its reserved messages forever. A premature
 * surface (reservation window configured above 60s on a provider with
 * static leases) costs one empty fetch; the claim query enforces the
 * real window.
 *
 * Deployed on fresh schemas AND re-deployed on every boot (see
 * deploySchema) so existing databases receive predicate changes.
 */
function getNotifyVisibleMessagesSQL(schemaName: string): string {
  const engineTable = `${schemaName}.engine_streams`;
  const workerTable = `${schemaName}.worker_streams`;
  //stale threshold tracks the configured base window: heartbeats refresh
  //reserved_at every base/2 (15s default), so live holders never look
  //stale; 2x base keeps static-lease holders (secured workers, one
  //adaptive doubling) from surfacing as false positives. Baked at
  //deploy time from the deploying node's env — a premature surface
  //costs one empty fetch (the claim query enforces the real window).
  const staleSeconds = Math.max(60, HMSH_RESERVATION_TIMEOUT_S * 2);
  return `
    CREATE OR REPLACE FUNCTION ${schemaName}.notify_visible_messages()
    RETURNS INTEGER AS $$
    DECLARE
      msg RECORD;
      channel_name TEXT;
      payload JSON;
      notification_count INTEGER := 0;
    BEGIN
      -- Engine streams: visible unreserved work (active_messages
      -- partial index) UNION stale reservations whose holder died
      -- (inflight partial index) — each branch index-matched
      FOR msg IN
        SELECT DISTINCT u.stream_name FROM (
          (SELECT stream_name FROM ${engineTable}
           WHERE visible_at <= NOW()
             AND reserved_at IS NULL
             AND expired_at IS NULL
           LIMIT 50)
          UNION ALL
          (SELECT stream_name FROM ${engineTable}
           WHERE expired_at IS NULL
             AND reserved_at IS NOT NULL
             AND reserved_at < NOW() - INTERVAL '${staleSeconds} seconds'
           LIMIT 50)
        ) u
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

      -- Worker streams: same two index-matched branches
      FOR msg IN
        SELECT DISTINCT u.stream_name FROM (
          (SELECT stream_name FROM ${workerTable}
           WHERE visible_at <= NOW()
             AND reserved_at IS NULL
             AND expired_at IS NULL
           LIMIT 50)
          UNION ALL
          (SELECT stream_name FROM ${workerTable}
           WHERE expired_at IS NULL
             AND reserved_at IS NOT NULL
             AND reserved_at < NOW() - INTERVAL '${staleSeconds} seconds'
           LIMIT 50)
        ) u
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
  `;
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
