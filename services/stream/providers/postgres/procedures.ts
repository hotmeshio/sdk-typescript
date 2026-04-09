/**
 * SECURITY DEFINER stored procedures for worker router access control.
 *
 * Each procedure runs as the schema owner (admin) and validates the
 * caller's `app.allowed_streams` session variable before executing.
 * Worker roles are granted EXECUTE on these procedures only — they
 * have zero direct table access.
 *
 * ## Procedures
 *
 * | Procedure | Purpose |
 * |-----------|---------|
 * | `worker_dequeue` | Fetch and reserve messages from `worker_streams` |
 * | `worker_ack` | Soft-delete (ack) messages in `worker_streams` |
 * | `worker_dead_letter` | Dead-letter messages in `worker_streams` |
 * | `worker_respond` | Publish a response into `engine_streams` |
 * | `worker_listen` | Subscribe to NOTIFY channel for a stream |
 * | `worker_unlisten` | Unsubscribe from NOTIFY channel |
 */

const STREAM_ACCESS_CHECK = `
  DECLARE
    allowed_streams TEXT[];
  BEGIN
    allowed_streams := string_to_array(current_setting('app.allowed_streams', true), ',');
    IF allowed_streams IS NULL OR NOT (p_stream_name = ANY(allowed_streams)) THEN
      RAISE EXCEPTION 'access denied: stream_name "%" not in allowed streams', p_stream_name
        USING ERRCODE = '42501';
    END IF;
`;

export function getCreateProceduresSQL(schemaName: string): string[] {
  const engineTable = `${schemaName}.engine_streams`;
  const workerTable = `${schemaName}.worker_streams`;

  return [
    // -- worker_dequeue --
    `CREATE OR REPLACE FUNCTION ${schemaName}.worker_dequeue(
      p_stream_name TEXT,
      p_batch_size INT,
      p_consumer_id TEXT,
      p_reservation_timeout_sec INT DEFAULT 30
    )
    RETURNS TABLE (
      id BIGINT,
      message TEXT,
      workflow_name TEXT,
      max_retry_attempts INT,
      backoff_coefficient NUMERIC,
      maximum_interval_seconds INT,
      retry_attempt INT
    )
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = ${schemaName}, pg_temp
    AS $$
    ${STREAM_ACCESS_CHECK}
      RETURN QUERY
      UPDATE ${workerTable} ws
      SET reserved_at = NOW(), reserved_by = p_consumer_id
      WHERE ws.id IN (
        SELECT ws2.id FROM ${workerTable} ws2
        WHERE ws2.stream_name = p_stream_name
          AND (ws2.reserved_at IS NULL OR ws2.reserved_at < NOW() - (p_reservation_timeout_sec || ' seconds')::INTERVAL)
          AND ws2.expired_at IS NULL
          AND ws2.visible_at <= NOW()
        ORDER BY ws2.id
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
      )
      RETURNING ws.id, ws.message, ws.workflow_name, ws.max_retry_attempts,
                ws.backoff_coefficient, ws.maximum_interval_seconds, ws.retry_attempt;
    END;
    $$;`,

    // -- worker_ack --
    `CREATE OR REPLACE FUNCTION ${schemaName}.worker_ack(
      p_stream_name TEXT,
      p_message_ids BIGINT[]
    )
    RETURNS INT
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = ${schemaName}, pg_temp
    AS $$
    DECLARE
      affected INT;
    ${STREAM_ACCESS_CHECK}
      UPDATE ${workerTable}
      SET expired_at = NOW()
      WHERE stream_name = p_stream_name AND id = ANY(p_message_ids);
      GET DIAGNOSTICS affected = ROW_COUNT;
      RETURN affected;
    END;
    $$;`,

    // -- worker_dead_letter --
    `CREATE OR REPLACE FUNCTION ${schemaName}.worker_dead_letter(
      p_stream_name TEXT,
      p_message_ids BIGINT[]
    )
    RETURNS INT
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = ${schemaName}, pg_temp
    AS $$
    DECLARE
      affected INT;
    ${STREAM_ACCESS_CHECK}
      UPDATE ${workerTable}
      SET dead_lettered_at = NOW(), expired_at = NOW()
      WHERE stream_name = p_stream_name AND id = ANY(p_message_ids);
      GET DIAGNOSTICS affected = ROW_COUNT;
      RETURN affected;
    END;
    $$;`,

    // -- worker_respond --
    // Inserts into engine_streams. The engine stream_name is the appId
    // (hardcoded from the schema name, not caller-controlled).
    `CREATE OR REPLACE FUNCTION ${schemaName}.worker_respond(
      p_stream_name TEXT,
      p_message TEXT,
      p_max_retry_attempts INT DEFAULT NULL,
      p_backoff_coefficient NUMERIC DEFAULT NULL,
      p_maximum_interval_seconds INT DEFAULT NULL,
      p_visible_at TIMESTAMPTZ DEFAULT NOW(),
      p_retry_attempt INT DEFAULT 0
    )
    RETURNS BIGINT
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = ${schemaName}, pg_temp
    AS $$
    DECLARE
      new_id BIGINT;
      engine_stream_name TEXT;
    ${STREAM_ACCESS_CHECK}
      -- Engine stream_name is the schema/appId name
      engine_stream_name := '${schemaName}';

      IF p_max_retry_attempts IS NOT NULL THEN
        INSERT INTO ${engineTable}
          (stream_name, message, max_retry_attempts, backoff_coefficient, maximum_interval_seconds, visible_at, retry_attempt)
        VALUES
          (engine_stream_name, p_message, p_max_retry_attempts, p_backoff_coefficient, p_maximum_interval_seconds, p_visible_at, p_retry_attempt)
        RETURNING id INTO new_id;
      ELSE
        INSERT INTO ${engineTable}
          (stream_name, message, visible_at, retry_attempt)
        VALUES
          (engine_stream_name, p_message, p_visible_at, p_retry_attempt)
        RETURNING id INTO new_id;
      END IF;

      RETURN new_id;
    END;
    $$;`,

    // -- worker_listen --
    `CREATE OR REPLACE FUNCTION ${schemaName}.worker_listen(
      p_stream_name TEXT
    )
    RETURNS VOID
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = ${schemaName}, pg_temp
    AS $$
    DECLARE
      channel_name TEXT;
    ${STREAM_ACCESS_CHECK}
      channel_name := 'wrk_' || p_stream_name;
      IF length(channel_name) > 63 THEN
        channel_name := left(channel_name, 63);
      END IF;
      EXECUTE format('LISTEN %I', channel_name);
    END;
    $$;`,

    // -- worker_unlisten --
    `CREATE OR REPLACE FUNCTION ${schemaName}.worker_unlisten(
      p_stream_name TEXT
    )
    RETURNS VOID
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = ${schemaName}, pg_temp
    AS $$
    DECLARE
      channel_name TEXT;
    ${STREAM_ACCESS_CHECK}
      channel_name := 'wrk_' || p_stream_name;
      IF length(channel_name) > 63 THEN
        channel_name := left(channel_name, 63);
      END IF;
      EXECUTE format('UNLISTEN %I', channel_name);
    END;
    $$;`,
  ];
}

export function getCreateWorkerCredentialsTableSQL(schemaName: string): string {
  return `
    CREATE TABLE IF NOT EXISTS ${schemaName}.worker_credentials (
      id SERIAL PRIMARY KEY,
      role_name TEXT NOT NULL UNIQUE,
      stream_names TEXT[] NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      revoked_at TIMESTAMPTZ,
      last_rotated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `;
}
