import {
  HashContext,
  Multi,
  SqlResult,
  HSetOptions,
  ProviderTransaction,
  HScanResult,
} from './types';
import {
  isJobsTable,
  deriveType,
  processJobsRows,
  processRegularRows,
} from './utils';

export function createBasicOperations(context: HashContext['context']) {
  return {
    async hsetnx(
      key: string,
      field: string,
      value: string,
      multi?: ProviderTransaction,
      entity?: string,
    ): Promise<number> {
      const { sql, params } = _hset(
        context,
        key,
        { [field]: value },
        { nx: true, entity },
      );
      if (multi) {
        (multi as Multi).addCommand(sql, params, 'number');
        return Promise.resolve(0);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return res.rowCount;
        } catch (err) {
          console.error('hsetnx error', err, sql, params);
          return 0;
        }
      }
    },

    async hset(
      key: string,
      fields: Record<string, string>,
      options?: HSetOptions,
      multi?: ProviderTransaction,
    ): Promise<number | any> {
      const { sql, params } = _hset(context, key, fields, options);

      if (multi) {
        (multi as Multi).addCommand(sql, params, 'number');
        return Promise.resolve(0);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);

          // Check if this is a JSONB operation that returns a value
          const isJsonbOperation = Object.keys(fields).some(
            (k) => k.startsWith('@context:') && k !== '@context',
          );

          if (isJsonbOperation && res.rows[0]?.new_value !== undefined) {
            let returnValue;
            try {
              // Try to parse as JSON, fallback to string if it fails
              returnValue = JSON.parse(res.rows[0].new_value);
            } catch {
              returnValue = res.rows[0].new_value;
            }

            return returnValue;
          }

          return res.rowCount;
        } catch (err) {
          console.error('hset error', err, sql, params);
          return 0;
        }
      }
    },

    async hget(
      key: string,
      field: string,
      multi?: ProviderTransaction,
    ): Promise<string | null> {
      const { sql, params } = _hget(context, key, field);

      if (multi) {
        (multi as Multi).addCommand(sql, params, 'string', (rows) => {
          return rows[0]?.value || null;
        });
        return Promise.resolve(null);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return res.rows[0]?.value || null;
        } catch (error) {
          // Connection closed during test cleanup - return null
          if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
            return null;
          }
          // Re-throw unexpected errors
          throw error;
        }
      }
    },

    async hdel(
      key: string,
      fields: string[],
      multi?: unknown,
    ): Promise<number> {
      // Ensure fields is an array
      if (!Array.isArray(fields)) {
        fields = [fields];
      }
      const { sql, params } = _hdel(context, key, fields);
      if (multi) {
        (multi as unknown as Multi).addCommand(sql, params, 'number');
        return Promise.resolve(0);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return Number(res.rows[0]?.count || 0);
        } catch (error) {
          // Connection closed during test cleanup - return 0
          if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
            return 0;
          }
          // Re-throw unexpected errors
          throw error;
        }
      }
    },

    async hmget(
      key: string,
      fields: string[],
      multi?: ProviderTransaction,
    ): Promise<(string | null)[]> {
      const { sql, params } = _hmget(context, key, fields);

      const processRows = (rows: any[]) => {
        const tableName = context.tableForKey(key, 'hash');
        const isJobsTableResult = isJobsTable(tableName);

        if (isJobsTableResult) {
          return processJobsRows(rows, fields);
        } else {
          const fieldValueMap = new Map<string, string | null>();
          for (const row of rows) {
            fieldValueMap.set(row.field, row.value);
          }
          return fields.map((field) => fieldValueMap.get(field) || null);
        }
      };

      if (multi) {
        (multi as Multi).addCommand(sql, params, 'array', (rows) => {
          return processRows(rows);
        });
        return Promise.resolve([]);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return processRows(res.rows);
        } catch (err) {
          console.error('hmget error', err, sql, params);
          throw err;
        }
      }
    },

    async hgetall(
      key: string,
      multi?: ProviderTransaction,
    ): Promise<Record<string, string>> {
      const tableName = context.tableForKey(key, 'hash');
      const isJobsTableResult = isJobsTable(tableName);

      const { sql, params } = _hgetall(context, key);

      const processRows = (rows: any[]): Record<string, string> => {
        if (isJobsTableResult) {
          return processJobsRows(rows);
        } else {
          return processRegularRows(rows);
        }
      };

      if (multi) {
        (multi as Multi).addCommand(sql, params, 'object', (rows) => {
          return processRows(rows);
        });
        return Promise.resolve({});
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return processRows(res.rows);
        } catch (err) {
          console.error('hgetall error', err, sql, params);
          throw err;
        }
      }
    },

    async hincrbyfloat(
      key: string,
      field: string,
      increment: number,
      multi?: ProviderTransaction,
    ): Promise<number> {
      const { sql, params } = _hincrbyfloat(context, key, field, increment);
      if (multi) {
        (multi as Multi).addCommand(sql, params, 'number', (rows) => {
          return parseFloat(rows[0].value);
        });
        return Promise.resolve(0);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return parseFloat(res.rows[0].value);
        } catch (error) {
          // Connection closed during test cleanup - return 0
          if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
            return 0;
          }
          // Re-throw unexpected errors
          throw error;
        }
      }
    },

    /**
     * 2) KVSQL METHOD (non-transactional + transactional)
     * ---------------------------------------------------
     * Matches hincrbyfloat() pattern:
     * - when multi is present: enqueue command and return 0 immediately
     * - when multi absent: execute immediately and return parsed numeric value
     *
     * Returns: thresholdHit (0/1) as number
     */
    async setStatusAndCollateGuid(
      key: string,                  // jobKey
      statusDelta: number,          // semaphore delta
      threshold: number,            // desired threshold (usually 0)
      guidField: string,            // jobs_attributes.field for the guid ledger
      guidWeight: number,           // e.g. 100B digit increment
      multi?: ProviderTransaction,
    ): Promise<number> {
      const { sql, params } = _setStatusAndCollateGuid(
        context,
        key,
        statusDelta,
        threshold,
        guidField,
        guidWeight,
      );

      if (multi) {
        (multi as Multi).addCommand(sql, params, 'number', (rows) => {
          return parseFloat(rows[0].value);
        });
        return Promise.resolve(0);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return parseFloat(res.rows[0].value);
        } catch (error) {
          // Connection closed during test cleanup - return 0
          if (
            error?.message?.includes('closed') ||
            error?.message?.includes('queryable')
          ) {
            return 0;
          }
          throw error;
        }
      }
    },
  };
}

// Internal helper functions
export function _hset(
  context: HashContext['context'],
  key: string,
  fields: Record<string, string>,
  options?: HSetOptions,
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
  const isJobsTableResult = isJobsTable(tableName);
  const fieldEntries = Object.entries(fields);
  const isStatusOnly = fieldEntries.length === 1 && fieldEntries[0][0] === ':';

  let targetTable = tableName; // Default table name

  if (isJobsTableResult) {
    if (isStatusOnly) {
      // Target the jobs table directly when setting only the status field
      targetTable = tableName;
    } else {
      // For other fields, target the attributes table
      targetTable = `${tableName}_attributes`;
    }
  }

  const params = [];
  let sql = '';

  if (isJobsTableResult && isStatusOnly) {
    if (options?.nx) {
      // Use WHERE NOT EXISTS to enforce nx
      sql = `
        INSERT INTO ${targetTable} (id, key, status, entity)
        SELECT gen_random_uuid(), $1, $2, $3
        WHERE NOT EXISTS (
          SELECT 1 FROM ${targetTable}
          WHERE key = $1 AND is_live
        )
        RETURNING 1 as count
      `;
      params.push(key, fields[':'], options?.entity ?? null);
    } else {
      // Update existing job or insert new one
      sql = `
        INSERT INTO ${targetTable} (id, key, status, entity)
        VALUES (gen_random_uuid(), $1, $2, $3)
        ON CONFLICT (key) WHERE is_live DO UPDATE SET status = EXCLUDED.status
        RETURNING 1 as count
      `;
      params.push(key, fields[':'], options?.entity ?? null);
    }
  } else if (isJobsTableResult) {
    const schemaName = context.safeName(context.appId);
    const conflictAction = options?.nx
      ? 'ON CONFLICT DO NOTHING'
      : `ON CONFLICT (job_id, field) DO UPDATE SET value = EXCLUDED.value`;

    const placeholders = fieldEntries
      .map(([field, value], index) => {
        const baseIndex = index * 3 + 2; // Adjusted baseIndex
        params.push(field, value, deriveType(field));
        return `($${baseIndex}, $${baseIndex + 1}, $${baseIndex + 2}::${schemaName}.type_enum)`;
      })
      .join(', ');

    sql = `
      INSERT INTO ${targetTable} (job_id, field, value, type)
      SELECT 
        job.id,
        vals.field,
        vals.value,
        vals.type
      FROM (
        SELECT id FROM ${tableName} WHERE key = $1 AND is_live
      ) AS job
      CROSS JOIN (
        VALUES ${placeholders}
      ) AS vals(field, value, type)
      ${conflictAction}
      RETURNING 1 as count
    `;
    params.unshift(key); // Add key as first parameter
  } else {
    // For non-jobs tables
    const conflictAction = options?.nx
      ? 'ON CONFLICT DO NOTHING'
      : `ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value`;

    const placeholders = fieldEntries
      .map(([field, value], index) => {
        params.push(field, value);
        return `($1, $${index * 2 + 2}, $${index * 2 + 3})`;
      })
      .join(', ');

    sql = `
      INSERT INTO ${targetTable} (key, field, value)
      VALUES ${placeholders}
      ${conflictAction}
      RETURNING 1 as count
    `;
    params.unshift(key); // Add key as the first parameter
  }

  return { sql, params };
}

export function _hget(
  context: HashContext['context'],
  key: string,
  field: string,
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
  const isJobsTableResult = isJobsTable(tableName);
  const isStatusField = field === ':';
  const isContextField = field === '@';

  if (isJobsTableResult && isStatusField) {
    // Fetch status from jobs table
    const sql = `
      SELECT status::text AS value
      FROM ${tableName}
      WHERE key = $1 AND is_live
    `;
    return { sql, params: [key] };
  } else if (isJobsTableResult && isContextField) {
    // Fetch context from jobs table
    const sql = `
      SELECT context::text AS value
      FROM ${tableName}
      WHERE key = $1 AND is_live
    `;
    return { sql, params: [key] };
  } else if (isJobsTableResult) {
    // Fetch a specific field from the attributes table for a job
    const sql = `
      SELECT value
      FROM ${tableName}_attributes
      WHERE job_id = (
        SELECT id FROM ${tableName}
        WHERE key = $1 AND is_live
      )
        AND field = $2
    `;
    return { sql, params: [key, field] };
  } else {
    // Non-jobs tables
    const baseQuery = `
      SELECT value
      FROM ${tableName}
      WHERE key = $1 AND field = $2
    `;
    const sql = context.appendExpiryClause(baseQuery, tableName);
    return { sql, params: [key, field] };
  }
}

export function _hdel(
  context: HashContext['context'],
  key: string,
  fields: string[],
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
  const isJobsTableResult = isJobsTable(tableName);
  const targetTable = isJobsTableResult ? `${tableName}_attributes` : tableName;

  const fieldPlaceholders = fields.map((_, i) => `$${i + 2}`).join(', ');
  const params = [key, ...fields];

  if (isJobsTableResult) {
    const sql = `
      WITH valid_job AS (
        SELECT id
        FROM ${tableName}
        WHERE key = $1 AND is_live
      ),
      deleted AS (
        DELETE FROM ${targetTable}
        WHERE job_id IN (SELECT id FROM valid_job) AND field IN (${fieldPlaceholders})
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM deleted
    `;
    return { sql, params };
  } else {
    const sql = `
      WITH deleted AS (
        DELETE FROM ${targetTable}
        WHERE key = $1 AND field IN (${fieldPlaceholders})
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM deleted
    `;
    return { sql, params };
  }
}

export function _hmget(
  context: HashContext['context'],
  key: string,
  fields: string[],
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
  const isJobsTableResult = isJobsTable(tableName);

  if (isJobsTableResult) {
    const sql = `
      WITH valid_job AS (
        SELECT id, status, context
        FROM ${tableName}
        WHERE key = $1 
        AND (expired_at IS NULL OR expired_at > NOW())
        LIMIT 1
      ),
      job_fields AS (
        -- Include both status and context fields from jobs table
        SELECT 
          'status' AS field,
          status::text AS value
        FROM valid_job
        
        UNION ALL
        
        SELECT 
          'context' AS field,
          context::text AS value
        FROM valid_job
        
        UNION ALL
        
        -- Get attribute fields with proper type handling
        SELECT 
          a.field,
          a.value
        FROM ${tableName}_attributes a
        JOIN valid_job j ON j.id = a.job_id
        WHERE a.field = ANY($2::text[])
      )
      SELECT field, value
      FROM job_fields
      ORDER BY field
    `;
    return { sql, params: [key, fields] };
  } else {
    // Non-job tables logic remains the same
    const baseQuery = `
      SELECT field, value
      FROM ${tableName}
      WHERE key = $1
        AND field = ANY($2::text[])
    `;
    const sql = context.appendExpiryClause(baseQuery, tableName);
    return { sql, params: [key, fields] };
  }
}

export function _hgetall(
  context: HashContext['context'],
  key: string,
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
  const isJobsTableResult = isJobsTable(tableName);

  if (isJobsTableResult) {
    const sql = `
      WITH valid_job AS (
        SELECT id, status, context
        FROM ${tableName}
        WHERE key = $1 AND is_live
      ),
      job_data AS (
        SELECT 'status' AS field, status::text AS value
        FROM ${tableName}
        WHERE key = $1 AND is_live
        
        UNION ALL
        
        SELECT 'context' AS field, context::text AS value
        FROM ${tableName}
        WHERE key = $1 AND is_live
      ),
      attribute_data AS (
        SELECT field, value
        FROM ${tableName}_attributes
        WHERE job_id IN (SELECT id FROM valid_job)
      )
      SELECT * FROM job_data
      UNION ALL
      SELECT * FROM attribute_data;
    `;
    return { sql, params: [key] };
  } else {
    // Non-job tables
    const sql = context.appendExpiryClause(
      `
        SELECT field, value
        FROM ${tableName}
        WHERE key = $1
      `,
      tableName,
    );
    return { sql, params: [key] };
  }
}

export function _hincrbyfloat(
  context: HashContext['context'],
  key: string,
  field: string,
  increment: number,
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
  const isJobsTableResult = isJobsTable(tableName);
  const isStatusField = field === ':';

  if (isJobsTableResult && isStatusField) {
    const sql = `
      UPDATE ${tableName}
      SET status = status + $2
      WHERE key = $1 AND is_live
      RETURNING status::text AS value
    `;
    return { sql, params: [key, increment] };
  } else if (isJobsTableResult) {
    // Update the condition here
    const sql = `
      WITH valid_job AS (
        SELECT id
        FROM ${tableName}
        WHERE key = $1 AND is_live
      )
      INSERT INTO ${tableName}_attributes (job_id, field, value, type)
      SELECT id, $2, ($3::double precision)::text, $4
      FROM valid_job
      ON CONFLICT (job_id, field) DO UPDATE
      SET
        value = ((COALESCE(${tableName}_attributes.value, '0')::double precision) + $3::double precision)::text,
        type = EXCLUDED.type
      RETURNING value;
    `;
    return { sql, params: [key, field, increment, deriveType(field)] };
  } else {
    const sql = `
      INSERT INTO ${tableName} (key, field, value)
      VALUES ($1, $2, ($3)::text)
      ON CONFLICT (key, field) DO UPDATE
      SET value = ((COALESCE(${tableName}.value, '0')::double precision + $3::double precision)::text)
      RETURNING value
    `;
    return { sql, params: [key, field, increment] };
  }
}

/**
 * 3) LOW-LEVEL SQL GENERATOR
 * --------------------------
 * This is the core compound statement. It must:
 * - update jobs.status by statusDelta
 * - compute thresholdHit (0/1) when status_after == threshold
 * - increment the guidField by thresholdHit * guidWeight (typically 100B digit)
 * - return thresholdHit as "value" so it fits the existing return parsing
 *
 * Notes:
 * - Uses tableForKey(key, 'hash') which will resolve to the jobs table for job keys.
 * - Assumes jobs_attributes table is `${jobsTableName}_attributes` (same as _hincrbyfloat)
 * - Uses deriveType(guidField) to be consistent with your attribute typing system.
 */
export function _setStatusAndCollateGuid(
  context: HashContext['context'],
  key: string,                  // jobKey
  statusDelta: number,
  threshold: number,
  guidField: string,
  guidWeight: number,
): SqlResult {
  const jobsTableName = context.tableForKey(key, 'hash');

  if (!isJobsTable(jobsTableName)) {
    throw new Error(
      `_setStatusAndCollateGuid requires a jobs table key; got table ${jobsTableName}`,
    );
  }

  const sql = `
    WITH status_update AS (
      UPDATE ${jobsTableName}
      SET status = status + $2
      WHERE key = $1 AND is_live
      RETURNING id AS job_id, status AS status_after
    ),
    hit AS (
      SELECT
        job_id,
        CASE WHEN status_after = $3 THEN 1 ELSE 0 END AS threshold_hit,
        CASE WHEN status_after = $3 THEN ($4::double precision) ELSE 0::double precision END AS guid_increment
      FROM status_update
    )
    INSERT INTO ${jobsTableName}_attributes (job_id, field, value, type)
    SELECT
      job_id,
      $5 AS field,
      (guid_increment)::text AS value,
      $6
    FROM hit
    ON CONFLICT (job_id, field) DO UPDATE
    SET
      value = ((COALESCE(${jobsTableName}_attributes.value, '0')::double precision) + EXCLUDED.value::double precision)::text,
      type = EXCLUDED.type
    RETURNING (SELECT threshold_hit::text FROM hit) AS value
  `;

  return {
    sql,
    params: [
      key,                      // $1
      statusDelta,              // $2
      threshold,                // $3
      guidWeight,               // $4 (typically 100_000_000_000)
      guidField,                // $5
      deriveType(guidField),    // $6
    ],
  };
}
