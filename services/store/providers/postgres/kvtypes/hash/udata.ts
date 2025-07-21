import { HashContext, SqlResult, HSetOptions } from './types';
import { isJobsTable, deriveType } from './utils';

export function createUdataOperations(context: HashContext['context']) {
  return {
    handleUdataSet,
    handleUdataGet,
    handleUdataMget,
    handleUdataDelete,
    handleUdataIncrement,
    handleUdataMultiply,
    handleUdataAll,
  };

  function handleUdataSet(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:set',
    );
    const udata = JSON.parse(fields['@udata:set']);
    const params = [];
    let sql = '';

    // Extract the fields to set (can be object or key-value pairs)
    const fieldsToSet: Record<string, string> = {};
    if (typeof udata === 'object' && !Array.isArray(udata)) {
      // Object format: { field1: 'value1', field2: 'value2' }
      for (const [fieldName, value] of Object.entries(udata)) {
        fieldsToSet[fieldName] = String(value);
      }
    } else if (Array.isArray(udata)) {
      // Array format: ['field1', 'value1', 'field2', 'value2']
      for (let i = 0; i < udata.length; i += 2) {
        const fieldName = udata[i];
        const value = udata[i + 1];
        fieldsToSet[fieldName] = String(value);
      }
    }

    const fieldEntries = Object.entries(fieldsToSet);
    if (fieldEntries.length === 0) {
      // No fields to set, return a no-op
      return { sql: 'SELECT 0 as count', params: [] };
    }

    const schemaName = context.safeName(context.appId);
    
    if (replayId) {
      // Version with replay storage
      const placeholders = fieldEntries
        .map(([fieldName, value], index) => {
          const baseIndex = index * 3 + 3;
          params.push(fieldName, value, 'udata');
          return `($${baseIndex}, $${baseIndex + 1}, $${baseIndex + 2}::${schemaName}.type_enum)`;
        })
        .join(', ');

      sql = `
        WITH valid_job AS (
          SELECT id FROM ${tableName} WHERE key = $1 AND is_live
        ),
        upsert_fields AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT 
            job.id,
            vals.field,
            vals.value,
            vals.type
          FROM valid_job job
          CROSS JOIN (
            VALUES ${placeholders}
          ) AS vals(field, value, type)
          ON CONFLICT (job_id, field) DO UPDATE SET value = EXCLUDED.value
          RETURNING 1 as field_count
        ),
        count_result AS (
          SELECT COUNT(*) as new_fields_count FROM upsert_fields
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT job.id, $2, new_fields_count::text, $${2 + fieldEntries.length * 3 + 1}::${schemaName}.type_enum
          FROM valid_job job, count_result
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT new_fields_count FROM count_result
      `;
      
      params.unshift(key, replayId);
      params.push(deriveType(replayId));
    } else {
      // Version without replay storage
      const placeholders = fieldEntries
        .map(([fieldName, value], index) => {
          const baseIndex = index * 3 + 2;
          params.push(fieldName, value, 'udata');
          return `($${baseIndex}, $${baseIndex + 1}, $${baseIndex + 2}::${schemaName}.type_enum)`;
        })
        .join(', ');

      sql = `
        WITH valid_job AS (
          SELECT id FROM ${tableName} WHERE key = $1 AND is_live
        )
        INSERT INTO ${tableName}_attributes (job_id, field, value, type)
        SELECT 
          job.id,
          vals.field,
          vals.value,
          vals.type
        FROM valid_job job
        CROSS JOIN (
          VALUES ${placeholders}
        ) AS vals(field, value, type)
        ON CONFLICT (job_id, field) DO UPDATE SET value = EXCLUDED.value
        RETURNING 1 as count
      `;
      
      params.unshift(key);
    }

    return { sql, params };
  }

  function handleUdataGet(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const fieldName = fields['@udata:get'];
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:get',
    );
    const params = [];
    let sql = '';

    if (replayId) {
      sql = `
        WITH field_data AS (
          SELECT COALESCE(a.value, '') as field_value
          FROM ${tableName} j
          LEFT JOIN ${tableName}_attributes a ON j.id = a.job_id AND a.field = $2
          WHERE j.key = $1 AND j.is_live
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT j.id, $3, field_value, $4
          FROM ${tableName} j, field_data
          WHERE j.key = $1 AND j.is_live
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT field_value as new_value FROM field_data
      `;
      params.push(key, fieldName, replayId, deriveType(replayId));
    } else {
      sql = `
        SELECT COALESCE(a.value, '') as new_value
        FROM ${tableName} j
        LEFT JOIN ${tableName}_attributes a ON j.id = a.job_id AND a.field = $2
        WHERE j.key = $1 AND j.is_live
      `;
      params.push(key, fieldName);
    }

    return { sql, params };
  }

  function handleUdataMget(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const fieldNames = JSON.parse(fields['@udata:mget']);
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:mget',
    );
    const params = [];
    let sql = '';

    if (replayId) {
      sql = `
        WITH field_data AS (
          SELECT array_agg(COALESCE(a.value, '') ORDER BY field_order.idx) as field_values
          FROM ${tableName} j
          CROSS JOIN (
            SELECT unnest($2::text[]) as field_name, generate_subscripts($2::text[], 1) as idx
          ) as field_order
          LEFT JOIN ${tableName}_attributes a ON j.id = a.job_id AND a.field = field_order.field_name
          WHERE j.key = $1 AND j.is_live
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT j.id, $3, array_to_string(field_values, '|||'), $4
          FROM ${tableName} j, field_data
          WHERE j.key = $1 AND j.is_live
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT field_values as new_value FROM field_data
      `;
      params.push(key, fieldNames, replayId, deriveType(replayId));
    } else {
      sql = `
        SELECT array_agg(COALESCE(a.value, '') ORDER BY field_order.idx) as new_value
        FROM ${tableName} j
        CROSS JOIN (
          SELECT unnest($2::text[]) as field_name, generate_subscripts($2::text[], 1) as idx
        ) as field_order
        LEFT JOIN ${tableName}_attributes a ON j.id = a.job_id AND a.field = field_order.field_name
        WHERE j.key = $1 AND j.is_live
      `;
      params.push(key, fieldNames);
    }

    return { sql, params };
  }

  function handleUdataDelete(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const fieldNames = JSON.parse(fields['@udata:delete']);
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:delete',
    );
    const params = [];
    let sql = '';

    if (replayId) {
      sql = `
        WITH deleted_fields AS (
          DELETE FROM ${tableName}_attributes
          WHERE job_id = (
            SELECT id FROM ${tableName} WHERE key = $1 AND is_live
          )
          AND field = ANY($2::text[])
          RETURNING 1 as deleted_count
        ),
        count_result AS (
          SELECT COUNT(*) as total_deleted FROM deleted_fields
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT j.id, $3, total_deleted::text, $4
          FROM ${tableName} j, count_result
          WHERE j.key = $1 AND j.is_live
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT total_deleted as new_value FROM count_result
      `;
      params.push(key, fieldNames, replayId, deriveType(replayId));
    } else {
      sql = `
        WITH deleted_fields AS (
          DELETE FROM ${tableName}_attributes
          WHERE job_id = (
            SELECT id FROM ${tableName} WHERE key = $1 AND is_live
          )
          AND field = ANY($2::text[])
          RETURNING 1 as deleted_count
        )
        SELECT COUNT(*) as new_value FROM deleted_fields
      `;
      params.push(key, fieldNames);
    }

    return { sql, params };
  }

  function handleUdataIncrement(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { field, value } = JSON.parse(fields['@udata:increment']);
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:increment',
    );
    const schemaName = context.safeName(context.appId);
    const params = [];
    let sql = '';

    if (replayId) {
      sql = `
        WITH valid_job AS (
          SELECT id FROM ${tableName} WHERE key = $1 AND is_live
        ),
        increment_result AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT id, $2, $3::text, $4::${schemaName}.type_enum
          FROM valid_job
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = ((COALESCE(${tableName}_attributes.value, '0')::double precision) + $3::double precision)::text
          RETURNING value
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT job.id, $5, inc.value, $6::${schemaName}.type_enum
          FROM valid_job job, increment_result inc
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT value as new_value FROM increment_result
      `;
      params.push(key, field, value, 'udata', replayId, deriveType(replayId));
    } else {
      sql = `
        WITH valid_job AS (
          SELECT id FROM ${tableName} WHERE key = $1 AND is_live
        )
        INSERT INTO ${tableName}_attributes (job_id, field, value, type)
        SELECT id, $2, $3::text, $4::${schemaName}.type_enum
        FROM valid_job
        ON CONFLICT (job_id, field) DO UPDATE
        SET value = ((COALESCE(${tableName}_attributes.value, '0')::double precision) + $3::double precision)::text
        RETURNING value as new_value
      `;
      params.push(key, field, value, 'udata');
    }

    return { sql, params };
  }

  function handleUdataMultiply(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { field, value } = JSON.parse(fields['@udata:multiply']);
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:multiply',
    );
    const schemaName = context.safeName(context.appId);
    const params = [];
    let sql = '';

    // For multiplication, we work with logarithms to support exponential multiplication
    // log(a * b) = log(a) + log(b), so exp(log(a) + log(b)) = a * b
    if (replayId) {
      sql = `
        WITH valid_job AS (
          SELECT id FROM ${tableName} WHERE key = $1 AND is_live
        ),
        multiply_result AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT id, $2, ln($3::double precision)::text, $4::${schemaName}.type_enum
          FROM valid_job
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = (COALESCE(${tableName}_attributes.value::double precision, 0) + ln($3::double precision))::text
          RETURNING value
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT job.id, $5, mult.value, $6::${schemaName}.type_enum
          FROM valid_job job, multiply_result mult
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT value as new_value FROM multiply_result
      `;
      params.push(key, field, value, 'udata', replayId, deriveType(replayId));
    } else {
      sql = `
        WITH valid_job AS (
          SELECT id FROM ${tableName} WHERE key = $1 AND is_live
        )
        INSERT INTO ${tableName}_attributes (job_id, field, value, type)
        SELECT id, $2, ln($3::double precision)::text, $4::${schemaName}.type_enum
        FROM valid_job
        ON CONFLICT (job_id, field) DO UPDATE
        SET value = (COALESCE(${tableName}_attributes.value::double precision, 0) + ln($3::double precision))::text
        RETURNING value as new_value
      `;
      params.push(key, field, value, 'udata');
    }

    return { sql, params };
  }

  function handleUdataAll(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && k !== '@udata:all',
    );
    const params = [];
    let sql = '';

    if (replayId) {
      sql = `
        WITH field_data AS (
          SELECT jsonb_object_agg(a.field, a.value) as field_values
          FROM ${tableName} j
          LEFT JOIN ${tableName}_attributes a ON j.id = a.job_id 
          WHERE j.key = $1 AND j.is_live
          AND a.type = 'udata' AND a.field LIKE '\\_%'
        ),
        replay_insert AS (
          INSERT INTO ${tableName}_attributes (job_id, field, value, type)
          SELECT j.id, $2, field_values::text, $3
          FROM ${tableName} j, field_data
          WHERE j.key = $1 AND j.is_live
          ON CONFLICT (job_id, field) DO UPDATE
          SET value = EXCLUDED.value
          RETURNING 1
        )
        SELECT field_values as new_value FROM field_data
      `;
      params.push(key, replayId, deriveType(replayId));
    } else {
      sql = `
        SELECT jsonb_object_agg(a.field, a.value) as new_value
        FROM ${tableName} j
        LEFT JOIN ${tableName}_attributes a ON j.id = a.job_id 
        WHERE j.key = $1 AND j.is_live
        AND a.type = 'udata' AND a.field LIKE '\\_%'
      `;
      params.push(key);
    }

    return { sql, params };
  }
} 