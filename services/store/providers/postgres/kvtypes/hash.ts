import { PostgresJobEnumType } from '../../../../../types/postgres';
import {
  HScanResult,
  HSetOptions,
  ProviderTransaction,
  SetOptions,
} from '../../../../../types/provider';
import type { KVSQL } from '../kvsql';

interface Multi extends ProviderTransaction {
  addCommand: (
    sql: string,
    params: any[],
    returnType: string,
    transform?: (rows: any[]) => any,
  ) => void;
}

export const hashModule = (context: KVSQL) => ({
  async hsetnx(
    key: string,
    field: string,
    value: string,
    multi?: ProviderTransaction,
    entity?: string,
  ): Promise<number> {
    const { sql, params } = this._hset(key, { [field]: value }, { nx: true, entity });
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
    const { sql, params } = this._hset(key, fields, options);

    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        
        // Check if this is a JSONB operation that returns a value
        const isJsonbOperation = Object.keys(fields).some(k => 
          k.startsWith('@context:') && k !== '@context'
        );
        
        // Special handling for @context:get operations
        const isGetOperation = '@context:get' in fields;
        
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

  /**
   * Derives the enumerated `type` value based on the field name when
   * setting a field in a jobs table (a 'jobshash' table type).
   */
  _deriveType(fieldName: string): PostgresJobEnumType {
    if (fieldName === ':') {
      return 'status';
    } else if (fieldName.startsWith('_')) {
      return 'udata';
    } else if (fieldName.startsWith('-')) {
      return fieldName.includes(',') ? 'hmark' : 'jmark';
    } else if (fieldName.length === 3) {
      return 'jdata';
    } else if (fieldName.includes(',')) {
      return 'adata';
    } else {
      return 'other';
    }
  },

  _hset(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);
    const fieldEntries = Object.entries(fields);
    const isStatusOnly =
      fieldEntries.length === 1 && fieldEntries[0][0] === ':';

    let targetTable = tableName; // Default table name

    if (isJobsTable) {
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

    if (isJobsTable && isStatusOnly) {
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
    } else if (isJobsTable && '@context' in fields) {
      // Handle JSONB context updates - use the jobs table directly
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context');
      
      if (options?.nx) {
        if (replayId) {
          sql = `
            WITH inserted_job AS (
              INSERT INTO ${tableName} (id, key, context)
              SELECT gen_random_uuid(), $1, $2::jsonb
              WHERE NOT EXISTS (
                SELECT 1 FROM ${tableName}
                WHERE key = $1 AND is_live
              )
              RETURNING id, context::text as new_value
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $3, new_value, $4
              FROM inserted_job
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT new_value FROM inserted_job
          `;
          params.push(key, fields['@context'], replayId, this._deriveType(replayId));
        } else {
          sql = `
            INSERT INTO ${tableName} (id, key, context)
            SELECT gen_random_uuid(), $1, $2::jsonb
            WHERE NOT EXISTS (
              SELECT 1 FROM ${tableName}
              WHERE key = $1 AND is_live
            )
            RETURNING context::text as new_value
          `;
          params.push(key, fields['@context']);
        }
      } else {
        if (replayId) {
          sql = `
            WITH updated_job AS (
              UPDATE ${tableName}
              SET context = $2::jsonb
              WHERE key = $1 AND is_live
              RETURNING id, context::text as new_value
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $3, new_value, $4
              FROM updated_job
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT new_value FROM updated_job
          `;
          params.push(key, fields['@context'], replayId, this._deriveType(replayId));
        } else {
          sql = `
            UPDATE ${tableName}
            SET context = $2::jsonb
            WHERE key = $1 AND is_live
            RETURNING context::text as new_value
          `;
          params.push(key, fields['@context']);
        }
      }
    } else if (isJobsTable && '@context:merge' in fields) {
      // Handle JSONB context merge - deep merge operation
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:merge');
      
      if (options?.nx) {
        sql = `
          INSERT INTO ${tableName} (id, key, context)
          SELECT gen_random_uuid(), $1, $2::jsonb
          WHERE NOT EXISTS (
            SELECT 1 FROM ${tableName}
            WHERE key = $1 AND is_live
          )
          RETURNING context::text as new_value
        `;
        params.push(key, fields['@context:merge']);
      } else {
        if (replayId) {
          // Store replay value and update context in one transaction with deep merge
          sql = `
            WITH updated_job AS (
              UPDATE ${tableName}
              SET context = (
                WITH RECURSIVE deep_merge(original, new_data, result) AS (
                  -- Base case: start with the original and new data
                  SELECT 
                    COALESCE(context, '{}'::jsonb) as original,
                    $2::jsonb as new_data,
                    COALESCE(context, '{}'::jsonb) as result
                  FROM ${tableName}
                  WHERE key = $1 AND is_live
                ),
                merged_data AS (
                  SELECT 
                    (
                      SELECT jsonb_object_agg(
                        key,
                        CASE 
                          -- If both are objects, merge them recursively
                          WHEN jsonb_typeof(original -> key) = 'object' AND jsonb_typeof(new_data -> key) = 'object'
                          THEN (
                            WITH nested_keys AS (
                              SELECT unnest(ARRAY(SELECT jsonb_object_keys((original -> key) || (new_data -> key)))) as nested_key
                            )
                            SELECT jsonb_object_agg(
                              nested_key,
                              CASE 
                                WHEN (new_data -> key) ? nested_key
                                THEN (new_data -> key) -> nested_key
                                ELSE (original -> key) -> nested_key
                              END
                            )
                            FROM nested_keys
                          )
                          -- If new data has this key, use new value
                          WHEN new_data ? key
                          THEN new_data -> key
                          -- Otherwise keep original value
                          ELSE original -> key
                        END
                      )
                      FROM (
                        SELECT unnest(ARRAY(SELECT jsonb_object_keys(original || new_data))) as key
                      ) all_keys
                    ) as merged_context
                  FROM deep_merge
                )
                SELECT merged_context FROM merged_data
              )
              WHERE key = $1 AND is_live
              RETURNING id, context::text as new_value
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $3, new_value, $4
              FROM updated_job
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT new_value FROM updated_job
          `;
          params.push(key, fields['@context:merge'], replayId, this._deriveType(replayId));
        } else {
          sql = `
            UPDATE ${tableName}
            SET context = (
              WITH merged_data AS (
                SELECT 
                  (
                    SELECT jsonb_object_agg(
                      key,
                      CASE 
                        -- If both are objects, merge them recursively
                        WHEN jsonb_typeof(original -> key) = 'object' AND jsonb_typeof(new_data -> key) = 'object'
                        THEN (
                          WITH nested_keys AS (
                            SELECT unnest(ARRAY(SELECT jsonb_object_keys((original -> key) || (new_data -> key)))) as nested_key
                          )
                          SELECT jsonb_object_agg(
                            nested_key,
                            CASE 
                              WHEN (new_data -> key) ? nested_key
                              THEN (new_data -> key) -> nested_key
                              ELSE (original -> key) -> nested_key
                            END
                          )
                          FROM nested_keys
                        )
                        -- If new data has this key, use new value
                        WHEN new_data ? key
                        THEN new_data -> key
                        -- Otherwise keep original value
                        ELSE original -> key
                      END
                    )
                    FROM (
                      SELECT unnest(ARRAY(SELECT jsonb_object_keys(original || new_data))) as key
                    ) all_keys
                  ) as merged_context
                FROM (
                  SELECT 
                    COALESCE(context, '{}'::jsonb) as original,
                    $2::jsonb as new_data
                  FROM ${tableName}
                  WHERE key = $1 AND is_live
                ) base_data
              )
              SELECT merged_context FROM merged_data
            )
            WHERE key = $1 AND is_live
            RETURNING context::text as new_value
          `;
          params.push(key, fields['@context:merge']);
        }
      }
    } else if (isJobsTable && '@context:delete' in fields) {
      // Handle JSONB context delete - remove path
      const path = fields['@context:delete'];
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:delete');
      
      if (pathParts.length === 1) {
        // Simple key deletion
        if (replayId) {
          sql = `
            WITH updated_job AS (
              UPDATE ${tableName}
              SET context = context - $2
              WHERE key = $1 AND is_live
              RETURNING id, context::text as new_value
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $3, new_value, $4
              FROM updated_job
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT new_value FROM updated_job
          `;
          params.push(key, path, replayId, this._deriveType(replayId));
        } else {
          sql = `
            UPDATE ${tableName}
            SET context = context - $2
            WHERE key = $1 AND is_live
            RETURNING context::text as new_value
          `;
          params.push(key, path);
        }
      } else {
        // Nested path deletion using jsonb_set with null to remove
        if (replayId) {
          sql = `
            WITH updated_job AS (
              UPDATE ${tableName}
              SET context = context #- $2::text[]
              WHERE key = $1 AND is_live
              RETURNING id, context::text as new_value
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $3, new_value, $4
              FROM updated_job
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT new_value FROM updated_job
          `;
          params.push(key, pathParts, replayId, this._deriveType(replayId));
        } else {
          sql = `
            UPDATE ${tableName}
            SET context = context #- $2::text[]
            WHERE key = $1 AND is_live
            RETURNING context::text as new_value
          `;
          params.push(key, pathParts);
        }
      }
    } else if (isJobsTable && '@context:append' in fields) {
      // Handle JSONB array append
      const { path, value } = JSON.parse(fields['@context:append']);
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:append');
      
      if (replayId) {
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = jsonb_set(
              COALESCE(context, '{}'::jsonb),
              $2::text[],
              COALESCE(context #> $2::text[], '[]'::jsonb) || $3::jsonb,
              true
            )
            WHERE key = $1 AND is_live
            RETURNING id, (context #> $2::text[])::text as new_value
          ),
          replay_insert AS (
            INSERT INTO ${tableName}_attributes (job_id, field, value, type)
            SELECT id, $4, new_value, $5
            FROM updated_job
            ON CONFLICT (job_id, field) DO UPDATE
            SET value = EXCLUDED.value
            RETURNING 1
          )
          SELECT new_value FROM updated_job
        `;
        params.push(key, pathParts, JSON.stringify([value]), replayId, this._deriveType(replayId));
      } else {
        sql = `
          UPDATE ${tableName}
          SET context = jsonb_set(
            COALESCE(context, '{}'::jsonb),
            $2::text[],
            COALESCE(context #> $2::text[], '[]'::jsonb) || $3::jsonb,
            true
          )
          WHERE key = $1 AND is_live
          RETURNING (context #> $2::text[])::text as new_value
        `;
        params.push(key, pathParts, JSON.stringify([value]));
      }
    } else if (isJobsTable && '@context:prepend' in fields) {
      // Handle JSONB array prepend
      const { path, value } = JSON.parse(fields['@context:prepend']);
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:prepend');
      
      if (replayId) {
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = jsonb_set(
              COALESCE(context, '{}'::jsonb),
              $2::text[],
              $3::jsonb || COALESCE(context #> $2::text[], '[]'::jsonb),
              true
            )
            WHERE key = $1 AND is_live
            RETURNING id, (context #> $2::text[])::text as new_value
          ),
          replay_insert AS (
            INSERT INTO ${tableName}_attributes (job_id, field, value, type)
            SELECT id, $4, new_value, $5
            FROM updated_job
            ON CONFLICT (job_id, field) DO UPDATE
            SET value = EXCLUDED.value
            RETURNING 1
          )
          SELECT new_value FROM updated_job
        `;
        params.push(key, pathParts, JSON.stringify([value]), replayId, this._deriveType(replayId));
      } else {
        sql = `
          UPDATE ${tableName}
          SET context = jsonb_set(
            COALESCE(context, '{}'::jsonb),
            $2::text[],
            $3::jsonb || COALESCE(context #> $2::text[], '[]'::jsonb),
            true
          )
          WHERE key = $1 AND is_live
          RETURNING (context #> $2::text[])::text as new_value
        `;
        params.push(key, pathParts, JSON.stringify([value]));
      }
    } else if (isJobsTable && '@context:remove' in fields) {
      // Handle JSONB array remove by index
      const { path, index } = JSON.parse(fields['@context:remove']);
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:remove');
      
      if (replayId) {
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = jsonb_set(
              COALESCE(context, '{}'::jsonb),
              $2::text[],
              (
                SELECT jsonb_agg(value)
                FROM (
                  SELECT value, row_number() OVER () - 1 as idx
                  FROM jsonb_array_elements(COALESCE(context #> $2::text[], '[]'::jsonb))
                ) t
                WHERE idx != $3
              ),
              true
            )
            WHERE key = $1 AND is_live
            RETURNING id, (context #> $2::text[])::text as new_value
          ),
          replay_insert AS (
            INSERT INTO ${tableName}_attributes (job_id, field, value, type)
            SELECT id, $4, new_value, $5
            FROM updated_job
            ON CONFLICT (job_id, field) DO UPDATE
            SET value = EXCLUDED.value
            RETURNING 1
          )
          SELECT new_value FROM updated_job
        `;
        params.push(key, pathParts, index, replayId, this._deriveType(replayId));
      } else {
        sql = `
          UPDATE ${tableName}
          SET context = jsonb_set(
            COALESCE(context, '{}'::jsonb),
            $2::text[],
            (
              SELECT jsonb_agg(value)
              FROM (
                SELECT value, row_number() OVER () - 1 as idx
                FROM jsonb_array_elements(COALESCE(context #> $2::text[], '[]'::jsonb))
              ) t
              WHERE idx != $3
            ),
            true
          )
          WHERE key = $1 AND is_live
          RETURNING (context #> $2::text[])::text as new_value
        `;
        params.push(key, pathParts, index);
      }
    } else if (isJobsTable && '@context:increment' in fields) {
      // Handle JSONB numeric increment
      const { path, value } = JSON.parse(fields['@context:increment']);
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:increment');
      
      if (replayId) {
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = jsonb_set(
              COALESCE(context, '{}'::jsonb),
              $2::text[],
              to_jsonb((COALESCE((context #> $2::text[])::text::numeric, 0) + $3)::numeric),
              true
            )
            WHERE key = $1 AND is_live
            RETURNING id, (context #> $2::text[])::text as new_value
          ),
          replay_insert AS (
            INSERT INTO ${tableName}_attributes (job_id, field, value, type)
            SELECT id, $4, new_value, $5
            FROM updated_job
            ON CONFLICT (job_id, field) DO UPDATE
            SET value = EXCLUDED.value
            RETURNING 1
          )
          SELECT new_value FROM updated_job
        `;
        params.push(key, pathParts, value, replayId, this._deriveType(replayId));
      } else {
        sql = `
          UPDATE ${tableName}
          SET context = jsonb_set(
            COALESCE(context, '{}'::jsonb),
            $2::text[],
            to_jsonb((COALESCE((context #> $2::text[])::text::numeric, 0) + $3)::numeric),
            true
          )
          WHERE key = $1 AND is_live
          RETURNING (context #> $2::text[])::text as new_value
        `;
        params.push(key, pathParts, value);
      }
    } else if (isJobsTable && '@context:toggle' in fields) {
      // Handle JSONB boolean toggle
      const path = fields['@context:toggle'];
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:toggle');
      
      if (replayId) {
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = jsonb_set(
              COALESCE(context, '{}'::jsonb),
              $2::text[],
              to_jsonb(NOT COALESCE((context #> $2::text[])::text::boolean, false)),
              true
            )
            WHERE key = $1 AND is_live
            RETURNING id, (context #> $2::text[])::text as new_value
          ),
          replay_insert AS (
            INSERT INTO ${tableName}_attributes (job_id, field, value, type)
            SELECT id, $3, new_value, $4
            FROM updated_job
            ON CONFLICT (job_id, field) DO UPDATE
            SET value = EXCLUDED.value
            RETURNING 1
          )
          SELECT new_value FROM updated_job
        `;
        params.push(key, pathParts, replayId, this._deriveType(replayId));
      } else {
        sql = `
          UPDATE ${tableName}
          SET context = jsonb_set(
            COALESCE(context, '{}'::jsonb),
            $2::text[],
            to_jsonb(NOT COALESCE((context #> $2::text[])::text::boolean, false)),
            true
          )
          WHERE key = $1 AND is_live
          RETURNING (context #> $2::text[])::text as new_value
        `;
        params.push(key, pathParts);
      }
    } else if (isJobsTable && '@context:setIfNotExists' in fields) {
      // Handle JSONB conditional set
      const { path, value } = JSON.parse(fields['@context:setIfNotExists']);
      const pathParts = path.split('.');
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:setIfNotExists');
      
      if (replayId) {
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = CASE 
              WHEN context #> $2::text[] IS NULL THEN 
                jsonb_set(COALESCE(context, '{}'::jsonb), $2::text[], $3::jsonb, true)
              ELSE context
            END
            WHERE key = $1 AND is_live
            RETURNING id, (context #> $2::text[])::text as new_value
          ),
          replay_insert AS (
            INSERT INTO ${tableName}_attributes (job_id, field, value, type)
            SELECT id, $4, new_value, $5
            FROM updated_job
            ON CONFLICT (job_id, field) DO UPDATE
            SET value = EXCLUDED.value
            RETURNING 1
          )
          SELECT new_value FROM updated_job
        `;
        params.push(key, pathParts, JSON.stringify(value), replayId, this._deriveType(replayId));
      } else {
        sql = `
          UPDATE ${tableName}
          SET context = CASE 
            WHEN context #> $2::text[] IS NULL THEN 
              jsonb_set(COALESCE(context, '{}'::jsonb), $2::text[], $3::jsonb, true)
            ELSE context
          END
          WHERE key = $1 AND is_live
          RETURNING (context #> $2::text[])::text as new_value
        `;
        params.push(key, pathParts, JSON.stringify(value));
      }
    } else if (isJobsTable && Object.keys(fields).some(k => k.startsWith('@context:get:'))) {
      // Handle JSONB path extraction for get operations
      const getField = Object.keys(fields).find(k => k.startsWith('@context:get:'));
      const pathKey = getField.replace('@context:get:', '');
      const pathParts = JSON.parse(fields[getField]);
      
      // Extract the specific path and store it as a temporary field
      sql = `
        INSERT INTO ${tableName}_attributes (job_id, field, value, type)
        SELECT 
          job.id,
          $2,
          COALESCE((job.context #> $3::text[])::text, 'null'),
          $4
        FROM (
          SELECT id, context FROM ${tableName} WHERE key = $1 AND is_live
        ) AS job
        ON CONFLICT (job_id, field) DO UPDATE
        SET value = COALESCE((
          SELECT context #> $3::text[]
          FROM ${tableName} 
          WHERE key = $1 AND is_live
        )::text, 'null')
        RETURNING 1 as count
      `;
      params.push(key, getField, pathParts, this._deriveType(getField));
    } else if (isJobsTable && '@context:get' in fields) {
      // Handle JSONB context get operation with replay storage
      const path = fields['@context:get'];
      const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:get');
      
      if (path === '') {
        // Get entire context
        if (replayId) {
          sql = `
            WITH job_data AS (
              SELECT id, context::text as context_value
              FROM ${tableName}
              WHERE key = $1 AND is_live
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $2, context_value, $3
              FROM job_data
              WHERE id IS NOT NULL
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT context_value as new_value FROM job_data
          `;
          params.push(key, replayId, this._deriveType(replayId));
        } else {
          sql = `
            SELECT context::text as new_value
            FROM ${tableName}
            WHERE key = $1 AND is_live
          `;
          params.push(key);
        }
      } else {
        // Get specific path
        const pathParts = path.split('.');
        if (replayId) {
          sql = `
            WITH job_data AS (
              SELECT id, COALESCE((context #> $2::text[])::text, 'null') as path_value
              FROM ${tableName}
              WHERE key = $1 AND is_live
            ),
            replay_insert AS (
              INSERT INTO ${tableName}_attributes (job_id, field, value, type)
              SELECT id, $3, path_value, $4
              FROM job_data
              WHERE id IS NOT NULL
              ON CONFLICT (job_id, field) DO UPDATE
              SET value = EXCLUDED.value
              RETURNING 1
            )
            SELECT path_value as new_value FROM job_data
          `;
          params.push(key, pathParts, replayId, this._deriveType(replayId));
        } else {
          sql = `
            SELECT COALESCE((context #> $2::text[])::text, 'null') as new_value
            FROM ${tableName}
            WHERE key = $1 AND is_live
          `;
          params.push(key, pathParts);
        }
      }
    } else if (isJobsTable) {
      const schemaName = context.safeName(context.appId);
      const conflictAction = options?.nx
        ? 'ON CONFLICT DO NOTHING'
        : `ON CONFLICT (job_id, field) DO UPDATE SET value = EXCLUDED.value`;

      const placeholders = fieldEntries
        .map(([field, value], index) => {
          const baseIndex = index * 3 + 2; // Adjusted baseIndex
          params.push(field, value, this._deriveType(field));
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
  },

  async hget(
    key: string,
    field: string,
    multi?: ProviderTransaction,
  ): Promise<string | null> {
    const { sql, params } = this._hget(key, field);

    if (multi) {
      (multi as Multi).addCommand(sql, params, 'string', (rows) => {
        return rows[0]?.value || null;
      });
      return Promise.resolve(null);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rows[0]?.value || null;
    }
  },

  _hget(key: string, field: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);
    const isStatusField = field === ':';
    const isContextField = field === '@';

    if (isJobsTable && isStatusField) {
      // Fetch status from jobs table
      const sql = `
        SELECT status::text AS value
        FROM ${tableName}
        WHERE key = $1 AND is_live
      `;
      return { sql, params: [key] };
    } else if (isJobsTable && isContextField) {
      // Fetch context from jobs table
      const sql = `
        SELECT context::text AS value
        FROM ${tableName}
        WHERE key = $1 AND is_live
      `;
      return { sql, params: [key] };
    } else if (isJobsTable) {
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
  },

  async hdel(key: string, fields: string[], multi?: unknown): Promise<number> {
    // Ensure fields is an array
    if (!Array.isArray(fields)) {
      fields = [fields];
    }
    const { sql, params } = this._hdel(key, fields);
    if (multi) {
      (multi as unknown as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      const res = await context.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  },

  _hdel(key: string, fields: string[]): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);
    const targetTable = isJobsTable ? `${tableName}_attributes` : tableName;

    const fieldPlaceholders = fields.map((_, i) => `$${i + 2}`).join(', ');
    const params = [key, ...fields];

    if (isJobsTable) {
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
  },

  async hmget(
    key: string,
    fields: string[],
    multi?: ProviderTransaction,
  ): Promise<(string | null)[]> {
    const { sql, params } = this._hmget(key, fields);

    const processRows = (rows: any[]) => {
      let statusValue: string | null = null;
      let contextValue: string | null = null;
      const fieldValueMap = new Map<string, string | null>();

      for (const row of rows) {
        if (row.field === 'status') {
          statusValue = row.value;
          fieldValueMap.set(':', row.value); // Map status to ':'
        } else if (row.field === 'context') {
          contextValue = row.value;
          fieldValueMap.set('@', row.value); // Map context to '@'
        } else if (row.field !== ':' && row.field !== '@') {
          // Ignore old format fields
          fieldValueMap.set(row.field, row.value);
        }
      }

      // Ensure ':' and '@' are present in the map with their values
      if (statusValue !== null) {
        fieldValueMap.set(':', statusValue);
      }
      if (contextValue !== null) {
        fieldValueMap.set('@', contextValue);
      }

      // Map requested fields to their values, or null if not present
      return fields.map((field) => fieldValueMap.get(field) || null);
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

  _hmget(key: string, fields: string[]): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);

    if (isJobsTable) {
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
  },

  async hgetall(
    key: string,
    multi?: ProviderTransaction,
  ): Promise<Record<string, string>> {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);

    const { sql, params } = this._hgetall(key);
    const processRows = (rows: any[]): Record<string, string> => {
      const result: Record<string, string> = {};

      for (const row of rows) {
        // Map status to ':' and context to '@'
        // Ignore old format fields
        if (isJobsTable) {
          if (row.field === 'status') {
            result[':'] = row.value;
          } else if (row.field === 'context') {
            result['@'] = row.value;
          } else if (row.field !== ':' && row.field !== '@') {
            result[row.field] = row.value;
          }
        } else {
          result[row.field] = row.value;
        }
      }

      return result;
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

  _hgetall(key: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);

    if (isJobsTable) {
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
  },

  async hincrbyfloat(
    key: string,
    field: string,
    increment: number,
    multi?: ProviderTransaction,
  ): Promise<number> {
    const { sql, params } = this._hincrbyfloat(key, field, increment);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number', (rows) => {
        return parseFloat(rows[0].value);
      });
      return Promise.resolve(0);
    } else {
      const res = await context.pgClient.query(sql, params);
      return parseFloat(res.rows[0].value);
    }
  },

  _hincrbyfloat(
    key: string,
    field: string,
    increment: number,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const isJobsTable = this.isJobsTable(tableName);
    const isStatusField = field === ':';

    if (isJobsTable && isStatusField) {
      const sql = `
        UPDATE ${tableName}
        SET status = status + $2
        WHERE key = $1 AND is_live
        RETURNING status::text AS value
      `;
      return { sql, params: [key, increment] };
    } else if (isJobsTable) {
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
      return { sql, params: [key, field, increment, this._deriveType(field)] };
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
  },

  async hscan(
    key: string,
    cursor: string,
    count = 10,
    pattern?: string,
    multi?: ProviderTransaction,
  ): Promise<HScanResult> {
    const { sql, params } = this._hscan(key, cursor, count, pattern);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'object', (rows) => {
        const items: Record<string, string> = {};
        for (const row of rows) {
          items[row.field] = row.value;
        }
        const newCursor =
          rows.length < count ? 0 : Number(cursor) + rows.length;
        return { cursor: newCursor.toString(), items };
      });
      return Promise.resolve({ cursor: '0', items: {} });
    } else {
      const res = await context.pgClient.query(sql, params);
      const items: Record<string, string> = {};
      for (const row of res.rows) {
        items[row.field] = row.value;
      }
      const newCursor =
        res.rowCount < count ? 0 : Number(cursor) + res.rowCount;
      return { cursor: newCursor.toString(), items };
    }
  },

  _hscan(
    key: string,
    cursor: string,
    count: number,
    pattern?: string,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'hash');
    const params = [key];
    let sql = `
      SELECT field, value FROM ${tableName}
      WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
    `;
    let paramIndex = 2;
    if (pattern) {
      const sqlPattern = pattern.replace(/\*/g, '%');
      sql += ` AND field LIKE $${paramIndex}`;
      params.push(sqlPattern);
      paramIndex++;
    }
    sql += `
      ORDER BY field
      OFFSET $${paramIndex} LIMIT $${paramIndex + 1}
    `;
    params.push(cursor.toString());
    params.push(count.toString());
    return { sql, params };
  },

  async expire(
    key: string,
    seconds: number,
    multi?: ProviderTransaction,
  ): Promise<boolean> {
    const { sql, params } = this._expire(key, seconds);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  },

  _expire(key: string, seconds: number): { sql: string; params: any[] } {
    //only job tables are ever expired
    const tableName = context.tableForKey(key);
    const expiryTime = new Date(Date.now() + seconds * 1000);
    const sql = `
      UPDATE ${tableName}
      SET expired_at = $2
      WHERE key = $1 AND is_live
      RETURNING true as success
    `;
    const params = [key, expiryTime];
    return { sql, params };
  },

  async scan(
    cursor: number,
    count = 10,
    pattern?: string,
    multi?: ProviderTransaction,
  ): Promise<{ cursor: number; keys: string[] }> {
    const { sql, params } = this._scan(cursor, count, pattern);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'object', (rows) => {
        const keys = rows.map((row) => row.key);
        const newCursor = cursor + rows.length;
        return { cursor: newCursor, keys };
      });
      return Promise.resolve({ cursor: 0, keys: [] });
    } else {
      const res = await context.pgClient.query(sql, params);
      const keys = res.rows.map((row) => row.key);
      const newCursor = cursor + res.rowCount;
      return { cursor: newCursor, keys };
    }
  },

  _scan(
    cursor: number,
    count: number,
    pattern?: string,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(`_:${context.appId}:j:_`);
    let sql = `
      SELECT key FROM ${tableName}
      WHERE (expired_at IS NULL OR expired_at > NOW())
    `;
    const params = [];

    if (pattern) {
      sql += ' AND key LIKE $1';
      params.push(pattern.replace(/\*/g, '%'));
    }

    sql += `
      ORDER BY key
      OFFSET $${params.length + 1} LIMIT $${params.length + 2}
    `;

    params.push(cursor.toString());
    params.push(count.toString());
    return { sql, params };
  },

  isJobsTable(tableName: string): boolean {
    return tableName.endsWith('jobs');
  },
});
