import { HashContext, SqlResult, HSetOptions } from './types';
import { isJobsTable, deriveType } from './utils';

export function createJsonbOperations(context: HashContext['context']) {
  return {
    handleContextSet,
    handleContextMerge,
    handleContextDelete,
    handleContextAppend,
    handleContextPrepend,
    handleContextRemove,
    handleContextIncrement,
    handleContextToggle,
    handleContextSetIfNotExists,
    handleContextGet,
    handleContextGetPath,
  };

  function handleContextSet(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context');
    const params = [];
    let sql = '';

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
        params.push(key, fields['@context'], replayId, deriveType(replayId));
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
        params.push(key, fields['@context'], replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextMerge(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:merge');
    const params = [];
    let sql = '';

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
        sql = `
          WITH updated_job AS (
            UPDATE ${tableName}
            SET context = (
              WITH RECURSIVE deep_merge(original, new_data, result) AS (
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
                        WHEN new_data ? key
                        THEN new_data -> key
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
        params.push(key, fields['@context:merge'], replayId, deriveType(replayId));
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
                      WHEN new_data ? key
                      THEN new_data -> key
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

    return { sql, params };
  }

  function handleContextDelete(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const path = fields['@context:delete'];
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:delete');
    const params = [];
    let sql = '';

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
        params.push(key, path, replayId, deriveType(replayId));
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
        params.push(key, pathParts, replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextAppend(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { path, value } = JSON.parse(fields['@context:append']);
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:append');
    const params = [];
    let sql = '';

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
      params.push(key, pathParts, JSON.stringify([value]), replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextPrepend(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { path, value } = JSON.parse(fields['@context:prepend']);
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:prepend');
    const params = [];
    let sql = '';

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
      params.push(key, pathParts, JSON.stringify([value]), replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextRemove(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { path, index } = JSON.parse(fields['@context:remove']);
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:remove');
    const params = [];
    let sql = '';

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
      params.push(key, pathParts, index, replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextIncrement(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { path, value } = JSON.parse(fields['@context:increment']);
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:increment');
    const params = [];
    let sql = '';

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
      params.push(key, pathParts, value, replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextToggle(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const path = fields['@context:toggle'];
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:toggle');
    const params = [];
    let sql = '';

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
      params.push(key, pathParts, replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextSetIfNotExists(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const { path, value } = JSON.parse(fields['@context:setIfNotExists']);
    const pathParts = path.split('.');
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:setIfNotExists');
    const params = [];
    let sql = '';

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
      params.push(key, pathParts, JSON.stringify(value), replayId, deriveType(replayId));
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

    return { sql, params };
  }

  function handleContextGetPath(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const getField = Object.keys(fields).find(k => k.startsWith('@context:get:'));
    const pathKey = getField.replace('@context:get:', '');
    const pathParts = JSON.parse(fields[getField]);
    const params = [];

    // Extract the specific path and store it as a temporary field
    const sql = `
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
    params.push(key, getField, pathParts, deriveType(getField));

    return { sql, params };
  }

  function handleContextGet(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): SqlResult {
    const tableName = context.tableForKey(key, 'hash');
    const path = fields['@context:get'];
    const replayId = Object.keys(fields).find(k => k.includes('-') && k !== '@context:get');
    const params = [];
    let sql = '';

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
        params.push(key, replayId, deriveType(replayId));
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
        params.push(key, pathParts, replayId, deriveType(replayId));
      } else {
        sql = `
          SELECT COALESCE((context #> $2::text[])::text, 'null') as new_value
          FROM ${tableName}
          WHERE key = $1 AND is_live
        `;
        params.push(key, pathParts);
      }
    }

    return { sql, params };
  }
}
