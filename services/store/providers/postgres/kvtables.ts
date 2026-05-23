import {
  HMSH_DEPLOYMENT_DELAY,
  HMSH_DEPLOYMENT_PAUSE,
} from '../../../../modules/enums';
import { sleepFor } from '../../../../modules/utils';
import {
  PostgresClientType,
  PostgresPoolClientType,
} from '../../../../types/postgres';

import type { PostgresStoreService } from './postgres';

export const KVTables = (context: PostgresStoreService) => ({
  /**
   * Deploys the necessary tables with the specified naming strategy.
   * @param appName - The name of the application.
   */
  async deploy(appName: string): Promise<void> {
    const transactionClient = context.pgClient as any;

    let client: any;
    let releaseClient = false;

    if (
      transactionClient?.totalCount !== undefined &&
      transactionClient?.idleCount !== undefined
    ) {
      // It's a Pool, need to acquire a client
      client = await (transactionClient as PostgresPoolClientType).connect();
      releaseClient = true;
    } else {
      // Assume it's a connected Client
      client = transactionClient as PostgresClientType;
    }

    try {
      // First, check if tables already exist (no lock needed)
      const tablesExist = await this.checkIfTablesExist(client, appName);
      if (tablesExist) {
        // Tables exist; apply any pending migrations
        await this.migrate(client, appName);
        return;
      }

      // Tables don't exist, need to acquire lock and create them
      const lockId = this.getAdvisoryLockId(appName);
      const lockResult = await client.query(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [lockId],
      );

      if (lockResult.rows[0].locked) {
        // Begin transaction
        await client.query('BEGIN');

        // Double-check tables don't exist (race condition safety)
        const tablesStillMissing = !(await this.checkIfTablesExist(
          client,
          appName,
        ));
        if (tablesStillMissing) {
          await this.createTables(client, appName);
        }

        // Commit transaction
        await client.query('COMMIT');

        // Release the lock
        await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
      } else {
        // Release the client before waiting
        if (releaseClient && client.release) {
          await client.release();
          releaseClient = false;
        }

        // Wait for the deploy process to complete
        await this.waitForTablesCreation(lockId, appName);
      }
    } catch (error) {
      console.error(error);
      context.logger.error('Error deploying tables', { error });
      throw error;
    } finally {
      if (releaseClient && client.release) {
        await client.release();
      }
    }
  },

  getAdvisoryLockId(appName: string): number {
    return this.hashStringToInt(appName);
  },

  hashStringToInt(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0; // Convert to 32-bit integer
    }
    return Math.abs(hash);
  },

  async waitForTablesCreation(lockId: number, appName: string): Promise<void> {
    let retries = 0;
    const maxRetries = Math.round(
      HMSH_DEPLOYMENT_DELAY / HMSH_DEPLOYMENT_PAUSE,
    );

    while (retries < maxRetries) {
      await sleepFor(HMSH_DEPLOYMENT_PAUSE);

      let client: any;
      let releaseClient = false;
      const transactionClient = context.pgClient as any;

      if (
        transactionClient?.totalCount !== undefined &&
        transactionClient?.idleCount !== undefined
      ) {
        // It's a Pool, need to acquire a client
        client = await (transactionClient as PostgresPoolClientType).connect();
        releaseClient = true;
      } else {
        // Assume it's a connected Client
        client = transactionClient as PostgresClientType;
      }

      try {
        // Check if tables exist directly (most efficient check)
        const tablesExist = await this.checkIfTablesExist(client, appName);
        if (tablesExist) {
          // Tables now exist, deployment is complete
          return;
        }

        // Fallback: check if the lock has been released (indicates completion)
        const lockCheck = await client.query(
          "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint) AS unlocked",
          [lockId],
        );
        if (lockCheck.rows[0].unlocked) {
          // Lock has been released, tables should exist now
          const tablesExistAfterLock = await this.checkIfTablesExist(
            client,
            appName,
          );
          if (tablesExistAfterLock) {
            return;
          }
        }
      } finally {
        if (releaseClient && client.release) {
          await client.release();
        }
      }

      retries++;
    }
    console.error('table-create-timeout', { appName });
    throw new Error('Timeout waiting for table creation');
  },

  async checkIfTablesExist(
    client: PostgresClientType,
    appName: string,
  ): Promise<boolean> {
    const tableNames = this.getTableNames(appName);

    const checkTablePromises = tableNames.map((tableName) =>
      client.query(`SELECT to_regclass('${tableName}') AS table`),
    );

    const results = await Promise.all(checkTablePromises);
    return results.every((res) => res.rows[0].table !== null);
  },

  async migrate(
    client: PostgresClientType | PostgresPoolClientType,
    appName: string,
  ): Promise<void> {
    const schemaName = context.storeClient.safeName(appName);
    const jobsTable = `${schemaName}.jobs`;

    // v0.14.5: track updated_at on job status changes
    const { rows } = await client.query(
      `SELECT 1 FROM pg_trigger WHERE tgname = 'trg_update_jobs_updated_at' LIMIT 1`,
    );
    if (rows.length === 0) {
      await client.query(`
        CREATE OR REPLACE FUNCTION ${schemaName}.update_jobs_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
          IF NEW.status <> OLD.status THEN
            NEW.updated_at = NOW();
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
      `);
      await client.query(`
        DROP TRIGGER IF EXISTS trg_update_jobs_updated_at ON ${jobsTable};
        CREATE TRIGGER trg_update_jobs_updated_at
        BEFORE UPDATE ON ${jobsTable}
        FOR EACH ROW EXECUTE FUNCTION ${schemaName}.update_jobs_updated_at();
      `);
    }

  },

  async createTables(
    client: PostgresClientType | PostgresPoolClientType,
    appName: string,
  ): Promise<void> {
    try {
      await client.query('BEGIN');
      const schemaName = context.storeClient.safeName(appName);
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName};`);
      const tableDefinitions = this.getTableDefinitions(appName);

      for (const tableDef of tableDefinitions) {
        const fullTableName = `${tableDef.schema}.${tableDef.name}`;

        switch (tableDef.type) {
          case 'relational_app':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                app_id TEXT PRIMARY KEY,
                version TEXT NOT NULL DEFAULT '1',
                active BOOLEAN DEFAULT TRUE,
                settings JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
              );
            `);
            await client.query(`
              CREATE TABLE IF NOT EXISTS public.hmsh_application_versions (
                app_id TEXT NOT NULL REFERENCES public.hmsh_applications(app_id) ON DELETE CASCADE,
                version TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'deployed',
                deployed_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (app_id, version)
              );
            `);
            break;

          case 'relational_connection':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                guid TEXT NOT NULL,
                app_id TEXT NOT NULL,
                role TEXT NOT NULL,
                version TEXT NOT NULL,
                connected_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (guid, app_id)
              );
            `);
            break;

          case 'string':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT PRIMARY KEY,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE
              );
            `);
            if (tableDef.name === 'signal_registry') {
              await client.query(`
                CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_expiry
                  ON ${fullTableName} (expiry)
                  WHERE expiry IS NOT NULL;
              `);
            }
            break;

          case 'hash':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT NOT NULL,
                field TEXT NOT NULL,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (key, field)
              );
            `);
            break;

          case 'jobhash':
            // Create the enum type in the schema
            await client.query(`
              DO $$
              BEGIN
                IF NOT EXISTS (
                  SELECT 1 FROM pg_type t
                  JOIN pg_namespace n ON n.oid = t.typnamespace
                  WHERE t.typname = 'type_enum' AND n.nspname = '${schemaName}'
                ) THEN
                  CREATE TYPE ${schemaName}.type_enum AS ENUM ('jmark', 'hmark', 'status', 'jdata', 'adata', 'udata', 'other');
                END IF;
              END$$;
            `);

            // Create the main jobs table with partitioning on id
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                id UUID DEFAULT gen_random_uuid(),
                key TEXT NOT NULL,
                entity TEXT,
                status INTEGER NOT NULL,
                context JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                expired_at TIMESTAMP WITH TIME ZONE,
                pruned_at TIMESTAMP WITH TIME ZONE,
                is_live BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (id)
              ) PARTITION BY HASH (id);
            `);

            // Create GIN index for full JSONB search
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_context_gin 
              ON ${fullTableName} USING GIN (context);
            `);

            // Create partitions using a DO block
            await client.query(`
              DO $$
              BEGIN
                FOR i IN 0..7 LOOP
                  EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS ${fullTableName}_part_%s PARTITION OF ${fullTableName}
                    FOR VALUES WITH (modulus 8, remainder %s)',
                    i, i
                  );
                END LOOP;
              END$$;
            `);

            // Create optimized indexes
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expired_at
              ON ${fullTableName} (key, expired_at) INCLUDE (is_live);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_entity_status
              ON ${fullTableName} (entity, status);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_expired_at
              ON ${fullTableName} (expired_at);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_pruned_at
              ON ${fullTableName} (pruned_at) WHERE pruned_at IS NULL;
            `);

            // Index for paginated entity listing with sort (dashboard entity queries)
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_entity_created
              ON ${fullTableName} (entity, created_at DESC)
              WHERE entity IS NOT NULL;
            `);

            // Create function to update is_live flag in the schema
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.update_is_live()
              RETURNS TRIGGER AS $$
              BEGIN
                NEW.is_live := NEW.expired_at IS NULL OR NEW.expired_at > NOW();
                RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for is_live updates
            await client.query(`
              CREATE TRIGGER trg_update_is_live
              BEFORE INSERT OR UPDATE ON ${fullTableName}
              FOR EACH ROW EXECUTE PROCEDURE ${schemaName}.update_is_live();
            `);

            // Create function to enforce uniqueness of live jobs
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.enforce_live_job_uniqueness()
              RETURNS TRIGGER AS $$
              BEGIN
                IF (NEW.expired_at IS NULL OR NEW.expired_at > NOW()) THEN
                  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.key, 0));
                  IF EXISTS (
                    SELECT 1 FROM ${fullTableName}
                    WHERE key = NEW.key
                    AND (expired_at IS NULL OR expired_at > NOW())
                    AND id <> NEW.id
                  ) THEN
                    RAISE EXCEPTION 'A live job with key % already exists.', NEW.key;
                  END IF;
                END IF;
                RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for uniqueness enforcement
            await client.query(`
              CREATE TRIGGER trg_enforce_live_job_uniqueness
              BEFORE INSERT OR UPDATE ON ${fullTableName}
              FOR EACH ROW EXECUTE PROCEDURE ${schemaName}.enforce_live_job_uniqueness();
            `);

            // Create function to update updated_at on status changes
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.update_jobs_updated_at()
              RETURNS TRIGGER AS $$
              BEGIN
                IF NEW.status <> OLD.status THEN
                  NEW.updated_at = NOW();
                END IF;
                RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for updated_at on job status changes
            await client.query(`
              DROP TRIGGER IF EXISTS trg_update_jobs_updated_at ON ${fullTableName};
              CREATE TRIGGER trg_update_jobs_updated_at
              BEFORE UPDATE ON ${fullTableName}
              FOR EACH ROW EXECUTE FUNCTION ${schemaName}.update_jobs_updated_at();
            `);

            // Create the attributes table with partitioning
            const attributesTableName = `${fullTableName}_attributes`;
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${attributesTableName} (
                job_id UUID NOT NULL,
                symbol TEXT NOT NULL,
                dimension TEXT NOT NULL DEFAULT '',
                value TEXT,
                type ${schemaName}.type_enum NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (job_id, symbol, dimension),
                FOREIGN KEY (job_id) REFERENCES ${fullTableName} (id) ON DELETE CASCADE
              ) PARTITION BY HASH (job_id);
            `);

            // Create trigger function for updating updated_at only for mutable types
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.update_attributes_updated_at()
              RETURNS TRIGGER AS $$
              BEGIN
                  IF NEW.type IN ('udata', 'jdata', 'hmark', 'jmark') AND 
                     (OLD.value IS NULL OR NEW.value <> OLD.value) THEN
                      NEW.updated_at = NOW();
                  END IF;
                  RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for updated_at updates
            await client.query(`
              DROP TRIGGER IF EXISTS trg_update_attributes_updated_at ON ${attributesTableName};
              CREATE TRIGGER trg_update_attributes_updated_at
                  BEFORE UPDATE ON ${attributesTableName}
                  FOR EACH ROW
                  EXECUTE FUNCTION ${schemaName}.update_attributes_updated_at();
            `);

            // Create partitions for attributes table
            await client.query(`
              DO $$
              BEGIN
                FOR i IN 0..7 LOOP
                  EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS ${attributesTableName}_part_%s PARTITION OF ${attributesTableName}
                    FOR VALUES WITH (modulus 8, remainder %s)',
                    i, i
                  );
                END LOOP;
              END$$;
            `);

            // Create indexes for attributes table
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_type_symbol
              ON ${attributesTableName} (type, symbol);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_symbol
              ON ${attributesTableName} (symbol);
            `);
            break;

          case 'list':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT NOT NULL,
                index BIGINT NOT NULL,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (key, index)
              );
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expiry 
              ON ${fullTableName} (key, expiry);
            `);
            break;

          case 'sorted_set':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT NOT NULL,
                member TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                expiry TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (key, member)
              );
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_score_member
              ON ${fullTableName} (key, score, member);
            `);
            break;

          case 'time_hooks':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                id BIGSERIAL PRIMARY KEY,
                type TEXT NOT NULL,
                job_id TEXT NOT NULL,
                graph_id TEXT NOT NULL,
                activity_id TEXT NOT NULL,
                dad TEXT NOT NULL DEFAULT '',
                score DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
              );
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_score
              ON ${fullTableName} (score);
            `);
            // Server-side function: atomically pop expired hook + insert TIMEHOOK into engine_streams
            await client.query(`
              CREATE OR REPLACE FUNCTION ${tableDef.schema}.process_next_time_hook(
                p_now_ms DOUBLE PRECISION,
                p_stream_name TEXT,
                p_guid TEXT
              )
              RETURNS TABLE (
                hook_id BIGINT,
                hook_type TEXT,
                hook_job_id TEXT,
                hook_graph_id TEXT,
                hook_activity_id TEXT,
                hook_dad TEXT,
                hook_score DOUBLE PRECISION,
                handled_by_db BOOLEAN
              ) AS $$
              DECLARE
                v_row RECORD;
                v_aid TEXT;
                v_dad TEXT;
                v_parts TEXT[];
                v_message TEXT;
              BEGIN
                DELETE FROM ${fullTableName}
                WHERE id = (
                  SELECT ts.id
                  FROM ${fullTableName} ts
                  WHERE ts.score <= p_now_ms
                  ORDER BY ts.score ASC, ts.id ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                RETURNING * INTO v_row;

                IF v_row IS NULL THEN
                  RETURN;
                END IF;

                IF v_row.type = 'sleep' THEN
                  v_parts := string_to_array(v_row.activity_id, ',');
                  v_aid := v_parts[1];
                  IF array_length(v_parts, 1) > 1 THEN
                    v_dad := ',' || array_to_string(v_parts[2:], ',');
                  ELSE
                    v_dad := v_row.dad;
                  END IF;

                  v_message := json_build_object(
                    'type', 'timehook',
                    'metadata', json_build_object(
                      'guid', p_guid,
                      'jid', v_row.job_id,
                      'gid', v_row.graph_id,
                      'dad', v_dad,
                      'aid', v_aid
                    ),
                    'data', json_build_object('timestamp', extract(epoch from now()) * 1000)
                  )::text;

                  INSERT INTO ${tableDef.schema}.engine_streams
                    (stream_name, message, priority)
                  VALUES
                    (p_stream_name, v_message, 5);

                  RETURN QUERY SELECT v_row.id, v_row.type, v_row.job_id, v_row.graph_id,
                                      v_row.activity_id, v_row.dad, v_row.score, true;
                ELSE
                  RETURN QUERY SELECT v_row.id, v_row.type, v_row.job_id, v_row.graph_id,
                                      v_row.activity_id, v_row.dad, v_row.score, false;
                END IF;
              END;
              $$ LANGUAGE plpgsql;
            `);
            break;

          default:
            context.logger.warn(`Unknown table type for ${tableDef.name}`);
            break;
        }
      }

      // Commit transaction
      await client.query('COMMIT');
    } catch (error) {
      context.logger.error('postgres-create-tables-error', { error });
      await client.query('ROLLBACK');
      throw error;
    }
  },

  getTableNames(appName: string): string[] {
    const tableNames = [];

    // Public relational tables
    tableNames.push(
      'public.hmsh_applications',
      'public.hmsh_application_versions',
      'public.hmsh_connections',
    );

    // Other tables with appName
    const tablesWithAppName = [
      'throttles',
      'roles',
      'task_priorities',
      'task_schedules',
      'task_lists',
      'events',
      'jobs',
      'stats_counted',
      'stats_indexed',
      'stats_ordered',
      'versions',
      'signal_patterns',
      'signal_registry',
      'symbols',
    ];

    tablesWithAppName.forEach((table) => {
      tableNames.push(`${context.storeClient.safeName(appName)}.${table}`);
    });

    return tableNames;
  },

  getTableDefinitions(
    appName: string,
  ): Array<{ schema: string; name: string; type: string }> {
    const schemaName = context.storeClient.safeName(appName);

    const tableDefinitions = [
      {
        schema: 'public',
        name: 'hmsh_applications',
        type: 'relational_app',
      },
      {
        schema: 'public',
        name: 'hmsh_connections',
        type: 'relational_connection',
      },
      {
        schema: schemaName,
        name: 'throttles',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'roles',
        type: 'string',
      },
      {
        schema: schemaName,
        name: 'task_schedules',
        type: 'time_hooks',
      },
      {
        schema: schemaName,
        name: 'task_priorities',
        type: 'sorted_set',
      },
      {
        schema: schemaName,
        name: 'task_lists',
        type: 'list',
      },
      {
        schema: schemaName,
        name: 'events',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'jobs',
        type: 'jobhash', // Adds partitioning, indexes, and enum type
      },
      {
        schema: schemaName,
        name: 'stats_counted',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'stats_ordered',
        type: 'sorted_set',
      },
      {
        schema: schemaName,
        name: 'stats_indexed',
        type: 'list',
      },
      {
        schema: schemaName,
        name: 'versions',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'signal_patterns',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'symbols',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'signal_registry',
        type: 'string',
      },
    ];

    return tableDefinitions;
  },
});
