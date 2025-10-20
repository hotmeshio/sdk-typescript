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
        // Tables already exist, no need to acquire lock or create tables
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
          case 'string':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT PRIMARY KEY,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE
              );
            `);
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

            // Create the attributes table with partitioning
            const attributesTableName = `${fullTableName}_attributes`;
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${attributesTableName} (
                job_id UUID NOT NULL,
                field TEXT NOT NULL,
                value TEXT,
                type ${schemaName}.type_enum NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (job_id, field),
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
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_type_field 
              ON ${attributesTableName} (type, field);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_field 
              ON ${attributesTableName} (field);
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

    // Applications table (only hotmesh prefix)
    tableNames.push('hotmesh_applications', 'hotmesh_connections');

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
        name: 'hotmesh_applications',
        type: 'hash',
      },
      {
        schema: 'public',
        name: 'hotmesh_connections',
        type: 'hash',
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
        type: 'sorted_set',
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
