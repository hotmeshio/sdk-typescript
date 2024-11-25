import { sleepFor } from '../../../../modules/utils';
import { PostgresClientType } from '../../../../types/postgres';
import type { PostgresStoreService } from './postgres';

export const KVTables = (context: PostgresStoreService) => ({

  /**
   * Deploys the necessary tables with the specified naming strategy.
   * @param appName - The name of the application.
   */
  async deploy(appName: string): Promise<void> {
    const client = context.pgClient;

    try {
      // Acquire advisory lock
      const lockId = this.getAdvisoryLockId(appName);
      const lockResult = await client.query(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [lockId],
      );

      if (lockResult.rows[0].locked) {
        // Begin transaction
        await client.query('BEGIN');

        // Check and create tables
        const tablesExist = await this.checkIfTablesExist(client, appName);
        if (!tablesExist) {
          await this.createTables(client, appName);
        }

        // Commit transaction
        await client.query('COMMIT');

        // Release the lock
        await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
      } else {
        // Wait for the deploy process to complete
        await this.waitForTablesCreation(client, lockId, appName);
      }
    } catch (error) {
      context.logger.error('Error deploying tables', { error });
      throw error;
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

  async waitForTablesCreation(
    client: PostgresClientType,
    lockId: number,
    appName: string,
  ): Promise<void> {
    let retries = 0;
    const maxRetries = 20;
    while (retries < maxRetries) {
      await sleepFor(150);
      const lockCheck = await client.query(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint) AS unlocked",
        [lockId],
      );
      if (lockCheck.rows[0].unlocked) {
        // Lock has been released, tables should exist now
        const tablesExist = await this.checkIfTablesExist(client, appName);
        if (tablesExist) {
          return;
        }
      }
      retries++;
    }
    throw new Error('Timeout waiting for table creation');
  },

  async checkIfTablesExist(
    client: PostgresClientType,
    appName: string,
  ): Promise<boolean> {
    const tableNames = this.getTableNames(appName);

    const checkTablePromises = tableNames.map((tableName) =>
      client.query(`SELECT to_regclass('public.${tableName}') AS table`),
    );

    const results = await Promise.all(checkTablePromises);
    return results.every((res) => res.rows[0].table !== null);
  },

  async createTables(
    client: PostgresClientType,
    appName: string,
  ): Promise<void> {
    // Begin transaction
    await client.query('BEGIN');

    const tableDefinitions = this.getTableDefinitions(
      context.storeClient.safeName(appName),
    );

    for (const tableDef of tableDefinitions) {
      switch (tableDef.type) {
        case 'string':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT PRIMARY KEY,
              value TEXT,
              expiry TIMESTAMP WITH TIME ZONE
            );
          `);
          break;

        case 'hash':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT NOT NULL,
              field TEXT NOT NULL,
              value TEXT,
              expiry TIMESTAMP WITH TIME ZONE,
              PRIMARY KEY (key, field)
            );
          `);
          break;

        case 'jobhash':
          // Create the enum type// Create the enum type
          await client.query(`
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'type_enum') THEN
                CREATE TYPE type_enum AS ENUM ('jmark', 'hmark', 'status', 'jdata', 'adata', 'udata', 'other');
              END IF;
            END$$;
          `);

          // Create the main jobs table with partitioning on id
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              id UUID DEFAULT gen_random_uuid(),
              key TEXT NOT NULL,
              entity TEXT,
              status INTEGER NOT NULL,
              created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
              updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
              expired_at TIMESTAMP WITH TIME ZONE,
              is_live BOOLEAN DEFAULT TRUE,
              PRIMARY KEY (id)  -- Primary key on id which is also our partitioning key
            ) PARTITION BY HASH (id);
          `);

          // Create partitions using a DO block
          await client.query(`
            DO $$
            BEGIN
              FOR i IN 0..7 LOOP
                EXECUTE format(
                  'CREATE TABLE IF NOT EXISTS ${tableDef.name}_part_%s PARTITION OF ${tableDef.name} 
                  FOR VALUES WITH (modulus 8, remainder %s)',
                  i, i
                );
              END LOOP;
            END$$;
          `);

          // Create optimized indexes
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expired_at 
            ON ${tableDef.name} (key, expired_at) INCLUDE (is_live);
          `);

          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_entity_status 
            ON ${tableDef.name} (entity, status);
          `);

          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_expired_at 
            ON ${tableDef.name} (expired_at);
          `);

          // Create function to update is_live flag
          await client.query(`
            CREATE OR REPLACE FUNCTION update_is_live()
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
            BEFORE INSERT OR UPDATE ON ${tableDef.name}
            FOR EACH ROW EXECUTE PROCEDURE update_is_live();
          `);

          // Create function to enforce uniqueness of live jobs
          await client.query(`
            CREATE OR REPLACE FUNCTION enforce_live_job_uniqueness()
            RETURNS TRIGGER AS $$
            BEGIN
              IF (NEW.expired_at IS NULL OR NEW.expired_at > NOW()) THEN
                PERFORM pg_advisory_xact_lock(hashtextextended(NEW.key, 0));
                IF EXISTS (
                  SELECT 1 FROM ${tableDef.name}
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
            BEFORE INSERT OR UPDATE ON ${tableDef.name}
            FOR EACH ROW EXECUTE PROCEDURE enforce_live_job_uniqueness();
          `);

          // Create the attributes table with partitioning
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name}_attributes (
              job_id UUID NOT NULL,
              field TEXT NOT NULL,
              value TEXT,
              type type_enum NOT NULL,
              PRIMARY KEY (job_id, field),
              FOREIGN KEY (job_id) REFERENCES ${tableDef.name} (id) ON DELETE CASCADE
            ) PARTITION BY HASH (job_id);
          `);

          // Create partitions for attributes table
          await client.query(`
            DO $$
            BEGIN
              FOR i IN 0..7 LOOP
                EXECUTE format(
                  'CREATE TABLE IF NOT EXISTS ${tableDef.name}_attributes_part_%s PARTITION OF ${tableDef.name}_attributes 
                  FOR VALUES WITH (modulus 8, remainder %s)',
                  i, i
                );
              END LOOP;
            END$$;
          `);

          // Create indexes for attributes table
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_type_field 
            ON ${tableDef.name}_attributes (type, field);
          `);

          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_field 
            ON ${tableDef.name}_attributes (field);
          `);
          break;

        case 'list':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT NOT NULL,
              index BIGINT NOT NULL,
              value TEXT,
              expiry TIMESTAMP WITH TIME ZONE,
              PRIMARY KEY (key, index)
            );
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expiry 
            ON ${tableDef.name} (key, expiry);
          `);
          break;

        case 'sorted_set':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT NOT NULL,
              member TEXT NOT NULL,
              score DOUBLE PRECISION NOT NULL,
              expiry TIMESTAMP WITH TIME ZONE,
              PRIMARY KEY (key, member)
            );
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_score_member 
            ON ${tableDef.name} (key, score, member);
          `);
          break;

        default:
          context.logger.warn(`Unknown table type for ${tableDef.name}`);
          break;
      }
    }

    // Commit transaction
    await client.query('COMMIT');
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
      tableNames.push(`hotmesh_${context.storeClient.safeName(appName)}_${table}`);
    });

    return tableNames;
  },

  getTableDefinitions(appName: string): Array<{ name: string; type: string }> {
    const tableDefinitions = [
      {
        name: 'hotmesh_applications',
        type: 'hash',
      },
      {
        name: `hotmesh_connections`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_throttles`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_roles`,
        type: 'string',
      },
      {
        name: `hotmesh_${appName}_task_schedules`,
        type: 'sorted_set',
      },
      {
        name: `hotmesh_${appName}_task_priorities`,
        type: 'sorted_set',
      },
      {
        name: `hotmesh_${appName}_task_lists`,
        type: 'list',
      },
      {
        name: `hotmesh_${appName}_events`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_jobs`,
        type: 'jobhash', //adds partitioning, indexes, and enum type
      },
      {
        name: `hotmesh_${appName}_stats_counted`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_stats_ordered`,
        type: 'sorted_set',
      },
      {
        name: `hotmesh_${appName}_stats_indexed`,
        type: 'list',
      },
      {
        name: `hotmesh_${appName}_versions`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_signal_patterns`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_symbols`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_signal_registry`,
        type: 'string',
      },
    ];

    return tableDefinitions;
  },
});
