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
          // Create the enumerated type if it doesn't exist
          await client.query(`
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'type_enum') THEN
                CREATE TYPE type_enum AS ENUM ('jmark', 'hmark', 'status', 'jdata', 'adata', 'udata', 'other');
              END IF;
            END$$;
          `);

          // Create the jobs table
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              id TEXT PRIMARY KEY,                     -- Unique job identifier (string)
              entity TEXT,                             -- Entity type (e.g., 'user', 'book', etc.)
              status INTEGER NOT NULL,                 -- Status semaphore for job state
              created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- Auto-generated timestamp
              updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- Tracks last update
              expired_at TIMESTAMP WITH TIME ZONE      -- Expiry timestamp for cleanup
            ) PARTITION BY HASH (id);
          `);

          // Create partitions for the jobs table
          for (let i = 0; i < 8; i++) {
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${tableDef.name}_part_${i} PARTITION OF ${tableDef.name}
              FOR VALUES WITH (modulus 8, remainder ${i});
            `);
          }

          // Add indexes to the jobs table
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_entity_status
            ON ${tableDef.name} (entity, status);
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_expired_at
            ON ${tableDef.name} (expired_at);
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_id_expired_at
            ON ${tableDef.name} (id, expired_at);
          `);

          // Create the attributes table
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name}_attributes (
              job_id TEXT NOT NULL,                    -- Foreign key referencing 'id' in '_jobs'
              field TEXT NOT NULL,                     -- Attribute name
              value TEXT,                              -- Attribute value
              type type_enum NOT NULL,                 -- Attribute type (e.g., 'udata', 'jdata')
              PRIMARY KEY (job_id, field),             -- Composite primary key
              FOREIGN KEY (job_id) REFERENCES ${tableDef.name} (id) ON DELETE CASCADE
            ) PARTITION BY HASH (job_id);
          `);

          // Create partitions for the attributes table
          for (let i = 0; i < 8; i++) {
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${tableDef.name}_attributes_part_${i} PARTITION OF ${tableDef.name}_attributes
              FOR VALUES WITH (modulus 8, remainder ${i});
            `);
          }

          // Add indexes to the attributes table
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_job_field_type
            ON ${tableDef.name}_attributes (job_id, field, type);
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_type_field
            ON ${tableDef.name}_attributes (type, field);
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_job_field
            ON ${tableDef.name}_attributes (job_id, field);
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
