import config from '../config';

export const ioredis_options = {
  host: config.REDIS_HOST,
  port: config.REDIS_PORT,
  password: config.REDIS_PASSWORD,
  db: config.REDIS_DATABASE,
};

export const redis_options = {
  socket: {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    tls: false,
  },
  password: config.REDIS_PASSWORD,
  database: config.REDIS_DATABASE,
};

export const postgres_options = {
  user: config.POSTGRES_USER,
  host: config.POSTGRES_HOST,
  database: config.POSTGRES_DB,
  password: config.POSTGRES_PASSWORD,
  port: config.POSTGRES_PORT,
};

// Drop all partitioned tables and their parent tables
export const dropTables = async (postgresClient: any): Promise<string[]> => {
  // Fetch all partitioned tables and their partitions
  const partitionsResult = await postgresClient.query(`
    SELECT 
      child.relname AS partition_name, 
      parent.relname AS parent_table
    FROM 
      pg_inherits 
      JOIN pg_class child ON pg_inherits.inhrelid = child.oid
      JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
      JOIN pg_namespace n ON n.oid = child.relnamespace
    WHERE 
      n.nspname = 'public'
      AND child.relkind = 'r'; -- Include only tables
  `);

  // Group partitions by their parent tables
  const partitionsMap = partitionsResult.rows.reduce((map, row) => {
    if (!map[row.parent_table]) {
      map[row.parent_table] = [];
    }
    map[row.parent_table].push(row.partition_name);
    return map;
  }, {});

  // Drop partitions first
  for (const [_parentTable, partitions] of Object.entries(
    partitionsMap,
  ) as unknown as [string, string[]]) {
    for (const partition of partitions) {
      await postgresClient.query(
        `DROP TABLE IF EXISTS "${partition}" CASCADE;`,
      );
    }
  }

  // Drop remaining tables
  const tablesResult = await postgresClient.query(`
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public';
  `);

  const tables = tablesResult.rows.map(
    (row: { table_name: string }) => row.table_name,
  );

  for (const table of tables) {
    await postgresClient.query(`DROP TABLE IF EXISTS "${table}" CASCADE;`);
  }
  return tables;
};

// Truncate all tables in the public schema
export const truncateTables = async (postgresClient: any, tables: string[]) => {
  await postgresClient.query('BEGIN');
  try {
    for (const table of tables) {
      const existsResult = await postgresClient.query(
        `SELECT EXISTS (
          SELECT 1 FROM pg_catalog.pg_tables 
          WHERE schemaname = 'public' AND tablename = $1
        );`,
        [table],
      );

      if (existsResult.rows[0].exists) {
        await postgresClient.query(
          `TRUNCATE ${table} RESTART IDENTITY CASCADE;`,
        );
      }
    }
    await postgresClient.query('COMMIT');
  } catch (error) {
    await postgresClient.query('ROLLBACK');
    console.error('Error during table truncation:', error);
    throw error;
  }
};
