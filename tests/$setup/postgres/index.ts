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

// Drop all user-defined schemas and their objects, then drop all tables in the public schema
export const dropTables = async (postgresClient: any): Promise<void> => {
  // Begin transaction
  await postgresClient.query('BEGIN');

  try {
    // Fetch all user-defined schemas excluding system schemas and 'public'
    const schemasResult = await postgresClient.query(`
      SELECT schema_name 
      FROM information_schema.schemata
      WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
      AND schema_name NOT LIKE 'pg_%'
      AND schema_name <> 'public';
    `);

    const schemas = schemasResult.rows.map(row => row.schema_name);

    // Drop all user-defined schemas except 'public'
    for (const schema of schemas) {
      await postgresClient.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE;`);
    }

    // Drop all tables in the 'public' schema
    const tablesResult = await postgresClient.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public';
    `);

    const tables = tablesResult.rows.map((row: { table_name: string }) => row.table_name);

    for (const table of tables) {
      await postgresClient.query(`DROP TABLE IF EXISTS "public"."${table}" CASCADE;`);
    }

    // Commit transaction
    await postgresClient.query('COMMIT');
  } catch (error) {
    // Rollback transaction on error
    await postgresClient.query('ROLLBACK');
    console.error('Error during schema and table dropping:', error);
    throw error;
  }
};

// Truncate all tables in all user-defined schemas
export const truncateTables = async (postgresClient: any): Promise<void> => {
  await postgresClient.query('BEGIN');
  try {
    // Fetch all user-defined schemas excluding system schemas
    const schemasResult = await postgresClient.query(`
      SELECT schema_name 
      FROM information_schema.schemata
      WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
      AND schema_name NOT LIKE 'pg_%';
    `);

    const schemas = schemasResult.rows.map(row => row.schema_name);

    // Fetch all tables in these schemas
    const tablesResult = await postgresClient.query(`
      SELECT table_schema, table_name 
      FROM information_schema.tables 
      WHERE table_schema = ANY ($1::text[]);
    `, [schemas]);

    const tables = tablesResult.rows.map(
      (row: { table_schema: string; table_name: string }) => `"${row.table_schema}"."${row.table_name}"`
    );

    if (tables.length > 0) {
      // Truncate all tables
      await postgresClient.query(`TRUNCATE ${tables.join(', ')} RESTART IDENTITY CASCADE;`);
    }

    await postgresClient.query('COMMIT');
  } catch (error) {
    await postgresClient.query('ROLLBACK');
    console.error('Error during table truncation:', error);
    throw error;
  }
};