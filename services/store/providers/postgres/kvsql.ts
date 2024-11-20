import format from 'pg-format';

import { KeyStoreParams } from '../../../../types/hotmesh';
import { PostgresClientType, PostgresJobEnumType } from '../../../../types/postgres';
import {
  HScanResult,
  HSetOptions,
  KVSQLProviderTransaction,
  ProviderTransaction,
  SetOptions,
  ZAddOptions
} from '../../../../types/provider';
import { HMNS, KeyService } from '../../../../modules/key';

/**
 * Utility function to format SQL commands with parameters.
 * Replaces $1, $2, etc., with %L or %s based on parameter types.
 */
function formatSqlCommand(sql: string, params: any[]): string {
  const formatParams: any[] = [];

  // Replace $1, $2, etc., with %L or %s and collect parameters
  const formattedSql = sql.replace(/\$(\d+)/g, (match, p1) => {
    const index = parseInt(p1, 10) - 1; // Convert $1 to index 0
    const param = params[index];

    if (param === null || param === undefined) {
      return 'NULL';
    }

    let specifier: string;
    if (typeof param === 'number') {
      specifier = '%s';
    } else if (typeof param === 'string') {
      specifier = '%L';
    } else if (typeof param === 'boolean') {
      specifier = '%L';
    } else {
      // For other types like arrays or objects, you may need to handle them differently
      specifier = '%L';
    }

    formatParams.push(param);
    return specifier;
  });

  // Use pg-format to safely interpolate parameters into the SQL command
  return format(formattedSql, ...formatParams);
}


class Multi implements KVSQLProviderTransaction {
  [key: string]: any;
  private commands: Array<{
    sql: string;
    params: any[];
    returnType: string;
    transform?: (rows: any[]) => any;
  }> = [];

  constructor(private kvsql: KVSQL) {}

  addCommand(
    sql: string,
    params: any[],
    returnType: string,
    transform?: (rows: any[]) => any
  ): this {
    this.commands.push({ sql, params, returnType, transform });
    return this;
  }

  // Methods that add SQL commands to the batch
  set(key: string, value: string, options?: SetOptions): this {
    const { sql, params } = this.kvsql._set(key, value, options);
    return this.addCommand(sql, params, 'boolean');
  }

  setnx(key: string, value: string): this {
    const { sql, params } = this.kvsql._set(key, value, { nx: true });
    return this.addCommand(sql, params, 'boolean');
  }

  setnxex(key: string, value: string, expireSeconds: number): this {
    const { sql, params } = this.kvsql._set(
      key,
      value,
      { nx: true, ex: expireSeconds },
    );
    return this.addCommand(sql, params, 'boolean');
  }

  get(key: string): this {
    const { sql, params } = this.kvsql._get(key);
    return this.addCommand(sql, params, 'string');
  }

  del(key: string): this {
    const { sql, params } = this.kvsql._del(key);
    return this.addCommand(sql, params, 'number');
  }

  expire(key: string, seconds: number): this {
    const { sql, params } = this.kvsql._expire(key, seconds);
    return this.addCommand(sql, params, 'boolean');
  }

  hset(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions
  ): this {
    const { sql, params } = this.kvsql._hset(key, fields, options);
    return this.addCommand(sql, params, 'number');
  }

  hget(key: string, field: string): this {
    const { sql, params } = this.kvsql._hget(key, field);
    return this.addCommand(sql, params, 'string');
  }

  hdel(key: string, fields: string[]): this {
    const { sql, params } = this.kvsql._hdel(key, fields);
    return this.addCommand(sql, params, 'number');
  }

  hmget(key: string, fields: string[]): this {
    const { sql, params } = this.kvsql._hmget(key, fields);
    const transform = (rows: any[]) => {
      const fieldValueMap = new Map(rows.map((row) => [row.field, row.value]));
      return fields.map((field) => fieldValueMap.get(field) || null);
    };
    return this.addCommand(sql, params, 'array', transform);
  }

  hgetall(key: string): this {
    const { sql, params } = this.kvsql._hgetall(key);
    const transform = (rows: any[]) => {
      const result: Record<string, string> = {};
      for (const row of rows) {
        result[row.field] = row.value;
      }
      return result;
    };
    return this.addCommand(sql, params, 'object', transform);
  }

  hincrbyfloat(key: string, field: string, increment: number): this {
    const { sql, params } = this.kvsql._hincrbyfloat(key, field, increment);
    return this.addCommand(sql, params, 'number', (rows) =>
      parseFloat(rows[0].value)
    );
  }

  hscan(key: string, cursor: string, count: number = 10): this {
    const { sql, params } = this.kvsql._hscan(key, cursor, count);
    const transform = (rows: any[]) => {
      const items: Record<string, string> = {};
      for (const row of rows) {
        items[row.field] = row.value;
      }
      const newCursor = cursor + rows.length;
      return { cursor: newCursor.toString(), items };
    };
    return this.addCommand(sql, params, 'object', transform);
  }

  lrange(key: string, start: number, end: number): this {
    const { sql, params } = this.kvsql._lrange(key, start, end);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => row.value)
    );
  }

  rpush(key: string, value: string | string[]): this {
    const { sql, params } = this.kvsql._rpush(key, value);
    return this.addCommand(sql, params, 'number', (rows) => rows[0]?.count || 0);
  }

  lpush(key: string, value: string | string[]): this {
    const { sql, params } = this.kvsql._lpush(key, value);
    return this.addCommand(sql, params, 'number', (rows) => rows[0]?.count || 0);
  }

  lpop(key: string): this {
    const { sql, params } = this.kvsql._lpop(key);
    return this.addCommand(sql, params, 'string');
  }

  lmove(
    source: string,
    destination: string,
    srcPosition: 'LEFT' | 'RIGHT',
    destPosition: 'LEFT' | 'RIGHT'
  ): this {
    const { sql, params } = this.kvsql._lmove(
      source,
      destination,
      srcPosition,
      destPosition
    );
    return this.addCommand(sql, params, 'string');
  }

  zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions
  ): this {
    const { sql, params } = this.kvsql._zadd(key, score, member, options);
    return this.addCommand(sql, params, 'number', (rows) => rows[0]?.count || 0);
  }

  zrange(key: string, start: number, stop: number): this {
    const { sql, params } = this.kvsql._zrange(key, start, stop);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => row.member)
    );
  }

  zrangebyscore(key: string, min: number, max: number): this {
    const { sql, params } = this.kvsql._zrangebyscore(key, min, max);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => row.member)
    );
  }

  zrangebyscore_withscores(key: string, min: number, max: number): this {
    const { sql, params } = this.kvsql._zrangebyscore_withscores(key, min, max);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => ({ member: row.member, score: row.score }))
    );
  }

  zrem(key: string, member: string): this {
    const { sql, params } = this.kvsql._zrem(key, member);
    return this.addCommand(sql, params, 'number', (rows) => rows[0]?.count || 0);
  }

  zrank(key: string, member: string): this {
    const { sql, params } = this.kvsql._zrank(key, member);
    return this.addCommand(sql, params, 'number', (rows) =>
      rows[0]?.rank !== undefined ? parseInt(rows[0].rank, 10) - 1 : null
    );
  }

  scan(cursor: number, count: number = 10): this {
    const { sql, params } = this.kvsql._scan(cursor, count);
    const transform = (rows: any[]) => {
      const keys = rows.map((row) => row.key);
      const newCursor = cursor + rows.length;
      return { cursor: newCursor, keys };
    };
    return this.addCommand(sql, params, 'object', transform);
  }

  rename(oldKey: string, newKey: string): this {
    const { sql, params } = this.kvsql._rename(oldKey, newKey);
    return this.addCommand(sql, params, 'void');
  }

  async exec(): Promise<any[]> {
    const client = this.kvsql.pgClient;

    try {
      await client.query('BEGIN');

      const sqlStatements: string[] = [];
      const results: any[] = [];

      for (const cmd of this.commands) {
        // Replace $1, $2, etc., with appropriate format specifiers (%L or %s)
        const formattedSql = formatSqlCommand(cmd.sql, cmd.params);
        sqlStatements.push(formattedSql);
      }

      // Combine all formatted SQL statements into a single query
      const combinedSql = sqlStatements.join(';\n');

      // Execute the combined SQL
      const res = await client.query(combinedSql);

      // Process the results
      const resultSets = Array.isArray(res) ? res : [res];
      let resIndex = 0;
      for (const cmd of this.commands) {
        const resultSet = resultSets[resIndex++];
        const rows = resultSet?.rows || [];
        let result;

        if (cmd.transform) {
          result = cmd.transform(rows);
        } else {
          switch (cmd.returnType) {
            case 'string':
              result = rows[0]?.value || null;
              break;
            case 'number':
              result = rows[0]?.count || 0;
              break;
            case 'boolean':
              result = rows[0]?.success ?? rows.length > 0;
              break;
            case 'array':
              result = rows.map((row) => row.value);
              break;
            case 'object':
              result = rows[0] || {};
              break;
            default:
              result = rows;
          }
        }

        results.push(result);
      }

      await client.query('COMMIT');
      return results;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    }
  }
}

export class KVSQL {
  pgClient: PostgresClientType;
  namespace: string;
  appId: string;

  constructor(pgClient: PostgresClientType, namespace: string, appId: string) {
    this.pgClient = pgClient;
    this.namespace = namespace;
    this.appId = appId;
  }

  getMulti(): ProviderTransaction {
    return new Multi(this);
  }

  transact(): ProviderTransaction {
    return new Multi(this);
  }

  exec(...args: any[]): Promise<Array<any>> {
    return Promise.resolve([]);
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    //no-op
    return '';
  }

  /**
   * Resolves the table name when provided a key
   */
  tableForKey(key: string, stats_type?: 'hash' | 'sorted_set' | 'list'): string {
    if (key === HMNS) {
      return 'hotmesh_connections';
    }
    const [_, appName, abbrev, ...rest] = key.split(':');
    if (appName === 'a') {
      return 'hotmesh_applications';
    }
    const id = rest?.length ? rest.join(':') : '';
    const entity = KeyService.resolveEntityType(abbrev, id);
    if (this.safeName(this.appId) !== this.safeName(appName)) {
      throw new Error(`App ID mismatch: ${this.appId} !== ${appName}`);
    }
    if (entity === 'stats') {
      if (stats_type === 'sorted_set') {
        return `hotmesh_${this.safeName(appName)}_stats_ordered`;
      } else if (stats_type === 'list') {
        return `hotmesh_${this.safeName(appName)}_stats_indexed`;
      } else if (stats_type === 'hash') {
        return `hotmesh_${this.safeName(appName)}_stats_counted`;
      } else {
        throw new Error(`Unknown stats type [${stats_type}] for key [${key}]`);
      }
    }
    if (entity === 'unknown_entity') {
      throw new Error(`Unknown entity type abbreviation: ${abbrev}`);
    } else if (entity === 'applications') {
      return 'hotmesh_applications';
    } else {
      return `hotmesh_${this.safeName(appName)}_${entity}`;
    }
  }

  /**
   * NOTE: implement key optimizations once tables are working!
   */
  reconstituteKey(entityType: string, id: string): string {
    //resolve the abbreviation and then reconstruct the key
    const entity = KeyService.resolveAbbreviation(entityType);
    return KeyService.reconstituteKey({
      namespace: this.namespace,
      app: this.appId,
      entity,
      id,
    });
  }

  safeName(input: string, prefix = ''): string {
    if (!input) {
      return 'connections';
    }
    // Step 1: Trim whitespace and convert to lowercase
    let tableName = input.trim().toLowerCase();
  
    // Step 2: Replace any non-alphanumeric characters with underscores
    tableName = tableName.replace(/[^a-z0-9]+/g, '_');
  
    // Step 3: Add prefix if provided
    if (prefix) {
      tableName = `${prefix}_${tableName}`;
    }
  
    // Step 4: Truncate to PostgreSQL's maximum table name length (63 characters)
    if (tableName.length > 63) {
      tableName = tableName.slice(0, 63);
    }
  
    // Step 5: Remove any trailing underscores from truncation or invalid replacement
    tableName = tableName.replace(/_+$/g, '');
  
    // Step 6: Ensure table name is not empty; fall back to a default name if needed
    if (!tableName) {
      tableName = 'connections';
    }
  
    return tableName;
  }

  // String Commands

  async get(key: string, multi?: ProviderTransaction): Promise<string | null> {
    const { sql, params } = this._get(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'string');
      return Promise.resolve(null);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows[0]?.value || null;
    }
  }

  _get(key: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key);
    const sql = `
      SELECT value FROM ${tableName}
      WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      LIMIT 1
    `;
    const params = [key];
    return { sql, params };
  }

  async setnx(key: string, value: string, multi?: ProviderTransaction): Promise<boolean> {
    const { sql, params } = this._set(key, value, { nx: true });
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  }

  async setnxex(key: string, value: string, delay: number, multi?: ProviderTransaction): Promise<boolean> {
    const { sql, params } = this._set(key, value, { nx: true, ex: delay });
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  }

  async set(
    key: string,
    value: string,
    options?: SetOptions,
    multi?: ProviderTransaction
  ): Promise<boolean> {
    const { sql, params } = this._set(key, value, options);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  }

  _set(
    key: string,
    value: string,
    options?: SetOptions
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key);
    let sql = '';
    const params = [key, value];
    let expiryClause = '';

    if (options?.ex) {
      expiryClause = ', expiry = NOW() + INTERVAL \'' + options.ex + ' seconds\'';
    }

    if (options?.nx) {
      sql = `
        INSERT INTO ${tableName} (key, value${expiryClause ? ', expiry' : ''})
        VALUES ($1, $2${expiryClause ? ', NOW() + INTERVAL \'' + options.ex + ' seconds\'' : ''})
        ON CONFLICT DO NOTHING
        RETURNING true as success
      `;
    } else {
      sql = `
        INSERT INTO ${tableName} (key, value${expiryClause ? ', expiry' : ''})
        VALUES ($1, $2${expiryClause ? ', NOW() + INTERVAL \'' + options.ex + ' seconds\'' : ''})
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value${expiryClause}
        RETURNING true as success
      `;
    }

    return { sql, params };
  }

  async del(key: string, multi?: ProviderTransaction): Promise<number> {
    const { sql, params } = this._del(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  }

  _del(key: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key);
    const sql = `
      WITH deleted AS (
        DELETE FROM ${tableName} WHERE key = $1
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM deleted
    `;
    const params = [key];
    return { sql, params };
  }

  async expire(
    key: string,
    seconds: number,
    multi?: ProviderTransaction
  ): Promise<boolean> {
    const { sql, params } = this._expire(key, seconds);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  }

  _expire(key: string, seconds: number): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key);
    const expiryTime = new Date(Date.now() + seconds * 1000);
    const sql = `
      UPDATE ${tableName}
      SET expiry = $2
      WHERE key = $1
      RETURNING true as success
    `;
    const params = [key, expiryTime];
    return { sql, params };
  }

  // Hash Commands

  async hsetnx(key: string, field: string, value: string, multi?: ProviderTransaction): Promise<number> {
    const { sql, params } = this._hset(key, {[field]: value}, { nx: true });
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rowCount;
    }
  }

  async hset(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
    multi?: ProviderTransaction
  ): Promise<number> {
    const { sql, params } = this._hset(key, fields, options);
    
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rowCount;
    }
  }

  /**
   * Derives the enumerated `type` value based on the field name when
   * setting a field in a jobs table (a 'jobshash' table type).
   */
  deriveType(fieldName: string): PostgresJobEnumType {
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
  }

  _hset(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions
  ): { sql: string; params: any[] } {
    let sql = '';
    const params = [key];
    const tableName = this.tableForKey(key, 'hash');
    const isJobsTable = tableName.endsWith('_jobs');
  
    // Create SQL dynamically to handle multiple fields and values
    const fieldEntries = Object.entries(fields);
    fieldEntries.forEach(([field, value], index) => {
      params.push(field, value);
      if (isJobsTable) {
        params.push(this.deriveType(field)); // Add derived type
      }
    });
  
    const placeholders = fieldEntries
      .map((_, i) => isJobsTable
        ? `($1, $${2 + i * 3}, $${3 + i * 3}, $${4 + i * 3})` // For jobs table
        : `($1, $${2 + i * 2}, $${3 + i * 2})`) // For regular table
      .join(', ');
  
    if (options?.nx) {
      sql = `
        INSERT INTO ${tableName} (key, field, value${isJobsTable ? ', type' : ''})
        VALUES ${placeholders}
        ON CONFLICT DO NOTHING
        RETURNING 1 as count
      `;
    } else {
      sql = `
        INSERT INTO ${tableName} (key, field, value${isJobsTable ? ', type' : ''})
        VALUES ${placeholders}
        ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value
        RETURNING 1 as count
      `;
    }
  
    return { sql, params };
  }  

  async hget(
    key: string,
    field: string,
    multi?: ProviderTransaction
  ): Promise<string | null> {
    const { sql, params } = this._hget(key, field);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'string');
      return Promise.resolve(null);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows[0]?.value || null;
    }
  }

  _hget(key: string, field: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'hash');
    const sql = `
      SELECT value FROM ${tableName}
      WHERE key = $1 AND field = $2 AND (expiry IS NULL OR expiry > NOW())
      LIMIT 1
    `;
    const params = [key, field];
    return { sql, params };
  }

  async hdel(
    key: string,
    fields: string[],
    multi?: ProviderTransaction
  ): Promise<number> {
    // Ensure fields is an array
    if (!Array.isArray(fields)) {
      fields = [fields];
    }
    const { sql, params } = this._hdel(key, fields);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  }
  
  _hdel(key: string, fields: string[]): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'hash');
    // Create placeholders for each field
    const fieldPlaceholders = fields.map((_, i) => `$${i + 2}`).join(', ');
    const sql = `
      WITH deleted AS (
        DELETE FROM ${tableName}
        WHERE key = $1 AND field IN (${fieldPlaceholders})
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM deleted
    `;
    const params = [key, ...fields];
    return { sql, params };
  }
  

  async hmget(
    key: string,
    fields: string[],
    multi?: ProviderTransaction
  ): Promise<(string | null)[]> {
    const { sql, params } = this._hmget(key, fields);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'array', (rows) => {
        const fieldValueMap = new Map(
          rows.map((row) => [row.field, row.value])
        );
        return fields.map((field) => fieldValueMap.get(field) || null);
      });
      return Promise.resolve([]);
    } else {
      const res = await this.pgClient.query(sql, params);
      const fieldValueMap = new Map(
        res.rows.map((row) => [row.field, row.value])
      );
      return fields.map((field) => fieldValueMap.get(field) || null);
    }
  }

  _hmget(key: string, fields: string[]): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'hash');
    const sql = `
      SELECT field, value FROM ${tableName}
      WHERE key = $1 AND field = ANY($2::text[])
    `;
    const params = [key, fields];
    return { sql, params };
  }

  async hgetall(
    key: string,
    multi?: ProviderTransaction
  ): Promise<Record<string, string>> {
    const { sql, params } = this._hgetall(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'object', (rows) => {
        const result: Record<string, string> = {};
        for (const row of rows) {
          result[row.field] = row.value;
        }
        return result;
      });
      return Promise.resolve({});
    } else {
      const res = await this.pgClient.query(sql, params);
      const result: Record<string, string> = {};
      for (const row of res.rows) {
        result[row.field] = row.value;
      }
      return result;
    }
  }

  _hgetall(key: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'hash');
    const sql = `
      SELECT field, value FROM ${tableName}
      WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
    `;
    const params = [key];
    return { sql, params };
  }

  async hincrbyfloat(
    key: string,
    field: string,
    increment: number,
    multi?: ProviderTransaction
  ): Promise<number> {
    const { sql, params } = this._hincrbyfloat(key, field, increment);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number', (rows) =>
        parseFloat(rows[0].value)
      );
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return parseFloat(res.rows[0].value);
    }
  }
  _hincrbyfloat(
    key: string,
    field: string,
    increment: number
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'hash');
    const isJobsTable = tableName.endsWith('_jobs');
    let sql: string;
    const params: any[] = [key, field, increment];
  
    if (isJobsTable) {
      // Derive the `type` value for jobs table
      const type = this.deriveType(field);
      params.push(type); // Add derived type to params
  
      sql = `
        INSERT INTO ${tableName} (key, field, value, type)
        VALUES ($1, $2, ($3)::text, $4)
        ON CONFLICT (key, field) DO UPDATE SET value = (${tableName}.value::double precision + $3::double precision)::text
        RETURNING value
      `;
    } else {
      sql = `
        INSERT INTO ${tableName} (key, field, value)
        VALUES ($1, $2, ($3)::text)
        ON CONFLICT (key, field) DO UPDATE SET value = (${tableName}.value::double precision + $3::double precision)::text
        RETURNING value
      `;
    }
  
    return { sql, params };
  }  

  async hscan(
    key: string,
    cursor: string,
    count: number = 10,
    pattern?: string,
    multi?: ProviderTransaction
  ): Promise<HScanResult> {
    const { sql, params } = this._hscan(key, cursor, count, pattern);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'object', (rows) => {
        const items: Record<string, string> = {};
        for (const row of rows) {
          items[row.field] = row.value;
        }
        const newCursor = rows.length < count ? 0 : Number(cursor) + rows.length;
        return { cursor: newCursor.toString(), items };
      });
      return Promise.resolve({ cursor: '0', items: {} });
    } else {
      const res = await this.pgClient.query(sql, params);
      const items: Record<string, string> = {};
      for (const row of res.rows) {
        items[row.field] = row.value;
      }
      const newCursor = res.rowCount < count ? 0 : Number(cursor) + res.rowCount;
      return { cursor: newCursor.toString(), items };
    }
  }
  
  _hscan(
    key: string,
    cursor: string,
    count: number,
    pattern?: string
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'hash');
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
  }

  // List Commands
  async lrange(
    key: string,
    start: number,
    end: number,
    multi?: ProviderTransaction
  ): Promise<string[]> {
    const { sql, params } = this._lrange(key, start, end);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "array", (rows) =>
        rows.map((row) => row.value)
      );
      return Promise.resolve([]);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows.map((row) => row.value);
    }
  }
  
  _lrange(
    key: string,
    start: number,
    end: number
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'list');
    const sql = `
      WITH numbered AS (
        SELECT value,
               ROW_NUMBER() OVER (ORDER BY "index" ASC) - 1 AS rn,
               COUNT(*) OVER() - 1 AS max_index
        FROM ${tableName}
        WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      ),
      indices AS (
        SELECT
          LEAST(GREATEST(CASE WHEN $2 >= 0 THEN $2 ELSE max_index + $2 + 1 END, 0), max_index) AS adjusted_start,
          LEAST(GREATEST(CASE WHEN $3 >= 0 THEN $3 ELSE max_index + $3 + 1 END, 0), max_index) AS adjusted_end
        FROM (SELECT max_index FROM numbered LIMIT 1) AS mi
      )
      SELECT value
      FROM numbered, indices
      WHERE rn BETWEEN indices.adjusted_start AND indices.adjusted_end
      ORDER BY rn ASC
    `;
    const params = [key, start, end];
    return { sql, params };
  }

  async rpush(
    key: string,
    value: string | string[],
    multi?: ProviderTransaction
  ): Promise<number> {
    const { sql, params } = this._rpush(key, value);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "number", (rows) => rows[0]?.count || 0);
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  }
  
  _rpush(key: string, value: string | string[]): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'list');
    const values = Array.isArray(value) ? value : [value];
    const placeholders = values.map((_, i) => `($1, (SELECT COALESCE(MAX("index"), 0) + ${i + 1} FROM ${tableName} WHERE key = $1), $${i + 2})`).join(', ');
  
    const sql = `
      WITH inserted AS (
        INSERT INTO ${tableName} (key, "index", value)
        VALUES ${placeholders}
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM inserted
    `;
    const params = [key, ...values];
    return { sql, params };
  }  

  async lpush(
    key: string,
    value: string | string[],
    multi?: ProviderTransaction
  ): Promise<number> {
    const { sql, params } = this._lpush(key, value);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "number", (rows) => rows[0]?.count || 0);
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  }

  _lpush(key: string, value: string | string[]): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'list');
    const values = Array.isArray(value) ? value : [value];
    const placeholders = values.map((_, i) => `($1, (SELECT COALESCE(MIN("index"), 0) - ${i + 1} FROM ${tableName} WHERE key = $1), $${i + 2})`).join(', ');
  
    const sql = `
      WITH inserted AS (
        INSERT INTO ${tableName} (key, "index", value)
        VALUES ${placeholders}
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM inserted
    `;
    const params = [key, ...values];
    return { sql, params };
  }  
  
  async lpop(
    key: string,
    multi?: ProviderTransaction
  ): Promise<string | null> {
    const { sql, params } = this._lpop(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "string");
      return Promise.resolve(null);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows[0]?.value || null;
    }
  }

  _lpop(key: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'list');
    const sql = `
      DELETE FROM ${tableName}
      WHERE key = $1 AND "index" = (
        SELECT MIN("index") FROM ${tableName} WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      )
      RETURNING value
    `;
    const params = [key];
    return { sql, params };
  }  

  async lmove(
    source: string,
    destination: string,
    srcPosition: "LEFT" | "RIGHT",
    destPosition: "LEFT" | "RIGHT",
    multi?: ProviderTransaction
  ): Promise<string | null> {
    const { sql, params } = this._lmove(
      source,
      destination,
      srcPosition,
      destPosition
    );
    if (multi) {
      (multi as Multi).addCommand(sql, params, "string");
      return Promise.resolve(null);
    } else {
      const client = this.pgClient;
      try {
        await client.query("BEGIN");
        const res = await client.query(sql, params);
        await client.query("COMMIT");
        return res.rows[0]?.value || null;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    }
  }
  
  _lmove(
    source: string,
    destination: string,
    srcPosition: "LEFT" | "RIGHT",
    destPosition: "LEFT" | "RIGHT"
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(source, 'list');
    const srcOrder = srcPosition === "LEFT" ? "ASC" : "DESC";
    const destIndexAdjustment =
      destPosition === "LEFT"
        ? `(SELECT COALESCE(MIN("index"), 0) - 1 FROM ${tableName} WHERE key = $2)`
        : `(SELECT COALESCE(MAX("index"), 0) + 1 FROM ${tableName} WHERE key = $2)`;
  
    const sql = `
      WITH moved AS (
        DELETE FROM ${tableName}
        WHERE ctid IN (
          SELECT ctid FROM ${tableName}
          WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
          ORDER BY "index" ${srcOrder}
          LIMIT 1
        )
        RETURNING value
      ),
      inserted AS (
        INSERT INTO ${tableName} (key, "index", value)
        SELECT $2, ${destIndexAdjustment}, value FROM moved
        RETURNING value
      )
      SELECT value FROM inserted
    `;
    const params = [source, destination];
    return { sql, params };
  }  

  // Sorted Set Commands

  async zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions,
    multi?: ProviderTransaction
  ): Promise<number> {
    const { sql, params } = this._zadd(key, score, member, options);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "number", (rows) => rows[0]?.count || 0);
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  }

  _zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    let sql = "";
    const params = [key, member, score];

    if (options?.nx) {
      sql = `
        INSERT INTO ${tableName} (key, member, score)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        RETURNING 1 as count
      `;
    } else {
      sql = `
        INSERT INTO ${tableName} (key, member, score)
        VALUES ($1, $2, $3)
        ON CONFLICT (key, member) DO UPDATE SET score = $3
        RETURNING 1 as count
      `;
    }

    return { sql, params };
  }

  async zrange(
    key: string,
    start: number,
    stop: number,
    facet?: 'WITHSCORES',
    multi?: ProviderTransaction,
  ): Promise<string[]> {
    const { sql, params } = this._zrange(key, start, stop, facet);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "array", (rows) => {
        if (facet === 'WITHSCORES') {
          // Include scores in the result
          return rows.flatMap((row) => [row.member, row.score.toString()]);
        } else {
          return rows.map((row) => row.member);
        }
      });
      return Promise.resolve([]);
    } else {
      const res = await this.pgClient.query(sql, params);
      if (facet === 'WITHSCORES') {
        // Include scores in the result
        return res.rows.flatMap((row) => [row.member, row.score.toString()]);
      } else {
        return res.rows.map((row) => row.member);
      }
    }
  }
  
  _zrange(
    key: string,
    start: number,
    stop: number,
    facet?: 'WITHSCORES',
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    const selectColumns = facet === 'WITHSCORES' ? 'member, score' : 'member';
    const sql = `
      WITH total_entries AS (
        SELECT COUNT(*) - 1 AS max_index FROM ${tableName}
        WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      ), ordered_entries AS (
        SELECT ${selectColumns},
               ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rn,
               (SELECT max_index FROM total_entries) AS max_index
        FROM ${tableName}
        WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      ), indices AS (
        SELECT
          CASE WHEN $2 >= 0 THEN $2 ELSE max_index + $2 + 1 END AS adjusted_start,
          CASE WHEN $3 >= 0 THEN $3 ELSE max_index + $3 + 1 END AS adjusted_stop
        FROM total_entries
      )
      SELECT ${selectColumns}
      FROM ordered_entries, indices
      WHERE rn BETWEEN LEAST(GREATEST(adjusted_start, 0), max_index)
                   AND LEAST(GREATEST(adjusted_stop, 0), max_index)
      ORDER BY rn ASC;
    `;
    const params = [key, start, stop];
    return { sql, params };
  }
  

  async zscore(
    key: string,
    member: string,
    multi?: ProviderTransaction,
  ): Promise<number | null> {
    const { sql, params } = this._zscore(key, member);
    
    if (multi) {
      (multi as Multi).addCommand(sql, params, "single", (row) => {
        return row ? parseFloat((row as unknown as {score:string}).score) : null;
      });
      return Promise.resolve(null);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows.length ? parseFloat(res.rows[0].score) : null;
    }
  }
  
  _zscore(
    key: string,
    member: string,
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    const sql = `
      SELECT score
      FROM ${tableName}
      WHERE key = $1 AND member = $2
        AND (expiry IS NULL OR expiry > NOW())
      LIMIT 1
    `;
    const params = [key, member];
    return { sql, params };
  }
  
  async zrangebyscore(
    key: string,
    min: number,
    max: number,
    multi?: ProviderTransaction
  ): Promise<string[]> {
    const { sql, params } = this._zrangebyscore(key, min, max);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "array", (rows) =>
        rows.map((row) => row.member)
      );
      return Promise.resolve([]);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows.map((row) => row.member);
    }
  }

  _zrangebyscore(
    key: string,
    min: number,
    max: number
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    const sql = `
      SELECT member FROM ${tableName}
      WHERE key = $1 AND score BETWEEN $2 AND $3 AND (expiry IS NULL OR expiry > NOW())
      ORDER BY score ASC, member ASC
    `;
    const params = [key, min, max];
    return { sql, params };
  }

  async zrangebyscore_withscores(
    key: string,
    min: number,
    max: number,
    multi?: ProviderTransaction
  ): Promise<{ member: string; score: number }[]> {
    const { sql, params } = this._zrangebyscore_withscores(key, min, max);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "array", (rows) =>
        rows.map((row) => ({ member: row.member, score: row.score }))
      );
      return Promise.resolve([]);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows.map((row) => ({ member: row.member, score: row.score }));
    }
  }

  _zrangebyscore_withscores(
    key: string,
    min: number,
    max: number
  ): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    const sql = `
      SELECT member, score FROM ${tableName}
      WHERE key = $1 AND score BETWEEN $2 AND $3 AND (expiry IS NULL OR expiry > NOW())
      ORDER BY score ASC, member ASC
    `;
    const params = [key, min, max];
    return { sql, params };
  }

  async zrem(
    key: string,
    member: string,
    multi?: ProviderTransaction
  ): Promise<number> {
    const { sql, params } = this._zrem(key, member);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "number", (rows) => rows[0]?.count || 0);
      return Promise.resolve(0);
    } else {
      const res = await this.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  }

  _zrem(key: string, member: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    const sql = `
      WITH deleted AS (
        DELETE FROM ${tableName}
        WHERE key = $1 AND member = $2
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM deleted
    `;
    const params = [key, member];
    return { sql, params };
  }

  async zrank(
    key: string,
    member: string,
    multi?: ProviderTransaction
  ): Promise<number | null> {
    const { sql, params } = this._zrank(key, member);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "number", (rows) =>
        rows[0]?.rank !== undefined ? parseInt(rows[0].rank, 10) - 1 : null
      );
      return Promise.resolve(null);
    } else {
      const res = await this.pgClient.query(sql, params);
      return res.rows[0]?.rank
        ? (parseInt(res.rows[0].rank, 10) > 0 ? parseInt(res.rows[0].rank, 10) - 1 : null)
        : null;
    }
  }

  _zrank(key: string, member: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key, 'sorted_set');
    const sql = `
      WITH member_score AS (
        SELECT score FROM ${tableName}
        WHERE key = $1 AND member = $2 AND (expiry IS NULL OR expiry > NOW())
      )
      SELECT COUNT(*) AS rank FROM ${tableName} ms, member_score
      WHERE ms.key = $1 AND (expiry IS NULL OR expiry > NOW())
      AND (ms.score < member_score.score OR (ms.score = member_score.score AND ms.member < $2))
    `;
    const params = [key, member];
    return { sql, params };
  }  

  // Key Management Commands
  async scan(
    cursor: number,
    count: number = 10,
    pattern?: string,
    multi?: ProviderTransaction
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
      const res = await this.pgClient.query(sql, params);
      const keys = res.rows.map((row) => row.key);
      const newCursor = cursor + res.rowCount;
      return { cursor: newCursor, keys };
    }
  }
  
  // Updated _scan method
  _scan(cursor: number, count: number, pattern?: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(`_:${this.appId}:j:_`);
    let sql = `
      SELECT key FROM ${tableName}
      WHERE (expiry IS NULL OR expiry > NOW())
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
  }

  async rename(
    oldKey: string,
    newKey: string,
    multi?: ProviderTransaction
  ): Promise<void> {
    const { sql, params } = this._rename(oldKey, newKey);
    if (multi) {
      (multi as Multi).addCommand(sql, params, "void");
      return Promise.resolve();
    } else {
      const client = this.pgClient;
      try {
        await client.query("BEGIN");
        await client.query(sql, params);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    }
  }

  _rename(oldKey: string, newKey: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(oldKey, 'list');
    const sql = `
      UPDATE ${tableName} SET key = $2 WHERE key = $1;
    `;
    const params = [oldKey, newKey];
    return { sql, params };
  }

  async exists(key: string): Promise<string | 0> {
    const { sql, params } = this._exists(key);
    const res = await this.pgClient.query(sql, params);
    return res.rows.length ? res.rows[0].table_name : 0;
  }
  
  _exists(key: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key);
    const sql = `
      SELECT FROM ${tableName}
      WHERE key = $1
        AND (expiry IS NULL OR expiry > NOW())
      LIMIT 1;
    `;
    const params = [key];
    return { sql, params };
  }  
}
