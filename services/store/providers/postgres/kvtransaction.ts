import format from 'pg-format';

import {
  HSetOptions,
  KVSQLProviderTransaction,
  SetOptions,
  ZAddOptions,
} from '../../../../types/provider';

import type { KVSQL } from './kvsql';

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

export class KVTransaction implements KVSQLProviderTransaction {
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
    transform?: (rows: any[]) => any,
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
    const { sql, params } = this.kvsql._set(key, value, {
      nx: true,
      ex: expireSeconds,
    });
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
    options?: HSetOptions,
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
    this.kvsql.hgetall(key, this);
    const transform = (rows: any[]) => {
      const result: Record<string, string> = {};
      for (const row of rows) {
        result[row.field] = row.value;
      }
      return result;
    };
    return this;
  }

  hincrbyfloat(key: string, field: string, increment: number): this {
    const { sql, params } = this.kvsql._hincrbyfloat(key, field, increment);
    return this.addCommand(sql, params, 'number', (rows) => {
      try {
        return parseFloat(rows[0].value);
      } catch (err) {
        console.error('hincrbyfloat error', err, sql, params, rows);
      }
    });
  }

  hscan(key: string, cursor: string, count = 10): this {
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
      rows.map((row) => row.value),
    );
  }

  rpush(key: string, value: string | string[]): this {
    const { sql, params } = this.kvsql._rpush(key, value);
    return this.addCommand(
      sql,
      params,
      'number',
      (rows) => rows[0]?.count || 0,
    );
  }

  lpush(key: string, value: string | string[]): this {
    const { sql, params } = this.kvsql._lpush(key, value);
    return this.addCommand(
      sql,
      params,
      'number',
      (rows) => rows[0]?.count || 0,
    );
  }

  lpop(key: string): this {
    const { sql, params } = this.kvsql._lpop(key);
    return this.addCommand(sql, params, 'string');
  }

  lmove(
    source: string,
    destination: string,
    srcPosition: 'LEFT' | 'RIGHT',
    destPosition: 'LEFT' | 'RIGHT',
  ): this {
    const { sql, params } = this.kvsql._lmove(
      source,
      destination,
      srcPosition,
      destPosition,
    );
    return this.addCommand(sql, params, 'string');
  }

  zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions,
  ): this {
    const { sql, params } = this.kvsql._zadd(key, score, member, options);
    return this.addCommand(
      sql,
      params,
      'number',
      (rows) => rows[0]?.count || 0,
    );
  }

  zrange(key: string, start: number, stop: number): this {
    const { sql, params } = this.kvsql._zrange(key, start, stop);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => row.member),
    );
  }

  zrangebyscore(key: string, min: number, max: number): this {
    const { sql, params } = this.kvsql._zrangebyscore(key, min, max);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => row.member),
    );
  }

  zrangebyscore_withscores(key: string, min: number, max: number): this {
    const { sql, params } = this.kvsql._zrangebyscore_withscores(key, min, max);
    return this.addCommand(sql, params, 'array', (rows) =>
      rows.map((row) => ({ member: row.member, score: row.score })),
    );
  }

  zrem(key: string, member: string): this {
    const { sql, params } = this.kvsql._zrem(key, member);
    return this.addCommand(
      sql,
      params,
      'number',
      (rows) => rows[0]?.count || 0,
    );
  }

  zrank(key: string, member: string): this {
    const { sql, params } = this.kvsql._zrank(key, member);
    return this.addCommand(sql, params, 'number', (rows) =>
      rows[0]?.rank !== undefined ? parseInt(rows[0].rank, 10) - 1 : null,
    );
  }

  scan(cursor: number, count = 10): this {
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
      const results: any[] = [];

      //build the transaction SQL string
      const sqlStatements: string[] = ['BEGIN'];
      for (const cmd of this.commands) {
        const formattedSql = formatSqlCommand(cmd.sql, cmd.params);
        sqlStatements.push(formattedSql);
      }
      sqlStatements.push('COMMIT;');

      // Combine into one and execute
      const combinedSql = sqlStatements.join(';\n');
      const res = await client.query(combinedSql);

      // Process the results
      const resultSets = Array.isArray(res) ? res : [res];
      //filter out the BEGIN and COMMIT results
      resultSets.shift();
      resultSets.pop();
      let resIndex = 0;
      //iterate set of cached commands that were just executed to process the results
      for (const cmd of this.commands) {
        const resultSet = resultSets[resIndex++];
        const rows = resultSet?.rows || [];
        let result: any;

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
      return results;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    }
  }
}
