import { ProviderTransaction } from '../../../../../types/provider';

interface Multi extends ProviderTransaction {
  addCommand: (
    sql: string,
    params: any[],
    returnType: string,
    transform?: (rows: any[]) => any,
  ) => void;
}

export const listModule = (context: any) => ({
  async lrange(
    key: string,
    start: number,
    end: number,
    multi?: ProviderTransaction,
  ): Promise<string[]> {
    const { sql, params } = this._lrange(key, start, end);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'array', (rows) =>
        rows.map((row) => row.value),
      );
      return Promise.resolve([]);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rows.map((row) => row.value);
    }
  },

  _lrange(
    key: string,
    start: number,
    end: number,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'list');
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
  },

  async rpush(
    key: string,
    value: string | string[],
    multi?: ProviderTransaction,
  ): Promise<number> {
    const { sql, params } = this._rpush(key, value);
    if (multi) {
      (multi as Multi).addCommand(
        sql,
        params,
        'number',
        (rows) => rows[0]?.count || 0,
      );
      return Promise.resolve(0);
    } else {
      const res = await context.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  },

  _rpush(
    key: string,
    value: string | string[],
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'list');
    const values = Array.isArray(value) ? value : [value];
    const placeholders = values
      .map(
        (_, i) =>
          `($1, (SELECT COALESCE(MAX("index"), 0) + ${i + 1} FROM ${tableName} WHERE key = $1), $${i + 2})`,
      )
      .join(', ');

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
  },

  async lpush(
    key: string,
    value: string | string[],
    multi?: ProviderTransaction,
  ): Promise<number> {
    const { sql, params } = this._lpush(key, value);
    if (multi) {
      (multi as Multi).addCommand(
        sql,
        params,
        'number',
        (rows) => rows[0]?.count || 0,
      );
      return Promise.resolve(0);
    } else {
      const res = await context.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  },

  _lpush(
    key: string,
    value: string | string[],
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'list');
    const values = Array.isArray(value) ? value : [value];
    const placeholders = values
      .map(
        (_, i) =>
          `($1, (SELECT COALESCE(MIN("index"), 0) - ${i + 1} FROM ${tableName} WHERE key = $1), $${i + 2})`,
      )
      .join(', ');

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
  },

  async lpop(key: string, multi?: ProviderTransaction): Promise<string | null> {
    const { sql, params } = this._lpop(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'string');
      return Promise.resolve(null);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rows[0]?.value || null;
    }
  },

  _lpop(key: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'list');
    const sql = `
      DELETE FROM ${tableName}
      WHERE key = $1 AND "index" = (
        SELECT MIN("index") FROM ${tableName} WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      )
      RETURNING value
    `;
    const params = [key];
    return { sql, params };
  },

  async lmove(
    source: string,
    destination: string,
    srcPosition: 'LEFT' | 'RIGHT',
    destPosition: 'LEFT' | 'RIGHT',
    multi?: ProviderTransaction,
  ): Promise<string | null> {
    const { sql, params } = this._lmove(
      source,
      destination,
      srcPosition,
      destPosition,
    );
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'string');
      return Promise.resolve(null);
    } else {
      const client = context.pgClient;
      try {
        await client.query('BEGIN');
        const res = await client.query(sql, params);
        await client.query('COMMIT');
        return res.rows[0]?.value || null;
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      }
    }
  },

  _lmove(
    source: string,
    destination: string,
    srcPosition: 'LEFT' | 'RIGHT',
    destPosition: 'LEFT' | 'RIGHT',
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(source, 'list');
    const srcOrder = srcPosition === 'LEFT' ? 'ASC' : 'DESC';
    const destIndexAdjustment =
      destPosition === 'LEFT'
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
  },

  async rename(
    oldKey: string,
    newKey: string,
    multi?: ProviderTransaction,
  ): Promise<void> {
    const { sql, params } = this._rename(oldKey, newKey);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'void');
      return Promise.resolve();
    } else {
      const client = context.pgClient;
      try {
        await client.query('BEGIN');
        await client.query(sql, params);
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      }
    }
  },

  _rename(oldKey: string, newKey: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(oldKey, 'list');
    const sql = `
      UPDATE ${tableName} SET key = $2 WHERE key = $1;
    `;
    const params = [oldKey, newKey];
    return { sql, params };
  },
});
