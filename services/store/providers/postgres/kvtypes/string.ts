import { ProviderTransaction, SetOptions } from '../../../../../types/provider';

interface Multi extends ProviderTransaction {
  addCommand: (sql: string, params: any[], returnType: string) => void;
}

export const stringModule = (context: any) => ({
  async get(key: string, multi?: ProviderTransaction): Promise<string | null> {
    const { sql, params } = this._get(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'string');
      return Promise.resolve(null);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rows[0]?.value || null;
    }
  },

  _get(key: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key);
    const sql = `
      SELECT value FROM ${tableName}
      WHERE key = $1 AND (expiry IS NULL OR expiry > NOW())
      LIMIT 1
    `;
    const params = [key];
    return { sql, params };
  },

  async setnx(
    key: string,
    value: string,
    multi?: ProviderTransaction,
  ): Promise<boolean> {
    const { sql, params } = this._set(key, value, { nx: true });
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  },

  async setnxex(
    key: string,
    value: string,
    delay: number,
    multi?: ProviderTransaction,
  ): Promise<boolean> {
    const { sql, params } = this._set(key, value, { nx: true, ex: delay });
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  },

  async set(
    key: string,
    value: string,
    options?: SetOptions,
    multi?: ProviderTransaction,
  ): Promise<boolean> {
    const { sql, params } = this._set(key, value, options);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'boolean');
      return Promise.resolve(true);
    } else {
      const res = await context.pgClient.query(sql, params);
      return res.rowCount > 0;
    }
  },

  _set(
    key: string,
    value: string,
    options?: SetOptions,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key);
    let sql = '';
    const params = [key, value];
    let expiryClause = '';

    if (options?.ex) {
      expiryClause = ", expiry = NOW() + INTERVAL '" + options.ex + " seconds'";
    }

    if (options?.nx) {
      // INSERT only if no valid ownership exists
      sql = `
        INSERT INTO ${tableName} (key, value${expiryClause ? ', expiry' : ''})
        VALUES ($1, $2${expiryClause ? ", NOW() + INTERVAL '" + options.ex + " seconds'" : ''})
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value${expiryClause ? ', expiry = EXCLUDED.expiry' : ''}
        WHERE ${tableName}.expiry IS NULL OR ${tableName}.expiry <= NOW()
        RETURNING true as success
      `;
    } else {
      // INSERT or UPDATE, reclaiming expired ownership if necessary
      sql = `
        INSERT INTO ${tableName} (key, value${expiryClause ? ', expiry' : ''})
        VALUES ($1, $2${expiryClause ? ", NOW() + INTERVAL '" + options.ex + " seconds'" : ''})
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value${expiryClause ? ', expiry = EXCLUDED.expiry' : ''}
        WHERE ${tableName}.expiry IS NULL OR ${tableName}.expiry <= NOW()
        RETURNING true as success
      `;
    }

    return { sql, params };
  },

  async del(key: string, multi?: ProviderTransaction): Promise<number> {
    const { sql, params } = this._del(key);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number');
      return Promise.resolve(0);
    } else {
      const res = await context.pgClient.query(sql, params);
      return Number(res.rows[0]?.count || 0);
    }
  },

  _del(key: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key);
    const sql = `
      WITH deleted AS (
        DELETE FROM ${tableName} WHERE key = $1
        RETURNING 1
      )
      SELECT COUNT(*) as count FROM deleted
    `;
    const params = [key];
    return { sql, params };
  },
});
