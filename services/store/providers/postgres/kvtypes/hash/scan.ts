import {
  HashContext,
  Multi,
  SqlResult,
  HScanResult,
  ProviderTransaction,
} from './types';

export function createScanOperations(context: HashContext['context']) {
  return {
    async hscan(
      key: string,
      cursor: string,
      count = 10,
      pattern?: string,
      multi?: ProviderTransaction,
    ): Promise<HScanResult> {
      const { sql, params } = _hscan(context, key, cursor, count, pattern);
      if (multi) {
        (multi as Multi).addCommand(sql, params, 'object', (rows) => {
          const items: Record<string, string> = {};
          for (const row of rows) {
            items[row.field] = row.value;
          }
          const newCursor =
            rows.length < count ? 0 : Number(cursor) + rows.length;
          return { cursor: newCursor.toString(), items };
        });
        return Promise.resolve({ cursor: '0', items: {} });
      } else {
        const res = await context.pgClient.query(sql, params);
        const items: Record<string, string> = {};
        for (const row of res.rows) {
          items[row.field] = row.value;
        }
        const newCursor =
          res.rowCount < count ? 0 : Number(cursor) + res.rowCount;
        return { cursor: newCursor.toString(), items };
      }
    },

    async scan(
      cursor: number,
      count = 10,
      pattern?: string,
      multi?: ProviderTransaction,
    ): Promise<{ cursor: number; keys: string[] }> {
      const { sql, params } = _scan(context, cursor, count, pattern);
      if (multi) {
        (multi as Multi).addCommand(sql, params, 'object', (rows) => {
          const keys = rows.map((row) => row.key);
          const newCursor = cursor + rows.length;
          return { cursor: newCursor, keys };
        });
        return Promise.resolve({ cursor: 0, keys: [] });
      } else {
        const res = await context.pgClient.query(sql, params);
        const keys = res.rows.map((row) => row.key);
        const newCursor = cursor + res.rowCount;
        return { cursor: newCursor, keys };
      }
    },
  };
}

export function _hscan(
  context: HashContext['context'],
  key: string,
  cursor: string,
  count: number,
  pattern?: string,
): SqlResult {
  const tableName = context.tableForKey(key, 'hash');
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

export function _scan(
  context: HashContext['context'],
  cursor: number,
  count: number,
  pattern?: string,
): SqlResult {
  const tableName = context.tableForKey(`_:${context.appId}:j:_`);
  let sql = `
    SELECT key FROM ${tableName}
    WHERE (expired_at IS NULL OR expired_at > NOW())
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
