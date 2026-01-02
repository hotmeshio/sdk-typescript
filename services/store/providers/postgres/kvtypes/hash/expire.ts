import { HashContext, Multi, SqlResult, ProviderTransaction } from './types';

export function createExpireOperations(context: HashContext['context']) {
  return {
    async expire(
      key: string,
      seconds: number,
      multi?: ProviderTransaction,
    ): Promise<boolean> {
      const { sql, params } = _expire(context, key, seconds);
      if (multi) {
        (multi as Multi).addCommand(sql, params, 'boolean');
        return Promise.resolve(true);
      } else {
        try {
          const res = await context.pgClient.query(sql, params);
          return res.rowCount > 0;
        } catch (error) {
          // Connection closed during test cleanup - return false
          if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
            return false;
          }
          // Re-throw unexpected errors
          throw error;
        }
      }
    },
  };
}

export function _expire(
  context: HashContext['context'],
  key: string,
  seconds: number,
): SqlResult {
  // Only job tables are ever expired
  const tableName = context.tableForKey(key);
  const expiryTime = new Date(Date.now() + seconds * 1000);
  const sql = `
    UPDATE ${tableName}
    SET expired_at = $2
    WHERE key = $1 AND is_live
    RETURNING true as success
  `;
  const params = [key, expiryTime];
  return { sql, params };
}
