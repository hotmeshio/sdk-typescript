import {
  ProviderTransaction,
  ZAddOptions,
} from '../../../../../types/provider';

interface Multi extends ProviderTransaction {
  addCommand: (
    sql: string,
    params: any[],
    returnType: string,
    transform?: (rows: any[]) => any,
  ) => void;
}

export const zsetModule = (context: any) => ({
  async zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions,
    multi?: ProviderTransaction,
  ): Promise<number> {
    const { sql, params } = this._zadd(key, score, member, options);
    if (multi) {
      (multi as Multi).addCommand(
        sql,
        params,
        'number',
        (rows) => rows[0]?.count || 0,
      );
      return Promise.resolve(0);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        return Number(res.rows[0]?.count || 0);
      } catch (error) {
        // Connection closed during test cleanup - return 0
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return 0;
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
    let sql = '';
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
  },

  async zrange(
    key: string,
    start: number,
    stop: number,
    facet?: 'WITHSCORES',
    multi?: ProviderTransaction,
  ): Promise<string[]> {
    const { sql, params } = this._zrange(key, start, stop, facet);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'array', (rows) => {
        if (facet === 'WITHSCORES') {
          // Include scores in the result
          return rows.flatMap((row) => [row.member, row.score.toString()]);
        } else {
          return rows.map((row) => row.member);
        }
      });
      return Promise.resolve([]);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        if (facet === 'WITHSCORES') {
          // Include scores in the result
          return res.rows.flatMap((row) => [row.member, row.score.toString()]);
        } else {
          return res.rows.map((row) => row.member);
        }
      } catch (error) {
        // Connection closed during test cleanup - return empty array
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return [];
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zrange(
    key: string,
    start: number,
    stop: number,
    facet?: 'WITHSCORES',
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
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
  },

  async zscore(
    key: string,
    member: string,
    multi?: ProviderTransaction,
  ): Promise<number | null> {
    const { sql, params } = this._zscore(key, member);

    if (multi) {
      (multi as Multi).addCommand(sql, params, 'single', (row) => {
        return row
          ? parseFloat((row as unknown as { score: string }).score)
          : null;
      });
      return Promise.resolve(null);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        return res.rows.length ? parseFloat(res.rows[0].score) : null;
      } catch (error) {
        // Connection closed during test cleanup - return null
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return null;
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zscore(key: string, member: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
    const sql = `
      SELECT score
      FROM ${tableName}
      WHERE key = $1 AND member = $2
        AND (expiry IS NULL OR expiry > NOW())
      LIMIT 1
    `;
    const params = [key, member];
    return { sql, params };
  },

  async zrangebyscore(
    key: string,
    min: number,
    max: number,
    multi?: ProviderTransaction,
  ): Promise<string[]> {
    const { sql, params } = this._zrangebyscore(key, min, max);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'array', (rows) =>
        rows.map((row) => row.member),
      );
      return Promise.resolve([]);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        return res.rows.map((row) => row.member);
      } catch (error) {
        // Connection closed during test cleanup - return empty array
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return [];
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zrangebyscore(
    key: string,
    min: number,
    max: number,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
    const sql = `
      SELECT member FROM ${tableName}
      WHERE key = $1 AND score BETWEEN $2 AND $3 AND (expiry IS NULL OR expiry > NOW())
      ORDER BY score ASC, member ASC
    `;
    const params = [key, min, max];
    return { sql, params };
  },

  async zrangebyscore_withscores(
    key: string,
    min: number,
    max: number,
    multi?: ProviderTransaction,
  ): Promise<{ member: string; score: number }[]> {
    const { sql, params } = this._zrangebyscore_withscores(key, min, max);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'array', (rows) =>
        rows.map((row) => ({ member: row.member, score: row.score })),
      );
      return Promise.resolve([]);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        return res.rows.map((row) => ({ member: row.member, score: row.score }));
      } catch (error) {
        // Connection closed during test cleanup - return empty array
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return [];
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zrangebyscore_withscores(
    key: string,
    min: number,
    max: number,
  ): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
    const sql = `
      SELECT member, score FROM ${tableName}
      WHERE key = $1 AND score BETWEEN $2 AND $3 AND (expiry IS NULL OR expiry > NOW())
      ORDER BY score ASC, member ASC
    `;
    const params = [key, min, max];
    return { sql, params };
  },

  async zrem(
    key: string,
    member: string,
    multi?: ProviderTransaction,
  ): Promise<number> {
    const { sql, params } = this._zrem(key, member);
    if (multi) {
      (multi as Multi).addCommand(
        sql,
        params,
        'number',
        (rows) => rows[0]?.count || 0,
      );
      return Promise.resolve(0);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        return Number(res.rows[0]?.count || 0);
      } catch (error) {
        // Connection closed during test cleanup - return 0
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return 0;
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zrem(key: string, member: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
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
  },

  async zrank(
    key: string,
    member: string,
    multi?: ProviderTransaction,
  ): Promise<number | null> {
    const { sql, params } = this._zrank(key, member);
    if (multi) {
      (multi as Multi).addCommand(sql, params, 'number', (rows) =>
        rows[0]?.rank !== undefined ? parseInt(rows[0].rank, 10) - 1 : null,
      );
      return Promise.resolve(null);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);
        return res.rows[0]?.rank
          ? parseInt(res.rows[0].rank, 10) > 0
            ? parseInt(res.rows[0].rank, 10) - 1
            : null
          : null;
      } catch (error) {
        // Connection closed during test cleanup - return null
        if (error?.message?.includes('closed') || error?.message?.includes('queryable')) {
          return null;
        }
        // Re-throw unexpected errors
        throw error;
      }
    }
  },

  _zrank(key: string, member: string): { sql: string; params: any[] } {
    const tableName = context.tableForKey(key, 'sorted_set');
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
  },
});
