import { HMNS, KeyService } from '../../../../modules/key';
import { KeyStoreParams } from '../../../../types/hotmesh';
import { PostgresClientType } from '../../../../types/postgres';
import { ProviderTransaction } from '../../../../types/provider';

import { KVTransaction as Multi } from './kvtransaction';
import { stringModule } from './kvtypes/string';
import { hashModule } from './kvtypes/hash/index';
import { listModule } from './kvtypes/list';
import { zsetModule } from './kvtypes/zset';

/**
 * KVSQL is a class that provides a set of methods to interact with a Postgres database.
 * It is used to interact with the database in a key-value manner.
 */
export class KVSQL {
  pgClient: PostgresClientType;
  namespace: string;
  appId: string;
  string: ReturnType<typeof stringModule>;
  hash: ReturnType<typeof hashModule>;
  list: ReturnType<typeof listModule>;
  zset: ReturnType<typeof zsetModule>;

  constructor(pgClient: PostgresClientType, namespace: string, appId: string) {
    this.pgClient = pgClient;
    this.namespace = namespace;
    this.appId = appId;
    this.hash = hashModule(this);
    this.list = listModule(this);
    this.zset = zsetModule(this);
    this.string = stringModule(this);
  }

  isStatusOnly(fields: string[]): boolean {
    return fields.length === 1 && fields[0] === ':';
  }

  appendExpiryClause(baseQuery: string, tableAlias: string): string {
    return `
      ${baseQuery}
      AND (${tableAlias}.expiry IS NULL OR ${tableAlias}.expiry > NOW())
    `;
  }

  appendJobExpiryClause(baseQuery: string, tableAlias: string): string {
    return `
      ${baseQuery}
      AND (${tableAlias}.expired_at IS NULL OR ${tableAlias}.expired_at > NOW())
    `;
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
  tableForKey(
    key: string,
    stats_type?: 'hash' | 'sorted_set' | 'list',
  ): string {
    if (key === HMNS) {
      return 'public.hotmesh_connections';
    }

    const [_, appName, abbrev, ...rest] = key.split(':');
    if (appName === 'a') {
      return 'public.hotmesh_applications';
    }

    const id = rest?.length ? rest.join(':') : '';
    const entity = KeyService.resolveEntityType(abbrev, id);

    if (this.safeName(this.appId) !== this.safeName(appName)) {
      throw new Error(`App ID mismatch: ${this.appId} !== ${appName}`);
    }

    const schemaName = this.safeName(appName);

    if (entity === 'stats') {
      let tableName: string;
      if (stats_type === 'sorted_set') {
        tableName = 'stats_ordered';
      } else if (stats_type === 'list' || key.endsWith(':processed')) {
        tableName = 'stats_indexed';
      } else if (stats_type === 'hash' || /:\d$/.test(key)) {
        tableName = 'stats_counted';
      } else {
        throw new Error(`Unknown stats type [${stats_type}] for key [${key}]`);
      }
      return `${schemaName}.${tableName}`;
    }

    if (entity === 'unknown_entity') {
      throw new Error(`Unknown entity type abbreviation: ${abbrev}`);
    } else if (entity === 'applications') {
      return 'public.hotmesh_applications';
    } else {
      return `${schemaName}.${entity}`;
    }
  }

  safeName(input: string, prefix = ''): string {
    if (!input) {
      return 'connections';
    }
    let tableName = input.trim().toLowerCase();
    tableName = tableName.replace(/[^a-z0-9]+/g, '_');
    if (prefix) {
      tableName = `${prefix}_${tableName}`;
    }
    if (tableName.length > 63) {
      tableName = tableName.slice(0, 63);
    }
    tableName = tableName.replace(/_+$/g, '');
    if (!tableName) {
      tableName = 'connections';
    }
    return tableName;
  }

  // String Commands
  set = (...args: Parameters<typeof this.string.set>) =>
    this.string.set(...args);
  _set = (...args: Parameters<typeof this.string._set>) =>
    this.string._set(...args);
  get = (...args: Parameters<typeof this.string.get>) =>
    this.string.get(...args);
  _get = (...args: Parameters<typeof this.string._get>) =>
    this.string._get(...args);
  del = (...args: Parameters<typeof this.string.del>) =>
    this.string.del(...args);
  _del = (...args: Parameters<typeof this.string._del>) =>
    this.string._del(...args);
  setnx = (...args: Parameters<typeof this.string.setnx>) =>
    this.string.setnx(...args);
  setnxex = (...args: Parameters<typeof this.string.setnxex>) =>
    this.string.setnxex(...args);

  // Hash Commands
  hset = (...args: Parameters<typeof this.hash.hset>) =>
    this.hash.hset(...args);
  _hset = (...args: Parameters<typeof this.hash._hset>) =>
    this.hash._hset(...args);
  hsetnx = (...args: Parameters<typeof this.hash.hsetnx>) =>
    this.hash.hsetnx(...args);
  hget = (...args: Parameters<typeof this.hash.hget>) =>
    this.hash.hget(...args);
  _hget = (...args: Parameters<typeof this.hash._hget>) =>
    this.hash._hget(...args);
  hdel = (...args: Parameters<typeof this.hash.hdel>) =>
    this.hash.hdel(...args);
  _hdel = (...args: Parameters<typeof this.hash._hdel>) =>
    this.hash._hdel(...args);
  hmget = (...args: Parameters<typeof this.hash.hmget>) =>
    this.hash.hmget(...args);
  _hmget = (...args: Parameters<typeof this.hash._hmget>) =>
    this.hash._hmget(...args);
  hgetall = (...args: Parameters<typeof this.hash.hgetall>) =>
    this.hash.hgetall(...args);
  hincrbyfloat = (...args: Parameters<typeof this.hash.hincrbyfloat>) =>
    this.hash.hincrbyfloat(...args);
  collateLeg2Entry = (...args: Parameters<typeof this.hash.collateLeg2Entry>) =>
    this.hash.collateLeg2Entry(...args);
  _collateLeg2Entry = (...args: Parameters<typeof this.hash._collateLeg2Entry>) =>
    this.hash._collateLeg2Entry(...args);
  setStatusAndCollateGuid = (...args: Parameters<typeof this.hash.setStatusAndCollateGuid>) =>
    this.hash.setStatusAndCollateGuid(...args);
  _setStatusAndCollateGuid = (...args: Parameters<typeof this.hash._setStatusAndCollateGuid>) =>
    this.hash._setStatusAndCollateGuid(...args);
  _hincrbyfloat = (...args: Parameters<typeof this.hash._hincrbyfloat>) =>
    this.hash._hincrbyfloat(...args);
  hscan = (...args: Parameters<typeof this.hash.hscan>) =>
    this.hash.hscan(...args);
  _hscan = (...args: Parameters<typeof this.hash._hscan>) =>
    this.hash._hscan(...args);
  expire = (...args: Parameters<typeof this.hash.expire>) =>
    this.hash.expire(...args);
  _expire = (...args: Parameters<typeof this.hash._expire>) =>
    this.hash._expire(...args);
  scan = (...args: Parameters<typeof this.hash.scan>) =>
    this.hash.scan(...args);
  _scan = (...args: Parameters<typeof this.hash._scan>) =>
    this.hash._scan(...args);

  // List Commands
  lrange = (...args: Parameters<typeof this.list.lrange>) =>
    this.list.lrange(...args);
  _lrange = (...args: Parameters<typeof this.list._lrange>) =>
    this.list._lrange(...args);
  rpush = (...args: Parameters<typeof this.list.rpush>) =>
    this.list.rpush(...args);
  _rpush = (...args: Parameters<typeof this.list._rpush>) =>
    this.list._rpush(...args);
  lpush = (...args: Parameters<typeof this.list.lpush>) =>
    this.list.lpush(...args);
  _lpush = (...args: Parameters<typeof this.list._lpush>) =>
    this.list._lpush(...args);
  lpop = (...args: Parameters<typeof this.list.lpop>) =>
    this.list.lpop(...args);
  _lpop = (...args: Parameters<typeof this.list._lpop>) =>
    this.list._lpop(...args);
  lmove = (...args: Parameters<typeof this.list.lmove>) =>
    this.list.lmove(...args);
  _lmove = (...args: Parameters<typeof this.list._lmove>) =>
    this.list._lmove(...args);
  rename = (...args: Parameters<typeof this.list.rename>) =>
    this.list.rename(...args);
  _rename = (...args: Parameters<typeof this.list._rename>) =>
    this.list._rename(...args);

  // Sorted Set Commands
  zadd = (...args: Parameters<typeof this.zset.zadd>) =>
    this.zset.zadd(...args);
  _zadd = (...args: Parameters<typeof this.zset._zadd>) =>
    this.zset._zadd(...args);
  zrange = (...args: Parameters<typeof this.zset.zrange>) =>
    this.zset.zrange(...args);
  _zrange = (...args: Parameters<typeof this.zset._zrange>) =>
    this.zset._zrange(...args);
  zrangebyscore = (...args: Parameters<typeof this.zset.zrangebyscore>) =>
    this.zset.zrangebyscore(...args);
  _zrangebyscore = (...args: Parameters<typeof this.zset._zrangebyscore>) =>
    this.zset._zrangebyscore(...args);
  zrangebyscore_withscores = (
    ...args: Parameters<typeof this.zset.zrangebyscore_withscores>
  ) => this.zset.zrangebyscore_withscores(...args);
  _zrangebyscore_withscores = (
    ...args: Parameters<typeof this.zset._zrangebyscore_withscores>
  ) => this.zset._zrangebyscore_withscores(...args);
  zrem = (...args: Parameters<typeof this.zset.zrem>) =>
    this.zset.zrem(...args);
  _zrem = (...args: Parameters<typeof this.zset._zrem>) =>
    this.zset._zrem(...args);
  zrank = (...args: Parameters<typeof this.zset.zrank>) =>
    this.zset.zrank(...args);
  _zrank = (...args: Parameters<typeof this.zset._zrank>) =>
    this.zset._zrank(...args);
  zscore = (...args: Parameters<typeof this.zset.zscore>) =>
    this.zset.zscore(...args);
  _zscore = (...args: Parameters<typeof this.zset._zscore>) =>
    this.zset._zscore(...args);

  async exists(key: string): Promise<string | 0> {
    const { sql, params } = this._exists(key);
    const res = await this.pgClient.query(sql, params);
    return res.rows.length ? res.rows[0].table_name : 0;
  }

  _exists(key: string): { sql: string; params: any[] } {
    const tableName = this.tableForKey(key);
    const isJobsTable = tableName.endsWith('jobs');
    let sql: string;
    if (isJobsTable) {
      sql = `
        SELECT FROM ${tableName}
        WHERE id = $1
          AND (expired_at IS NULL OR expired_at > NOW())
        LIMIT 1;
      `;
    } else {
      sql = `
        SELECT FROM ${tableName}
        WHERE key = $1
          AND (expiry IS NULL OR expiry > NOW())
        LIMIT 1;
      `;
    }
    const params = [key];
    return { sql, params };
  }
}
