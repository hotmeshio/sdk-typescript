import { GetStateError } from '../../../../modules/errors';
import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
  VALSEP,
  TYPSEP,
} from '../../../../modules/key';
import { ILogger } from '../../../logger';
import {
  MDATA_SYMBOLS,
  SerializerService as Serializer,
} from '../../../serializer';
import { ActivityType, Consumes } from '../../../../types/activity';
import { AppVID } from '../../../../types/app';
import { HookRule, HookSignal } from '../../../../types/hook';
import {
  HotMeshApp,
  HotMeshApps,
  HotMeshSettings,
} from '../../../../types/hotmesh';
import {
  KVSQLProviderTransaction,
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';
import {
  SymbolSets,
  StringStringType,
  StringAnyType,
  Symbols,
} from '../../../../types/serializer';
import {
  IdsData,
  JobStats,
  JobStatsRange,
  StatsType,
} from '../../../../types/stats';
import { Transitions } from '../../../../types/transition';
import {
  formatISODate,
  getSymKey,
  sleepFor,
} from '../../../../modules/utils';
import { JobInterruptOptions } from '../../../../types/job';
import {
  HMSH_SCOUT_INTERVAL_SECONDS,
  HMSH_CODE_INTERRUPT,
  MAX_DELAY,
  HMSH_SIGNAL_EXPIRE,
} from '../../../../modules/enums';
import { WorkListTaskType } from '../../../../types/task';
import { ThrottleOptions } from '../../../../types/quorum';
import { Cache } from '../../cache';
import { StoreService } from '../..';
import { KVSQL } from './kvsql';
import { PostgresClientType } from '../../../../types';

class PostgresStoreService extends StoreService<ProviderClient, ProviderTransaction> {

  pgClient: PostgresClientType;

  transact(): ProviderTransaction {
    return this.storeClient.transact();
  }

  constructor(storeClient: ProviderClient) {
    super(storeClient);
    //Instead of directly referencing the 'pg' package and methods like 'query',
    //  the PostgresStore wraps the 'pg' client in a class that implements
    //  the Redis client interface. This allows the same methods to be called
    //  that were used when authoring the Redis client store provider.
    //In general, this.storeClient will behave like Redis, but will 
    //  use the 'pg' package and will read/write to a Postgres database.
    this.pgClient = storeClient as unknown as PostgresClientType;
    this.storeClient = new KVSQL(
      storeClient as unknown as PostgresClientType,
      this.namespace,
      this.appId,
    ) as unknown as ProviderClient;
  }

  async init(
    namespace = HMNS,
    appId: string,
    logger: ILogger,
  ): Promise<HotMeshApps> {
    //bind appId and namespace to storeClient once initialized
    // (it uses these values to construct keys for the store)
    this.storeClient.namespace = this.namespace = namespace;
    this.storeClient.appId = this.appId = appId;
    this.logger = logger;

    //ensure tables exist
    await this.deploy(appId);

    const settings = await this.getSettings(true);
    this.cache = new Cache(appId, settings);
    this.serializer = new Serializer();
    await this.getApp(appId);
    return this.cache.getApps();
  }

  /**
   * Deploys the necessary tables with the specified naming strategy.
   * @param appName - The name of the application.
   */
  async deploy(appName: string): Promise<void> {
    const client = this.pgClient;

    try {
      // Acquire advisory lock
      const lockId = this.getAdvisoryLockId(appName);
      const lockResult = await client.query(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [lockId],
      );

      if (lockResult.rows[0].locked) {
        // Begin transaction
        await client.query('BEGIN');

        // Check and create tables
        const tablesExist = await this.checkIfTablesExist(client, appName);
        if (!tablesExist) {
          await this.createTables(client, appName);
        }

        // Commit transaction
        await client.query('COMMIT');

        // Release the lock
        await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
      } else {
        // Wait for the deploy process to complete
        await this.waitForTablesCreation(client, lockId, appName);
      }
    } catch (error) {
      this.logger.error('Error deploying tables', { error });
      throw error;
    }
  }

  getAdvisoryLockId(appName: string): number {
    return this.hashStringToInt(appName);
  }

  hashStringToInt(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0; // Convert to 32-bit integer
    }
    return Math.abs(hash);
  }

  async waitForTablesCreation(
    client: PostgresClientType,
    lockId: number,
    appName: string,
  ): Promise<void> {
    let retries = 0;
    const maxRetries = 20;
    while (retries < maxRetries) {
      await sleepFor(150);
      const lockCheck = await client.query(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint) AS unlocked",
        [lockId],
      );
      if (lockCheck.rows[0].unlocked) {
        // Lock has been released, tables should exist now
        const tablesExist = await this.checkIfTablesExist(client, appName);
        if (tablesExist) {
          return;
        }
      }
      retries++;
    }
    throw new Error('Timeout waiting for table creation');
  }

  async checkIfTablesExist(
    client: PostgresClientType,
    appName: string,
  ): Promise<boolean> {
    const tableNames = this.getTableNames(appName);

    const checkTablePromises = tableNames.map((tableName) =>
      client.query(
        `SELECT to_regclass('public.${tableName}') AS table`,
      ),
    );

    const results = await Promise.all(checkTablePromises);
    return results.every((res) => res.rows[0].table !== null);
  }

  async createTables(
    client: PostgresClientType,
    appName: string,
  ): Promise<void> {
    // Begin transaction
    await client.query('BEGIN');

    const tableDefinitions = this.getTableDefinitions(
      this.storeClient.safeName(appName),
    );

    for (const tableDef of tableDefinitions) {
      switch (tableDef.type) {
        case 'string':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT PRIMARY KEY,
              value TEXT,
              expiry TIMESTAMP WITH TIME ZONE
            );
          `);
          break;

        case 'hash':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT NOT NULL,
              field TEXT NOT NULL,
              value TEXT,
              expiry TIMESTAMP WITH TIME ZONE,
              PRIMARY KEY (key, field)
            );
          `);
          break;

        case 'list':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT NOT NULL,
              index BIGINT NOT NULL,
              value TEXT,
              expiry TIMESTAMP WITH TIME ZONE,
              PRIMARY KEY (key, index)
            );
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expiry ON ${tableDef.name} (key, expiry);
          `);
          break;

        case 'sorted_set':
          await client.query(`
            CREATE TABLE IF NOT EXISTS ${tableDef.name} (
              key TEXT NOT NULL,
              member TEXT NOT NULL,
              score DOUBLE PRECISION NOT NULL,
              expiry TIMESTAMP WITH TIME ZONE,
              PRIMARY KEY (key, member)
            );
          `);
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_score_member
            ON ${tableDef.name} (key, score, member);
          `);
          break;

        default:
          this.logger.warn(`Unknown table type for ${tableDef.name}`);
          break;
      }
    }

    // Commit transaction
    await client.query('COMMIT');
  }

  getTableNames(appName: string): string[] {
    const tableNames = [];

    // Applications table (only hotmesh prefix)
    tableNames.push('hotmesh_applications', 'hotmesh_connections');

    // Other tables with appName
    const tablesWithAppName = [
      'throttles',
      'roles',
      'task_priorities',
      'task_schedules',
      'task_lists',
      'events',
      'jobs',
      'stats_counted',
      'stats_indexed',
      'stats_ordered',
      'versions',
      'signal_patterns',
      'signal_registry',
      'symbols',
    ];

    tablesWithAppName.forEach((table) => {
      tableNames.push(`hotmesh_${this.storeClient.safeName(appName)}_${table}`);
    });

    return tableNames;
  }

  getTableDefinitions(appName: string): Array<{ name: string; type: string }> {
    const tableDefinitions = [
      {
        name: 'hotmesh_applications',
        type: 'hash',
      },
      {
        name: `hotmesh_connections`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_throttles`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_roles`,
        type: 'string',
      },
      {
        name: `hotmesh_${appName}_task_schedules`,
        type: 'sorted_set',
      },
      {
        name: `hotmesh_${appName}_task_priorities`,
        type: 'sorted_set',
      },
      {
        name: `hotmesh_${appName}_task_lists`,
        type: 'list',
      },
      {
        name: `hotmesh_${appName}_events`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_jobs`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_stats_counted`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_stats_ordered`,
        type: 'sorted_set',
      },
      {
        name: `hotmesh_${appName}_stats_indexed`,
        type: 'list',
      },
      {
        name: `hotmesh_${appName}_versions`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_signal_patterns`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_symbols`,
        type: 'hash',
      },
      {
        name: `hotmesh_${appName}_signal_registry`,
        type: 'string',
      },
    ];

    return tableDefinitions;
  }

  isSuccessful(result: any): boolean {
    return result > 0 || result === 'OK' || result === true;
  }

  async delistSignalKey(key: string, target: string): Promise<void> {
    await this.kvsql().del(`${key}:${target}`);
  }

  async zAdd(
    key: string,
    score: number | string,
    value: string | number,
    transaction?: ProviderTransaction,
  ): Promise<any> {
    //default call signature uses 'ioredis' NPM Package format
    return await this.kvsql(transaction).zadd(
      key,
      Number(score),
      value.toString(),
    );
  }

  async zRangeByScore(
    key: string,
    score: number | string,
    value: string | number,
  ): Promise<string | null> {
    const result = await this.kvsql().zrangebyscore(
      key,
      Number(score),
      Number(value),
    );
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, params);
  }

  /**
   * strongly types the transaction or storeClient as KVSQL,
   * so methods are visible to the compiler/code editor
   */
  kvsql(transaction?: ProviderTransaction): KVSQL {
    return (transaction || this.storeClient) as unknown as KVSQL;
  }

  invalidateCache() {
    this.cache.invalidate();
  }

  /**
   * At any given time only a single engine will
   * check for and process work items in the
   * time and signal task queues.
   */
  async reserveScoutRole(
    scoutType: 'time' | 'signal' | 'activate',
    delay = HMSH_SCOUT_INTERVAL_SECONDS,
  ): Promise<boolean> {
    const key = this.mintKey(KeyType.WORK_ITEMS, {
      appId: this.appId,
      scoutType,
    });

    const success = await this.kvsql().set(
      key,
      `${scoutType}:${formatISODate(new Date())}`,
      { nx: true, ex: delay - 1 },
    );
    return this.isSuccessful(success);
  }

  async releaseScoutRole(
    scoutType: 'time' | 'signal' | 'activate',
  ): Promise<boolean> {
    const key = this.mintKey(KeyType.WORK_ITEMS, {
      appId: this.appId,
      scoutType,
    });
    const success = await this.kvsql().del(key);
    return this.isSuccessful(success);
  }

  async getSettings(bCreate = false): Promise<HotMeshSettings> {
    let settings = this.cache?.getSettings();
    if (settings) {
      return settings;
    } else {
      if (bCreate) {
        const packageJson = await import('../../../../package.json');
        const version: string = packageJson['version'] || '0.0.0';
        settings = { namespace: HMNS, version } as HotMeshSettings;
        await this.setSettings(settings);
        return settings;
      }
    }
    throw new Error('settings not found');
  }

  async setSettings(manifest: HotMeshSettings): Promise<any> {
    //HotMesh heartbeat. If a connection is made, the version will be set
    const params: KeyStoreParams = {};
    const key = this.mintKey(KeyType.HOTMESH, params);
    return await this.kvsql().hset(key, manifest);
  }

  async reserveSymbolRange(
    target: string,
    size: number,
    type: 'JOB' | 'ACTIVITY',
    tryCount = 1,
  ): Promise<[number, number, Symbols]> {
    const rangeKey = this.mintKey(KeyType.SYMKEYS, { appId: this.appId });
    const symbolKey = this.mintKey(KeyType.SYMKEYS, {
      activityId: target,
      appId: this.appId,
    });
    //reserve the slot in a `pending` state (range will be established in the next step)
    const response = await this.kvsql().hsetnx(
      rangeKey,
      target,
      '?:?',
    );



    if (response) {
      //if the key didn't exist, set the inclusive range and seed metadata fields
      const upperLimit = await this.kvsql().hincrbyfloat(
        rangeKey,
        ':cursor',
        size,
      );

      const lowerLimit = upperLimit - size;
      const inclusiveRange = `${lowerLimit}:${upperLimit - 1}`;
      await this.kvsql().hset(rangeKey, { [target]: inclusiveRange });
      const metadataSeeds = this.seedSymbols(target, type, lowerLimit);
      await this.kvsql().hset(symbolKey, metadataSeeds);
      return [lowerLimit + MDATA_SYMBOLS.SLOTS, upperLimit - 1, {} as Symbols];
    } else {
      //if the key already existed, get the lower limit and add the number of symbols
      const range = await this.kvsql().hget(
        rangeKey,
        target,
      );
      const [lowerLimitString] = range.split(':');
      if (lowerLimitString === '?') {
        await sleepFor(tryCount * 1000);
        if (tryCount < 5) {
          return this.reserveSymbolRange(target, size, type, tryCount + 1);
        } else {
          throw new Error(
            'Symbol range reservation failed due to deployment contention',
          );
        }
      } else {
        const lowerLimit = parseInt(lowerLimitString, 10);
        const symbols =
          await this.kvsql().hgetall(symbolKey);
        const symbolCount = Object.keys(symbols).length;
        const actualLowerLimit = lowerLimit + MDATA_SYMBOLS.SLOTS + symbolCount;
        const upperLimit = Number(lowerLimit + size - 1);
        return [actualLowerLimit, upperLimit, symbols as Symbols];
      }
    }
  }

  async getAllSymbols(): Promise<Symbols> {
    //get hash with all reserved symbol ranges
    const rangeKey = this.mintKey(KeyType.SYMKEYS, { appId: this.appId });
    const ranges = await this.kvsql().hgetall(rangeKey);
    const rangeKeys = Object.keys(ranges).sort();
    delete rangeKeys[':cursor'];

    //wrap the transact call in kvsql so datatypes are consistent
    const transaction = this.kvsql(this.transact());
    for (const rangeKey of rangeKeys) {
      const symbolKey = this.mintKey(KeyType.SYMKEYS, {
        activityId: rangeKey,
        appId: this.appId,
      });
      transaction.hgetall(symbolKey);
    }
    const results = (await transaction.exec()) as
      | Array<[null, Symbols]>
      | Array<Symbols>;

    const symbolSets: Symbols = {};
    results.forEach((result: [null, Symbols] | Symbols, index: number) => {
      if (result) {
        let vals: Symbols;
        if (Array.isArray(result) && result.length === 2) {
          vals = result[1];
        } else {
          vals = result as Symbols;
        }
        for (const [key, value] of Object.entries(vals)) {
          symbolSets[value as string] = key.startsWith(rangeKeys[index])
            ? key
            : `${rangeKeys[index]}/${key}`;
        }
      }
    });
    return symbolSets;
  }

  async getSymbols(activityId: string): Promise<Symbols> {
    let symbols: Symbols = this.cache.getSymbols(this.appId, activityId);
    if (symbols) {
      return symbols;
    } else {
      const params: KeyStoreParams = { activityId, appId: this.appId };
      const key = this.mintKey(KeyType.SYMKEYS, params);
      symbols = (await this.kvsql().hgetall(key)) as Symbols;
      this.cache.setSymbols(this.appId, activityId, symbols);
      return symbols;
    }
  }

  async addSymbols(activityId: string, symbols: Symbols): Promise<boolean> {
    if (!symbols || !Object.keys(symbols).length) return false;
    const params: KeyStoreParams = { activityId, appId: this.appId };
    const key = this.mintKey(KeyType.SYMKEYS, params);
    const success = await this.kvsql().hset(key, symbols);
    this.cache.deleteSymbols(this.appId, activityId);
    return success > 0;
  }

  seedSymbols(
    target: string,
    type: 'JOB' | 'ACTIVITY',
    startIndex: number,
  ): StringStringType {
    if (type === 'JOB') {
      return this.seedJobSymbols(startIndex);
    }
    return this.seedActivitySymbols(startIndex, target);
  }

  seedJobSymbols(startIndex: number): StringStringType {
    const hash: StringStringType = {};
    MDATA_SYMBOLS.JOB.KEYS.forEach((key) => {
      hash[`metadata/${key}`] = getSymKey(startIndex);
      startIndex++;
    });
    return hash;
  }

  seedActivitySymbols(
    startIndex: number,
    activityId: string,
  ): StringStringType {
    const hash: StringStringType = {};
    MDATA_SYMBOLS.ACTIVITY.KEYS.forEach((key) => {
      hash[`${activityId}/output/metadata/${key}`] = getSymKey(startIndex);
      startIndex++;
    });
    return hash;
  }

  async getSymbolValues(): Promise<Symbols> {
    let symvals: Symbols = this.cache.getSymbolValues(this.appId);
    if (symvals) {
      return symvals;
    } else {
      const key = this.mintKey(KeyType.SYMVALS, { appId: this.appId });
      symvals = await this.kvsql().hgetall(key);
      this.cache.setSymbolValues(this.appId, symvals as Symbols);
      return symvals;
    }
  }

  async addSymbolValues(symvals: Symbols): Promise<boolean> {
    if (!symvals || !Object.keys(symvals).length) return false;
    const key = this.mintKey(KeyType.SYMVALS, { appId: this.appId });
    const success = await this.kvsql().hset(key, symvals);
    this.cache.deleteSymbolValues(this.appId);
    return this.isSuccessful(success);
  }

  async getSymbolKeys(symbolNames: string[]): Promise<SymbolSets> {
    const symbolLookups = [];
    for (const symbolName of symbolNames) {
      symbolLookups.push(this.getSymbols(symbolName));
    }
    const symbolSets = await Promise.all(symbolLookups);
    const symKeys: SymbolSets = {};
    for (const symbolName of symbolNames) {
      symKeys[symbolName] = symbolSets.shift();
    }
    return symKeys;
  }

  async getApp(id: string, refresh = false): Promise<HotMeshApp> {
    let app: Partial<HotMeshApp> = this.cache.getApp(id);
    if (refresh || !(app && Object.keys(app).length > 0)) {
      const params: KeyStoreParams = { appId: id };
      const key = this.mintKey(KeyType.APP, params);
      const sApp = await this.kvsql().hgetall(key);
      if (!sApp) return null;
      app = {};
      for (const field in sApp) {
        try {
          if (field === 'active') {
            app[field] = sApp[field] === 'true';
          } else {
            app[field] = sApp[field];
          }
        } catch (e) {
          app[field] = sApp[field];
        }
      }
      this.cache.setApp(id, app as HotMeshApp);
    }
    return app as HotMeshApp;
  }

  async setApp(id: string, version: string): Promise<HotMeshApp> {
    const params: KeyStoreParams = { appId: id };
    const key = this.mintKey(KeyType.APP, params);
    const versionId = `versions/${version}`;
    const payload: HotMeshApp = {
      id,
      version,
      [versionId]: `deployed:${formatISODate(new Date())}`,
    };
    await this.kvsql().hset(key, payload as any);
    this.cache.setApp(id, payload);
    return payload;
  }

  async activateAppVersion(id: string, version: string): Promise<boolean> {
    const params: KeyStoreParams = { appId: id };
    const key = this.mintKey(KeyType.APP, params);
    const versionId = `versions/${version}`;
    const app = await this.getApp(id, true);
    if (app && app[versionId]) {
      const payload: HotMeshApp = {
        id,
        version: version.toString(),
        [versionId]: `activated:${formatISODate(new Date())}`,
        active: true,
      };
      Object.entries(payload).forEach(([key, value]) => {
        payload[key] = value.toString();
      });
      await this.kvsql().hset(key, payload as any);
      return true;
    }
    throw new Error(`Version ${version} does not exist for app ${id}`);
  }

  async registerAppVersion(appId: string, version: string): Promise<any> {
    const params: KeyStoreParams = { appId };
    const key = this.mintKey(KeyType.APP, params);
    const payload: HotMeshApp = {
      id: appId,
      version: version.toString(),
      [`versions/${version}`]: formatISODate(new Date()),
    };
    return await this.kvsql().hset(key, payload as any);
  }

  async setStats(
    jobKey: string,
    jobId: string,
    dateTime: string,
    stats: StatsType,
    appVersion: AppVID,
    transaction?: ProviderTransaction,
  ): Promise<any> {
    const params: KeyStoreParams = {
      appId: appVersion.id,
      jobId,
      jobKey,
      dateTime,
    };
    const localTransaction = transaction || this.transact();
    if (stats.general.length) {
      const generalStatsKey = this.mintKey(KeyType.JOB_STATS_GENERAL, params);
      for (const { target, value } of stats.general) {
        this.kvsql(localTransaction).hincrbyfloat(
          generalStatsKey,
          target,
          value as number,
        );
      }
    }
    for (const { target, value } of stats.index) {
      const indexParams = { ...params, facet: target };
      const indexStatsKey = this.mintKey(KeyType.JOB_STATS_INDEX, indexParams);
      this.kvsql(localTransaction).rpush(indexStatsKey, value.toString());
    }
    for (const { target, value } of stats.median) {
      const medianParams = { ...params, facet: target };
      const medianStatsKey = this.mintKey(
        KeyType.JOB_STATS_MEDIAN,
        medianParams,
      );
      await this.kvsql(localTransaction).zadd(
        medianStatsKey,
        Number(value),
        target,
      );
    }

    if (!transaction) {
      return await localTransaction.exec();
    }
  }

  hGetAllResult(result: any) {
    //default response signature uses 'redis' NPM Package format
    return result;
  }

  async getJobStats(jobKeys: string[]): Promise<JobStatsRange> {
    const transaction = this.kvsql(this.transact());
    for (const jobKey of jobKeys) {
      transaction.hgetall(jobKey);
    }
    const results = await transaction.exec();
    const output: { [key: string]: JobStats } = {};
    for (const [index, result] of results.entries()) {
      const key = jobKeys[index];
      const statsHash: unknown = result;
      if (statsHash && Object.keys(statsHash).length > 0) {
        const resolvedStatsHash: JobStats = { ...(statsHash as object) };
        for (const [key, val] of Object.entries(resolvedStatsHash)) {
          resolvedStatsHash[key] = Number(val);
        }
        output[key] = resolvedStatsHash;
      } else {
        output[key] = {} as JobStats;
      }
    }
    return output;
  }

  async getJobIds(
    indexKeys: string[],
    idRange: [number, number],
  ): Promise<IdsData> {
    const transaction = this.kvsql(this.transact());
    for (const idsKey of indexKeys) {
      transaction.lrange(idsKey, idRange[0], idRange[1]); //0,-1 returns all ids
    }
    const results = await transaction.exec();
    const output: IdsData = {};
    for (const [index, result] of results.entries()) {
      const key = indexKeys[index];

      //todo: resolve this discrepancy between redis/ioredis
      const idsList: string[] = result[1] || result;

      if (idsList && idsList.length > 0) {
        output[key] = idsList;
      } else {
        output[key] = [];
      }
    }
    return output;
  }

  async setStatus(
    collationKeyStatus: number,
    jobId: string,
    appId: string,
    transaction?: ProviderTransaction,
  ): Promise<any> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    return await this.kvsql(transaction).hincrbyfloat(
      jobKey,
      ':',
      collationKeyStatus,
    );
  }

  async getStatus(jobId: string, appId: string): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const status = await this.kvsql().hget(jobKey, ':');
    if (status === null) {
      throw new Error(`Job ${jobId} not found`);
    }
    return Number(status);
  }

  async setState(
    { ...state }: StringAnyType,
    status: number | null,
    jobId: string,
    symbolNames: string[],
    dIds: StringStringType,
    transaction?: ProviderTransaction,
  ): Promise<string> {
    delete state['metadata/js'];
    const hashKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    const symKeys = await this.getSymbolKeys(symbolNames);
    const symVals = await this.getSymbolValues();
    this.serializer.resetSymbols(symKeys, symVals, dIds);

    const hashData = this.serializer.package(state, symbolNames);
    if (status !== null) {
      hashData[':'] = status.toString();
    } else {
      delete hashData[':'];
    }
    await this.kvsql(transaction).hset(
      hashKey,
      hashData,
    );
    return jobId;
  }

  /**
   * Returns custom search fields and values.
   * NOTE: The `fields` param should NOT prefix items with an underscore.
   * NOTE: Literals are allowed if quoted.
   */
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    const key = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const _fields = fields.map((field) => {
      if (field.startsWith('"')) {
        return field.slice(1, -1);
      }
      return `_${field}`;
    });
    const jobDataArray = await this.kvsql().hmget(
      key,
      _fields,
    );
    const jobData: StringAnyType = {};
    fields.forEach((field, index) => {
      if (field.startsWith('"')) {
        field = field.slice(1, -1);
      }
      jobData[field] = jobDataArray[index];
    });
    return jobData;
  }

  async getState(
    jobId: string,
    consumes: Consumes,
    dIds: StringStringType,
  ): Promise<[StringAnyType, number] | undefined> {
    //get abbreviated field list (the symbols for the paths)
    const key = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const symbolNames = Object.keys(consumes);
    const symKeys = await this.getSymbolKeys(symbolNames);
    this.serializer.resetSymbols(symKeys, {}, dIds);
    const fields = this.serializer.abbreviate(consumes, symbolNames, [':']);

    const jobDataArray = await this.kvsql().hmget(
      key,
      fields,
    );
    const jobData: StringAnyType = {};
    let atLeast1 = false; //if status field (':') isn't present assume 404
    fields.forEach((field, index) => {
      if (jobDataArray[index]) {
        atLeast1 = true;
      }
      jobData[field] = jobDataArray[index];
    });
    if (atLeast1) {
      const symVals = await this.getSymbolValues();
      this.serializer.resetSymbols(symKeys, symVals, dIds);
      const state = this.serializer.unpackage(jobData, symbolNames);
      let status = 0;
      if (state[':']) {
        status = Number(state[':']);
        state[`metadata/js`] = status;
        delete state[':'];
      }
      return [state, status];
    } else {
      throw new GetStateError(jobId);
    }
  }

  async getRaw(jobId: string): Promise<StringStringType> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    const job = await this.kvsql().hgetall(jobKey);
    if (!job) {
      throw new GetStateError(jobId);
    }
    return job;
  }

  /**
   * collate is a generic method for incrementing a value in a hash
   * in order to track their progress during processing.
   */
  async collate(
    jobId: string,
    activityId: string,
    amount: number,
    dIds: StringStringType,
    transaction?: ProviderTransaction,
  ): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    const collationKey = `${activityId}/output/metadata/as`; //activity state
    const symbolNames = [activityId];
    const symKeys = await this.getSymbolKeys(symbolNames);
    const symVals = await this.getSymbolValues();
    this.serializer.resetSymbols(symKeys, symVals, dIds);

    const payload = { [collationKey]: amount.toString() };
    const hashData = this.serializer.package(payload, symbolNames);
    const targetId = Object.keys(hashData)[0];
    return await this.kvsql(transaction).hincrbyfloat(
      jobKey,
      targetId,
      amount,
    );
  }

  /**
   * Synthentic collation affects those activities in the graph
   * that represent the synthetic DAG that was materialized during compilation;
   * Synthetic collation distinguishes `re-entry due to failure` from
   * `purposeful re-entry`.
   */
  async collateSynthetic(
    jobId: string,
    guid: string,
    amount: number,
    transaction?: ProviderTransaction,
  ): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    return await this.kvsql(transaction).hincrbyfloat(
      jobKey,
      guid,
      amount,
    );
  }

  async setStateNX(
    jobId: string,
    appId: string,
    status?: number,
  ): Promise<boolean> {
    const hashKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const result = await this.kvsql().hsetnx(
      hashKey,
      ':',
      status?.toString() ?? '1',
    );
    return this.isSuccessful(result);
  }

  async getSchema(
    activityId: string,
    appVersion: AppVID,
  ): Promise<ActivityType> {
    const schema = this.cache.getSchema(
      appVersion.id,
      appVersion.version,
      activityId,
    );
    if (schema) {
      return schema;
    } else {
      const schemas = await this.getSchemas(appVersion);
      return schemas[activityId];
    }
  }

  async getSchemas(appVersion: AppVID): Promise<Record<string, ActivityType>> {
    let schemas = this.cache.getSchemas(appVersion.id, appVersion.version);
    if (schemas && Object.keys(schemas).length > 0) {
      return schemas;
    } else {
      const params: KeyStoreParams = {
        appId: appVersion.id,
        appVersion: appVersion.version,
      };
      const key = this.mintKey(KeyType.SCHEMAS, params);
      schemas = {};
      const hash = await this.kvsql().hgetall(key);
      Object.entries(hash).forEach(([key, value]) => {
        schemas[key] = JSON.parse(value as string);
      });
      this.cache.setSchemas(appVersion.id, appVersion.version, schemas);
      return schemas;
    }
  }

  async setSchemas(
    schemas: Record<string, ActivityType>,
    appVersion: AppVID,
  ): Promise<any> {
    const params: KeyStoreParams = {
      appId: appVersion.id,
      appVersion: appVersion.version,
    };
    const key = this.mintKey(KeyType.SCHEMAS, params);
    const _schemas = { ...schemas } as Record<string, string>;
    Object.entries(_schemas).forEach(([key, value]) => {
      _schemas[key] = JSON.stringify(value);
    });
    const response = await this.kvsql().hset(key, _schemas);
    this.cache.setSchemas(appVersion.id, appVersion.version, schemas);
    return response;
  }

  async setSubscriptions(
    subscriptions: Record<string, any>,
    appVersion: AppVID,
  ): Promise<boolean> {
    const params: KeyStoreParams = {
      appId: appVersion.id,
      appVersion: appVersion.version,
    };
    const key = this.mintKey(KeyType.SUBSCRIPTIONS, params);
    const _subscriptions = { ...subscriptions };
    Object.entries(_subscriptions).forEach(([key, value]) => {
      _subscriptions[key] = JSON.stringify(value);
    });
    const status = await this.kvsql().hset(
      key,
      _subscriptions,
    );
    this.cache.setSubscriptions(
      appVersion.id,
      appVersion.version,
      subscriptions,
    );
    return this.isSuccessful(status);
  }

  async getSubscriptions(appVersion: AppVID): Promise<Record<string, string>> {
    let subscriptions = this.cache.getSubscriptions(
      appVersion.id,
      appVersion.version,
    );
    if (subscriptions && Object.keys(subscriptions).length > 0) {
      return subscriptions;
    } else {
      const params: KeyStoreParams = {
        appId: appVersion.id,
        appVersion: appVersion.version,
      };
      const key = this.mintKey(KeyType.SUBSCRIPTIONS, params);
      subscriptions =
        await this.kvsql().hgetall(key) || {};
      Object.entries(subscriptions).forEach(([key, value]) => {
        subscriptions[key] = JSON.parse(value as string);
      });
      this.cache.setSubscriptions(
        appVersion.id,
        appVersion.version,
        subscriptions,
      );
      return subscriptions;
    }
  }

  async getSubscription(
    topic: string,
    appVersion: AppVID,
  ): Promise<string | undefined> {
    const subscriptions = await this.getSubscriptions(appVersion);
    return subscriptions[topic];
  }

  async setTransitions(
    transitions: Record<string, any>,
    appVersion: AppVID,
  ): Promise<any> {
    const params: KeyStoreParams = {
      appId: appVersion.id,
      appVersion: appVersion.version,
    };
    const key = this.mintKey(KeyType.SUBSCRIPTION_PATTERNS, params);
    const _subscriptions = { ...transitions };
    Object.entries(_subscriptions).forEach(([key, value]) => {
      _subscriptions[key] = JSON.stringify(value);
    });
    if (Object.keys(_subscriptions).length !== 0) {
      const response = await this.kvsql().hset(
        key,
        _subscriptions,
      );
      this.cache.setTransitions(appVersion.id, appVersion.version, transitions);
      return response;
    }
  }

  async getTransitions(appVersion: AppVID): Promise<Transitions> {
    let transitions = this.cache.getTransitions(
      appVersion.id,
      appVersion.version,
    );
    if (transitions && Object.keys(transitions).length > 0) {
      return transitions;
    } else {
      const params: KeyStoreParams = {
        appId: appVersion.id,
        appVersion: appVersion.version,
      };
      const key = this.mintKey(KeyType.SUBSCRIPTION_PATTERNS, params);
      transitions = {};
      const hash = await this.kvsql().hgetall(key);
      Object.entries(hash).forEach(([key, value]) => {
        transitions[key] = JSON.parse(value as string);
      });
      this.cache.setTransitions(appVersion.id, appVersion.version, transitions);
      return transitions;
    }
  }

  async setHookRules(hookRules: Record<string, HookRule[]>): Promise<any> {
    const key = this.mintKey(KeyType.HOOKS, { appId: this.appId });
    const _hooks = {};
    Object.entries(hookRules).forEach(([key, value]) => {
      _hooks[key.toString()] = JSON.stringify(value);
    });
    if (Object.keys(_hooks).length !== 0) {
      const response = await this.kvsql().hset(key, _hooks);
      this.cache.setHookRules(this.appId, hookRules);
      return response;
    }
  }

  async getHookRules(): Promise<Record<string, HookRule[]>> {
    let patterns = this.cache.getHookRules(this.appId);
    if (patterns && Object.keys(patterns).length > 0) {
      return patterns;
    } else {
      const key = this.mintKey(KeyType.HOOKS, { appId: this.appId });
      const _hooks = await this.kvsql().hgetall(key);
      patterns = {};
      Object.entries(_hooks).forEach(([key, value]) => {
        patterns[key] = JSON.parse(value as string);
      });
      this.cache.setHookRules(this.appId, patterns);
      return patterns;
    }
  }

  async setHookSignal(
    hook: HookSignal,
    transaction?: ProviderTransaction,
  ): Promise<any> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const { topic, resolved, jobId } = hook;
    const signalKey = `${topic}:${resolved}`;
    await this.kvsql(transaction).setnxex(
      `${key}:${signalKey}`,
      jobId,
      Math.max(hook.expire, HMSH_SIGNAL_EXPIRE),
    );
  }

  async getHookSignal(
    topic: string,
    resolved: string,
  ): Promise<string | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const response = await this.kvsql().get(
      `${key}:${topic}:${resolved}`,
    );
    return response ? response.toString() : undefined;
  }

  async deleteHookSignal(
    topic: string,
    resolved: string,
  ): Promise<number | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const response = await this.kvsql().del(
      `${key}:${topic}:${resolved}`,
    );
    return response ? Number(response) : undefined;
  }

  async addTaskQueues(keys: string[]): Promise<void> {
    const transaction = this.kvsql(this.transact());
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    for (const key of keys) {
      transaction.zadd(
        zsetKey,
        Date.now(),
        key,
        { nx: true },
      );
    }
    await transaction.exec();
  }

  async getActiveTaskQueue(): Promise<string | null> {
    let workItemKey = this.cache.getActiveTaskQueue(this.appId) || null;
    if (!workItemKey) {
      const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
      const result = await this.kvsql().zrange(
        zsetKey,
        0,
        0,
      );
      workItemKey = result.length > 0 ? result[0] : null;
      if (workItemKey) {
        this.cache.setWorkItem(this.appId, workItemKey);
      }
    }
    return workItemKey;
  }

  async deleteProcessedTaskQueue(
    workItemKey: string,
    key: string,
    processedKey: string,
    scrub = false,
  ): Promise<void> {
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    const didRemove = await this.kvsql().zrem(
      zsetKey,
      workItemKey,
    );
    if (didRemove) {
      if (scrub) {
        //indexes can be designed to be self-cleaning; `engine.hookAll` exposes this option
        this.kvsql().expire(processedKey, 0);
        this.kvsql().expire(
          key.split(':').slice(0, 5).join(':'),
          0,
        );
      } else {
        await this.kvsql().rename(processedKey, key);
      }
    }
    this.cache.removeWorkItem(this.appId);
  }

  async processTaskQueue(
    sourceKey: string,
    destinationKey: string,
  ): Promise<any> {
    return await this.kvsql().lmove(
      sourceKey,
      destinationKey,
      'LEFT',
      'RIGHT',
    );
  }

  async expireJob(
    jobId: string,
    inSeconds: number,
    transaction?: ProviderTransaction,
  ): Promise<void> {
    if (!isNaN(inSeconds) && inSeconds > 0) {
      const jobKey = this.mintKey(KeyType.JOB_STATE, {
        appId: this.appId,
        jobId,
      });
      await this.kvsql(transaction).expire(
        jobKey,
        inSeconds,
      );
    }
  }

  async getDependencies(jobId: string): Promise<string[]> {
    const depParams = { appId: this.appId, jobId };
    const depKey = this.mintKey(KeyType.JOB_DEPENDENTS, depParams);
    return this.kvsql().lrange(depKey, 0, -1);
  }

  /**
   * registers a hook activity to be awakened (uses ZSET to
   * store the 'sleep group' and LIST to store the events
   * for the given sleep group. Sleep groups are
   * organized into 'n'-second blocks (LISTS))
   */
  async registerTimeHook(
    jobId: string,
    gId: string,
    activityId: string,
    type: WorkListTaskType,
    deletionTime: number,
    dad: string,
    transaction?: ProviderTransaction,
  ): Promise<void> {
    const listKey = this.mintKey(KeyType.TIME_RANGE, {
      appId: this.appId,
      timeValue: deletionTime,
    });
    //register the task in the LIST
    const timeEvent = [type, activityId, gId, dad, jobId].join(VALSEP);
    const len = await this.kvsql(transaction).rpush(
      listKey,
      timeEvent,
    );
    //register the LIST in the ZSET
    if (transaction || len === 1) {
      const zsetKey = this.mintKey(KeyType.TIME_RANGE, { appId: this.appId });
      await this.zAdd(zsetKey, deletionTime.toString(), listKey, transaction);
    }
  }

  async getNextTask(
    listKey?: string,
  ): Promise<
    | [
        listKey: string,
        jobId: string,
        gId: string,
        activityId: string,
        type: WorkListTaskType,
      ]
    | boolean
  > {
    const zsetKey = this.mintKey(KeyType.TIME_RANGE, { appId: this.appId });
    listKey = listKey || await this.zRangeByScore(zsetKey, 0, Date.now());
    if (listKey) {
      let [pType, pKey] = this.resolveTaskKeyContext(listKey);
      const timeEvent = await this.kvsql().lpop(pKey);
      if (timeEvent) {
        //deconstruct composite key
        let [type, activityId, gId, _pd, ...jobId] = timeEvent.split(VALSEP);
        const jid = jobId.join(VALSEP);

        if (type === 'delist') {
          pType = 'delist';
        } else if (type === 'child') {
          pType = 'child';
        } else if (type === 'expire-child') {
          type = 'expire';
        }
        return [listKey, jid, gId, activityId, pType];
      }
      await this.kvsql().zrem(zsetKey, listKey);
      return true;
    }
    return false;
  }

  /**
   * when processing time jobs, the target LIST ID returned
   * from the ZSET query can be prefixed to denote what to
   * do with the work list. (not everything is known in advance,
   * so the ZSET key defines HOW to approach the work in the
   * generic LIST (lists typically contain target job ids)
   * @param {string} listKey - composite key
   */
  resolveTaskKeyContext(listKey: string): [WorkListTaskType, string] {
    if (listKey.startsWith(`${TYPSEP}INTERRUPT`)) {
      return ['interrupt', listKey.split(TYPSEP)[2]];
    } else if (listKey.startsWith(`${TYPSEP}EXPIRE`)) {
      return ['expire', listKey.split(TYPSEP)[2]];
    } else {
      return ['sleep', listKey];
    }
  }

  /**
   * Interrupts a job and sets sets a job error (410), if 'throw'!=false.
   * This method is called by the engine and not by an activity and is
   * followed by a call to execute job completion/cleanup tasks
   * associated with a job completion event.
   *
   * Todo: move most of this logic to the engine (too much logic for the store)
   */
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<void> {
    try {
      //verify job exists
      const status = await this.getStatus(jobId, this.appId);
      if (status <= 0) {
        //verify still active; job already completed
        throw new Error(`Job ${jobId} already completed`);
      }
      //decrement job status (:) by 1bil
      const amount = -1_000_000_000;
      const jobKey = this.mintKey(KeyType.JOB_STATE, {
        appId: this.appId,
        jobId,
      });
      const result = await this.kvsql().hincrbyfloat(
        jobKey,
        ':',
        amount,
      );
      if (result <= amount) {
        //verify active state; job already interrupted
        throw new Error(`Job ${jobId} already completed`);
      }
      //persist the error unless specifically told not to
      if (options.throw !== false) {
        const errKey = `metadata/err`; //job errors are stored at the path `metadata/err`
        const symbolNames = [`$${topic}`]; //the symbol for `metadata/err` is in redis and stored using the job topic
        const symKeys = await this.getSymbolKeys(symbolNames);
        const symVals = await this.getSymbolValues();
        this.serializer.resetSymbols(symKeys, symVals, {});

        //persists the standard 410 error (job is `gone`)
        const err = JSON.stringify({
          code: options.code ?? HMSH_CODE_INTERRUPT,
          message: options.reason ?? `job [${jobId}] interrupted`,
          stack: options.stack ?? '',
          job_id: jobId,
        });

        const payload = { [errKey]: amount.toString() };
        const hashData = this.serializer.package(payload, symbolNames);
        const errSymbol = Object.keys(hashData)[0];
        await this.kvsql().hset(jobKey, { [errSymbol]: err });
      }
    } catch (e) {
      if (!options.suppress) {
        throw e;
      } else {
        this.logger.debug('suppressed-interrupt', { message: e.message });
      }
    }
  }

  async scrub(jobId: string) {
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    await this.kvsql().del(jobKey);
  }

  async findJobs(
    queryString = '*',
    limit = 1000,
    //NOTE: unused in SQL provider; leave to keep signature consistent
    batchSize = 1000,
    cursor = '0',
  ): Promise<[string, string[]]> {

    const matchKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId: queryString,
    });

    const { cursor: _cursor, keys } = await this.kvsql().scan(
      Number(cursor),
      limit,
      matchKey,
    );
    return [_cursor.toPrecision(), keys];
  }

  async findJobFields(
    jobId: string,
    fieldMatchPattern = '*',
    limit = 1000,
    batchSize = 1000,
    cursor = '0',
  ): Promise<[string, StringStringType]> {
    const matchingFields: Record<string, string> = {};
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    let len = 0;
    do {
      const { cursor: newCursor, items } = await this.kvsql().hscan(
        jobKey,
        cursor,
        batchSize,
        fieldMatchPattern === '*' ? undefined : fieldMatchPattern
      );
      cursor = newCursor;
  
      for (const field in items) {
        len++;
        matchingFields[field] = items[field];
      }
      if (cursor === '0' || len >= limit) {
        break;
      }
    } while (true);
    return [cursor, matchingFields];
  }

  async setThrottleRate(options: ThrottleOptions): Promise<void> {
    const key = this.mintKey(KeyType.THROTTLE_RATE, { appId: this.appId });
    //engine guids are session specific. no need to persist
    if (options.guid) {
      return;
    }
    //if a topic, update one
    const rate = options.throttle.toString();
    if (options.topic) {
      await this.kvsql().hset(key, {
        [options.topic]: rate,
      });
    } else {
      //if no topic, update all
      const transaction = this.transact() as unknown as KVSQLProviderTransaction;
      transaction.del(key);
      transaction.hset(key, { ':': rate });
      await transaction.exec();
    }
  }

  async getThrottleRates(): Promise<StringStringType> {
    const key = this.mintKey(KeyType.THROTTLE_RATE, { appId: this.appId });
    const response = await this.kvsql().hgetall(key);
    return response ?? {};
  }

  async getThrottleRate(topic: string): Promise<number> {
    //always return a valid number range
    const resolveRate = (response: StringStringType, topic: string) => {
      const rate = topic in response ? Number(response[topic]) : 0;
      if (isNaN(rate)) return 0;
      if (rate == -1) return MAX_DELAY;
      return Math.max(Math.min(rate, MAX_DELAY), 0);
    };

    const response = await this.getThrottleRates();
    const globalRate = resolveRate(response, ':');
    if (topic === ':' || !(topic in response)) {
      //use global rate unless worker specifies rate
      return globalRate;
    }
    return resolveRate(response, topic);
  }
}

export { PostgresStoreService };
