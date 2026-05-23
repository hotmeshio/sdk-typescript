import { GetStateError } from '../../../../modules/errors';
import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../../modules/key';
import { guid } from '../../../../modules/utils';
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
  ScoutType,
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
import { formatISODate, getSymKey, sleepFor } from '../../../../modules/utils';
import { splitField } from './kvtypes/hash/utils';
import { JobInterruptOptions } from '../../../../types/job';
import {
  HMSH_SCOUT_INTERVAL_SECONDS,
  HMSH_CODE_INTERRUPT,
  MAX_DELAY,
  HMSH_SIGNAL_EXPIRE,
  HMSH_PENDING_SIGNAL_EXPIRE,
  HMSH_FIDELITY_SECONDS,
} from '../../../../modules/enums';
import { WorkListTaskType } from '../../../../types/task';
import { ThrottleOptions } from '../../../../types/quorum';
import { Cache } from '../../cache';
import { StoreService } from '../..';
import { PostgresClientType } from '../../../../types';

import { KVSQL } from './kvsql';
import { KVTables } from './kvtables';
import { getTimeNotifySql } from './time-notify';

class PostgresStoreService extends StoreService<
  ProviderClient,
  ProviderTransaction
> {
  pgClient: PostgresClientType;
  kvTables: ReturnType<typeof KVTables>;
  isScout = false;

  transact(): ProviderTransaction {
    return this.storeClient.transact();
  }

  constructor(storeClient: ProviderClient) {
    super(storeClient);
    //Instead of directly referencing the 'pg' package and methods like 'query',
    //  the PostgresStore wraps the 'pg' client in a class that implements
    //  an entity/attribute interface.
    this.pgClient = storeClient as unknown as PostgresClientType;
    this.storeClient = new KVSQL(
      storeClient as unknown as PostgresClientType,
      this.namespace,
      this.appId,
    ) as unknown as ProviderClient;
    //kvTables will provision tables and indexes in the Postgres db as necessary
    this.kvTables = KVTables(this);
  }

  async init(
    namespace = HMNS,
    appId: string,
    logger: ILogger,
    guid?: string,
    role?: string,
  ): Promise<HotMeshApps> {
    //bind appId and namespace to storeClient once initialized
    // (it uses these values to construct keys for the store)
    this.storeClient.namespace = this.namespace = namespace;
    this.storeClient.appId = this.appId = appId;
    this.logger = logger;

    //confirm db tables exist
    await this.kvTables.deploy(appId);

    // Deploy time notification triggers
    await this.deployTimeNotificationTriggers(appId);

    //note: getSettings will contact db to confirm r/w access
    const settings = await this.getSettings(true, guid, role);
    this.cache = new Cache(appId, settings);
    this.serializer = new Serializer();
    await this.getApp(appId);
    return this.cache.getApps();
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
    scoutType: ScoutType,
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
    scoutType: ScoutType,
  ): Promise<boolean> {
    const key = this.mintKey(KeyType.WORK_ITEMS, {
      appId: this.appId,
      scoutType,
    });
    const success = await this.kvsql().del(key);
    return this.isSuccessful(success);
  }

  async getSettings(
    bCreate = false,
    guid?: string,
    role?: string,
  ): Promise<HotMeshSettings> {
    let settings = this.cache?.getSettings();
    if (settings) {
      return settings;
    } else {
      if (bCreate) {
        const packageJson = await import('../../../../package.json');
        const version: string = packageJson['version'] || '0.0.0';
        settings = { namespace: HMNS, version } as HotMeshSettings;
        if (guid && role) {
          await this.registerConnection(guid, role, version);
        }
        return settings;
      }
    }
    throw new Error('settings not found');
  }

  async setSettings(manifest: HotMeshSettings): Promise<any> {
    // No-op for Postgres — settings are derived from package.json
    // and connections are registered via registerConnection()
    return;
  }

  async registerConnection(
    guid: string,
    role: string,
    version: string,
  ): Promise<void> {
    const sql = `INSERT INTO public.hmsh_connections (guid, app_id, role, version)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (guid, app_id) DO UPDATE SET
        version = EXCLUDED.version, connected_at = NOW()`;
    await this.pgClient.query(sql, [guid, this.appId, role, version]);
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
    const response = await this.kvsql().hsetnx(rangeKey, target, '?:?');

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
      const range = await this.kvsql().hget(rangeKey, target);
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
        const symbols = await this.kvsql().hgetall(symbolKey);
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
    let app: Partial<HotMeshApp> = this.cache?.getApp(id);
    if (refresh || !(app && Object.keys(app).length > 0)) {
      // Fetch from relational tables
      const appResult = await this.pgClient.query(
        `SELECT app_id, version, active, settings FROM public.hmsh_applications WHERE app_id = $1`,
        [id],
      );
      if (!appResult.rows.length) return null;
      const row = appResult.rows[0];
      app = {
        id: row.app_id,
        version: row.version,
        active: row.active,
      };
      // Fetch version history
      const versionsResult = await this.pgClient.query(
        `SELECT version, status, deployed_at FROM public.hmsh_application_versions WHERE app_id = $1`,
        [id],
      );
      for (const vRow of versionsResult.rows) {
        app[`versions/${vRow.version}`] = `${vRow.status}:${formatISODate(new Date(vRow.deployed_at))}`;
      }
      this.cache?.setApp(id, app as HotMeshApp);
    }
    return app as HotMeshApp;
  }

  async setApp(id: string, version: string): Promise<HotMeshApp> {
    const now = new Date();
    // Upsert into applications
    await this.pgClient.query(
      `INSERT INTO public.hmsh_applications (app_id, version, active, updated_at)
       VALUES ($1, $2, TRUE, $3)
       ON CONFLICT (app_id) DO UPDATE SET version = $2, updated_at = $3`,
      [id, version, now],
    );
    // Insert version record
    await this.pgClient.query(
      `INSERT INTO public.hmsh_application_versions (app_id, version, status, deployed_at)
       VALUES ($1, $2, 'deployed', $3)
       ON CONFLICT (app_id, version) DO UPDATE SET status = 'deployed', deployed_at = $3`,
      [id, version, now],
    );
    const payload: HotMeshApp = {
      id,
      version,
      [`versions/${version}`]: `deployed:${formatISODate(now)}`,
    };
    this.cache?.setApp(id, payload);
    return payload;
  }

  async activateAppVersion(id: string, version: string): Promise<boolean> {
    const app = await this.getApp(id, true);
    const versionId = `versions/${version}`;
    if (app && app[versionId]) {
      const now = new Date();
      await this.pgClient.query(
        `UPDATE public.hmsh_applications SET active = TRUE, version = $2, updated_at = $3 WHERE app_id = $1`,
        [id, version, now],
      );
      await this.pgClient.query(
        `UPDATE public.hmsh_application_versions SET status = 'activated', deployed_at = $3 WHERE app_id = $1 AND version = $2`,
        [id, version, now],
      );
      return true;
    }
    throw new Error(`Version ${version} does not exist for app ${id}`);
  }

  async registerAppVersion(appId: string, version: string): Promise<any> {
    const now = new Date();
    await this.pgClient.query(
      `INSERT INTO public.hmsh_applications (app_id, version, active, updated_at)
       VALUES ($1, $2, TRUE, $3)
       ON CONFLICT (app_id) DO UPDATE SET version = $2, updated_at = $3`,
      [appId, version, now],
    );
    await this.pgClient.query(
      `INSERT INTO public.hmsh_application_versions (app_id, version, status, deployed_at)
       VALUES ($1, $2, 'deployed', $3)
       ON CONFLICT (app_id, version) DO UPDATE SET status = 'deployed', deployed_at = $3`,
      [appId, version, now],
    );
    return 1;
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
    //default response signature
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

  /**
   * 1) HIGH-LEVEL STORE METHOD (engine/activity-facing)
   * ---------------------------------------------------
   * Mirrors setStatus(), but performs the compound Step-2 requirement:
   * - apply delta to job semaphore (jobs.status)
   * - compute thresholdHit (0/1) for desired threshold
   * - persist thresholdHit onto the Leg2 GUID ledger by incrementing the 100B digit (or other weight)
   * - return thresholdHit (0/1)
   */
  async setStatusAndCollateGuid(
    statusDelta: number,          // typically (N - 1)
    threshold: number,            // typically 0 (but supports 0,1,12,...)
    jobId: string,
    appId: string,
    guidField: string,            // the jobs_attributes.field for the Leg2 GUID ledger row
    guidWeight: number,           // e.g. 100_000_000_000 for GUID 100B digit
    transaction?: ProviderTransaction,
  ): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    return await this.kvsql(transaction).setStatusAndCollateGuid(
      jobKey,
      statusDelta,
      threshold,
      guidField,
      guidWeight,
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
    await this.kvsql(transaction).hset(hashKey, hashData);
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
    const jobDataArray = await this.kvsql().hmget(key, _fields);
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
    const jobDataArray = await this.kvsql().hmget(key, fields);
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
    return await this.kvsql(transaction).hincrbyfloat(jobKey, targetId, amount);
  }

  /**
   * Compound Leg2 entry: atomically increments the activity Leg2 entry
   * counter and seeds the GUID ledger with the ordinal IF NOT EXISTS.
   * Returns [activityValue, guidValue].
   */
  async collateLeg2Entry(
    jobId: string,
    activityId: string,
    guid: string,
    dIds: StringStringType,
    transaction?: ProviderTransaction,
  ): Promise<[number, number]> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    const collationKey = `${activityId}/output/metadata/as`;
    const symbolNames = [activityId];
    const symKeys = await this.getSymbolKeys(symbolNames);
    const symVals = await this.getSymbolValues();
    this.serializer.resetSymbols(symKeys, symVals, dIds);

    const payload = { [collationKey]: '1' };
    const hashData = this.serializer.package(payload, symbolNames);
    const targetId = Object.keys(hashData)[0];
    return await this.kvsql(transaction).collateLeg2Entry(
      jobKey,
      targetId,
      1,
      guid,
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
    return await this.kvsql(transaction).hincrbyfloat(jobKey, guid, amount);
  }

  async setStateNX(
    jobId: string,
    appId: string,
    status?: number,
    entity?: string,
    transaction?: ProviderTransaction,
  ): Promise<boolean> {
    const hashKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const result = await this.kvsql().hsetnx(
      hashKey,
      ':',
      status?.toString() ?? '1',
      transaction,
      entity,
    );
    if (transaction) return result as unknown as boolean;
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
    const status = await this.kvsql().hset(key, _subscriptions);
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
      subscriptions = await this.kvsql().hgetall(key) || {};
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
      const response = await this.kvsql().hset(key, _subscriptions);
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

  /**
   * Leg1: set hook signal, atomically detecting a pending signal.
   *
   * Standalone (no transaction): INSERT ON CONFLICT no-op captures the
   * current row value via RETURNING, then a follow-up UPDATE overwrites
   * with the hook value. No explicit transaction — the two statements
   * are serialized by the single-threaded event loop on the shared
   * connection.
   *
   * In a transaction: queues the setnxex; pending detection deferred.
   */
  async setHookSignal(
    hook: HookSignal,
    transaction?: ProviderTransaction,
  ): Promise<{ success: boolean; pendingData?: string }> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const { topic, resolved, jobId } = hook;
    const signalKey = `${topic}:${resolved}`;
    const fullKey = `${key}:${signalKey}`;
    const delay = Math.max(hook.expire, HMSH_SIGNAL_EXPIRE);

    if (transaction) {
      //in-transaction: unconditional upsert (overwrites $pending)
      const kv = this.kvsql();
      const tableName = kv.tableForKey(fullKey);
      const storedKey = kv.storageKey(fullKey);
      (transaction as any).addCommand(
        `INSERT INTO ${tableName} (key, value, expiry)
         VALUES ($1, $2, NOW() + INTERVAL '${delay} seconds')
         ON CONFLICT (key) DO UPDATE
           SET value = EXCLUDED.value, expiry = EXCLUDED.expiry`,
        [storedKey, jobId],
        'boolean',
      );
      return { success: true };
    }

    //standalone: INSERT ON CONFLICT no-op captures the current row
    //value (including values from concurrent transactions that committed
    //before our INSERT resolved). xmax = 0 distinguishes fresh insert
    //from conflict. If the row existed, a follow-up UPDATE overwrites
    //with the hook value. No explicit transaction needed — on a shared
    //connection, Node.js serializes the two awaited queries.
    const kv = this.kvsql();
    const tableName = kv.tableForKey(fullKey);
    const storedKey = kv.storageKey(fullKey);

    try {
      //step 1: insert-or-lock — if row exists, no-op acquires row lock
      const lockResult = await this.pgClient.query(
        `INSERT INTO ${tableName} (key, value, expiry)
         VALUES ($1, $2, NOW() + INTERVAL '${delay} seconds')
         ON CONFLICT (key) DO UPDATE
           SET value = ${tableName}.value
         RETURNING value, (xmax = 0) as inserted`,
        [storedKey, jobId],
      );
      const captured = lockResult.rows[0]?.value;
      const wasInsert = lockResult.rows[0]?.inserted;
      const isPending = captured?.startsWith('$pending::');

      if (!wasInsert) {
        //step 2: row existed — overwrite with hook value
        await this.pgClient.query(
          `UPDATE ${tableName}
           SET value = $1, expiry = NOW() + INTERVAL '${delay} seconds'
           WHERE key = $2`,
          [jobId, storedKey],
        );
      }

      if (isPending) {
        this.logger.debug('hook-signal-pending-consumed', {
          key: signalKey,
        });
        return {
          success: true,
          pendingData: captured.slice('$pending::'.length),
        };
      }
      if (!wasInsert && !isPending) {
        //hook already set by a previous Leg1 (idempotent)
        return { success: false };
      }
      return { success: true };
    } catch (error: any) {
      if (
        error?.message?.includes('closed') ||
        error?.message?.includes('queryable')
      ) {
        return { success: false };
      }
      throw error;
    }
  }

  /**
   * Leg2: get hook signal OR atomically set a pending signal.
   *
   * When `pendingData` is provided and no hook signal exists, the
   * pending value is stored so leg1's setHookSignal can detect it.
   *
   * INSERT ON CONFLICT no-op captures the current row value via
   * RETURNING. If a hook exists, returns it without modification.
   * Otherwise stores $pending for leg1 to consume.
   *
   * When `pendingData` is omitted, behaves as a plain read.
   */
  async getHookSignal(
    topic: string,
    resolved: string,
    pendingData?: string,
    pendingExpire?: number,
  ): Promise<string | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const fullKey = `${key}:${topic}:${resolved}`;

    if (!pendingData) {
      //plain read (used by deleteWebHookSignal path, tests, etc.)
      const response = await this.kvsql().get(fullKey);
      if (!response) return undefined;
      const value = response.toString();
      if (value.startsWith('$pending::')) return undefined;
      return value;
    }

    //INSERT ON CONFLICT no-op captures the current row value. If a
    //hook exists, return it without modification. If no hook (fresh
    //insert or existing pending), store/update the pending value.
    const kv = this.kvsql();
    const tableName = kv.tableForKey(fullKey);
    const storedKey = kv.storageKey(fullKey);
    const expire = pendingExpire || HMSH_PENDING_SIGNAL_EXPIRE;
    const pendingValue = `$pending::${pendingData}`;

    try {
      //step 1: insert-or-lock — if row exists, no-op acquires row lock
      const lockResult = await this.pgClient.query(
        `INSERT INTO ${tableName} (key, value, expiry)
         VALUES ($1, $2, NOW() + INTERVAL '${expire} seconds')
         ON CONFLICT (key) DO UPDATE
           SET value = ${tableName}.value
         RETURNING value, (xmax = 0) as inserted`,
        [storedKey, pendingValue],
      );
      const captured = lockResult.rows[0]?.value;
      const wasInsert = lockResult.rows[0]?.inserted;

      if (!wasInsert && captured && !captured.startsWith('$pending::')) {
        //hook exists (registered by concurrent or prior Leg1) — return it
        return captured;
      }

      if (!wasInsert && captured?.startsWith('$pending::')) {
        //step 2: existing pending — update with our new pending value
        await this.pgClient.query(
          `UPDATE ${tableName}
           SET value = $1, expiry = NOW() + INTERVAL '${expire} seconds'
           WHERE key = $2`,
          [pendingValue, storedKey],
        );
      }

      //no hook — $pending was stored (or updated existing pending)
      this.logger.debug('hook-signal-pending-stored', {
        topic,
        resolved,
      });
      return undefined;
    } catch (error: any) {
      if (
        error?.message?.includes('closed') ||
        error?.message?.includes('queryable')
      ) {
        return undefined;
      }
      throw error;
    }
  }

  async deleteHookSignal(
    topic: string,
    resolved: string,
  ): Promise<number | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const response = await this.kvsql().del(`${key}:${topic}:${resolved}`);
    return response ? Number(response) : undefined;
  }

  async addTaskQueues(keys: string[]): Promise<void> {
    const transaction = this.kvsql(this.transact());
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    for (const key of keys) {
      transaction.zadd(zsetKey, Date.now(), key, { nx: true });
    }
    await transaction.exec();
  }

  async getActiveTaskQueue(): Promise<string | null> {
    let workItemKey = this.cache.getActiveTaskQueue(this.appId) || null;
    if (!workItemKey) {
      const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
      const result = await this.kvsql().zrange(zsetKey, 0, 0);
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
    const didRemove = await this.kvsql().zrem(zsetKey, workItemKey);

    if (didRemove) {
      if (scrub) {
        //indexes can be designed to be self-cleaning; `engine.hookAll` exposes this option
        this.kvsql().del(processedKey);
        this.kvsql().del(key.split(':').slice(0, 5).join(':'));
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
    return await this.kvsql().lmove(sourceKey, destinationKey, 'LEFT', 'RIGHT');
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
      await this.kvsql(transaction).expire(jobKey, inSeconds);
    }
  }

  async getDependencies(jobId: string): Promise<string[]> {
    const depParams = { appId: this.appId, jobId };
    const depKey = this.mintKey(KeyType.JOB_DEPENDENTS, depParams);
    return this.kvsql().lrange(depKey, 0, -1);
  }

  /**
   * Register a time hook by inserting a TIMEHOOK message directly into
   * engine_streams with a future visible_at. The message is invisible
   * until the sleep duration elapses, then the engine's normal dequeue
   * picks it up — no intermediate table, no polling, fully transactional.
   */
  async registerTimeHook(
    jobId: string,
    gId: string,
    activityId: string,
    _type: WorkListTaskType,
    deletionTime: number,
    dad: string,
    transaction?: ProviderTransaction,
  ): Promise<void> {
    const schemaName = this.kvsql().safeName(this.appId);
    const delayMs = deletionTime - Date.now();
    const parts = activityId.split(',');
    const aid = parts[0];
    const msgDad = parts.length > 1 ? ',' + parts.slice(1).join(',') : dad;

    const message = JSON.stringify({
      type: 'timehook',
      metadata: { guid: guid(), jid: jobId, gid: gId, dad: msgDad, aid },
      data: { timestamp: Date.now() },
    });

    const sql = `INSERT INTO ${schemaName}.engine_streams
      (stream_name, message, priority, visible_at)
      VALUES ($1, $2, 5, NOW() + INTERVAL '${Math.max(delayMs, 0)} milliseconds')`;
    const params = [this.appId, message];

    if (transaction && typeof (transaction as any).addCommand === 'function') {
      (transaction as any).addCommand(sql, params);
    } else {
      await this.pgClient.query(sql, params);
    }
  }

  async getNextTask(
    _listKey?: string,
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
    //no-op: time hooks are now engine_streams messages with future visible_at.
    //the engine's normal dequeue handles them when they become visible.
    return false;
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
      const result = await this.kvsql().hincrbyfloat(jobKey, ':', amount);
      if (result <= amount) {
        //verify active state; job already interrupted
        throw new Error(`Job ${jobId} already completed`);
      }
      //persist the error unless specifically told not to
      if (options.throw !== false) {
        const errKey = `metadata/err`; //job errors are stored at the path `metadata/err`
        const symbolNames = [`$${topic}`]; //the symbol for `metadata/err` is in the backend
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
    batchSize = 1000, // Unused in SQL provider
    cursor = '0',
  ): Promise<[string, Record<string, string>]> {
    const matchingFields: Record<string, string> = {};
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });

    let enumType: string;
    let dimension: string | null = null;
    if (fieldMatchPattern.includes(',')) {
      const dimReg = /\d[^-]+-/gi;
      const dimensions = fieldMatchPattern.match(dimReg);
      dimension = ',' + (dimensions?.[0] ?? '');
      enumType = 'hmark';
    } else {
      enumType = 'jmark';
    }

    const offset = parseInt(cursor, 10) || 0; // Convert cursor to numeric offset
    const tableName = this.kvsql().tableForKey(jobKey, 'hash');

    // Initialize parameters array and parameter index
    const params: any[] = [jobKey];
    let paramIndex = params.length + 1; // Starts from 2 since $1 is jobKey

    // Build the valid_job CTE to get the job's UUID id
    const validJobSql = `
      SELECT id
      FROM ${tableName}
      WHERE key = $1
      AND (expired_at IS NULL OR expired_at > NOW())
      LIMIT 1
    `;

    // Build conditions for the WHERE clause
    const conditions = [];

    // Add enumType condition
    conditions.push(`a.type = $${paramIndex}`);
    params.push(enumType);
    paramIndex++;

    // Add dimension condition if applicable
    if (dimension) {
      conditions.push(`a.dimension LIKE $${paramIndex}`);
      params.push(`%${dimension}%`);
      paramIndex++;
    }

    // Add limit and offset parameters
    const limitParamIndex = paramIndex;
    const offsetParamIndex = paramIndex + 1;
    params.push(limit, offset);
    paramIndex += 2;

    // Construct the final SQL query
    const sql = `
      WITH valid_job AS (
        ${validJobSql}
      )
      SELECT a.symbol || a.dimension AS field, a.value
      FROM ${tableName}_attributes a
      JOIN valid_job j ON a.job_id = j.id
      WHERE ${conditions.join(' AND ')}
      LIMIT $${limitParamIndex} OFFSET $${offsetParamIndex}
    `;

    // Execute the query and map the results
    const res = await this.pgClient.query(sql, params);
    for (const row of res.rows) {
      matchingFields[row.field] = row.value;
    }

    // Determine the next cursor
    const nextCursor =
      res.rows.length < limit ? '0' : String(offset + res.rows.length);

    return [nextCursor, matchingFields];
  }

  async setCancel(jobId: string, appId: string): Promise<void> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    // Write the cancel marker as a jmark-type field via hset.
    // The hash module's splitField/deriveType classifies '-cancelled-'
    // as type='jmark', which findJobFields returns on re-entry.
    await this.kvsql().hset(jobKey, { '-cancelled-': '1' });
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
      const transaction =
        this.transact() as unknown as KVSQLProviderTransaction;
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

  /**
   * Deploy time-aware notification triggers and functions
   */
  private async deployTimeNotificationTriggers(appId: string): Promise<void> {
    const schemaName = this.kvsql().safeName(appId);
    const client = this.pgClient;

    try {
      // Get the SQL with schema placeholder replaced
      const sql = getTimeNotifySql(schemaName);

      // Execute the entire SQL as one statement (functions contain $$ blocks with semicolons)
      await client.query(sql);

      this.logger.debug('postgres-time-notifications-deployed', {
        appId,
        schemaName,
        message:
          'Time-aware notifications ENABLED - using LISTEN/NOTIFY instead of polling',
      });
    } catch (error) {
      this.logger.error('postgres-time-notifications-deploy-error', {
        appId,
        schemaName,
        error: error.message,
      });
      // Don't throw - fall back to polling mode
    }
  }

  // ── Exporter queries ───────────────────────────────────────────────────────

  /**
   * Fetch activity inputs for a workflow. Used by the exporter to enrich
   * timeline events with activity arguments.
   */
  async getActivityInputs(
    workflowId: string,
    symbolField: string,
  ): Promise<{
    byJobId: Map<string, any>;
    byNameIndex: Map<string, any>;
  }> {
    const { GET_ACTIVITY_INPUTS } = await import('./exporter-sql');
    const schemaName = this.kvsql().safeName(this.appId);
    const sql = GET_ACTIVITY_INPUTS.replace(/{schema}/g, schemaName);

    const jobKeyPattern = `hmsh:${this.appId}:j:-${workflowId}-%`;
    const { symbol, dimension } = splitField(symbolField);
    const result = await this.pgClient.query(sql, [jobKeyPattern, symbol, dimension]);

    const byJobId = new Map<string, any>();
    const byNameIndex = new Map<string, any>();

    for (const row of result.rows) {
      const jobKey = row.key as string;
      const jobId = jobKey.replace(`hmsh:${this.appId}:j:`, '');

      try {
        const parsed = this.parseHmshValue(row.value);
        byJobId.set(jobId, parsed);

        // Extract activityName and executionIndex from job_id
        // Format: -workflowId-$activityName-executionIndex
        const match = jobId.match(/\$([^-]+)-(\d+)$/);
        if (match) {
          const activityName = match[1];
          const executionIndex = match[2];
          byNameIndex.set(`${activityName}:${executionIndex}`, parsed);
        }
      } catch {
        // Skip unparseable values
      }
    }

    // If no results from legacy approach, try direct worker approach:
    // extract arguments from proxyer messages in worker_streams
    if (byNameIndex.size === 0) {
      const { GET_PROXYER_STREAM_INPUTS } = await import('./exporter-sql');
      const streamSql = GET_PROXYER_STREAM_INPUTS.replace(/{schema}/g, schemaName);
      const streamResult = await this.pgClient.query(streamSql, [workflowId]);

      for (const row of streamResult.rows) {
        try {
          const msg = typeof row.message === 'string' ? JSON.parse(row.message) : row.message;
          const data = msg?.data;
          if (data?.activityName && data?.arguments) {
            const activityName = data.activityName;
            const wfId = data.workflowId || '';
            const idxMatch = wfId.match(/-(\d+)$/);
            const execIndex = idxMatch ? idxMatch[1] : '0';
            byNameIndex.set(`${activityName}:${execIndex}`, data.arguments);
            byJobId.set(wfId, data.arguments);
          }
        } catch {
          // Skip unparseable messages
        }
      }
    }

    return { byJobId, byNameIndex };
  }

  /**
   * Fetch child workflow inputs in batch. Used by the exporter to enrich
   * child workflow events with their arguments.
   */
  async getChildWorkflowInputs(
    childJobKeys: string[],
    symbolField: string,
  ): Promise<Map<string, any>> {
    if (childJobKeys.length === 0) {
      return new Map();
    }

    const { buildChildWorkflowInputsQuery } = await import('./exporter-sql');
    const schemaName = this.kvsql().safeName(this.appId);
    const sql = buildChildWorkflowInputsQuery(childJobKeys.length, schemaName);

    const { symbol, dimension } = splitField(symbolField);
    const result = await this.pgClient.query(sql, [...childJobKeys, symbol, dimension]);

    const childInputMap = new Map<string, any>();
    for (const row of result.rows) {
      const jobKey = row.key as string;
      const childId = jobKey.replace(`hmsh:${this.appId}:j:`, '');

      try {
        const parsed = this.parseHmshValue(row.value);
        childInputMap.set(childId, parsed);
      } catch {
        // Skip unparseable values
      }
    }

    return childInputMap;
  }

  /**
   * Fetch job record and attributes by key. Used by the exporter to
   * reconstruct execution history for expired jobs.
   */
  async getJobByKeyDirect(jobKey: string): Promise<{
    job: {
      id: string;
      key: string;
      status: number;
      created_at: Date;
      updated_at: Date;
      expired_at?: Date;
      is_live: boolean;
    };
    attributes: Record<string, string>;
  }> {
    const { GET_JOB_BY_KEY, GET_JOB_ATTRIBUTES } = await import('./exporter-sql');
    const schemaName = this.kvsql().safeName(this.appId);

    const jobSql = GET_JOB_BY_KEY.replace(/{schema}/g, schemaName);
    const jobResult = await this.pgClient.query(jobSql, [jobKey]);

    if (jobResult.rows.length === 0) {
      throw new Error(`No job found for key "${jobKey}"`);
    }

    const job = jobResult.rows[0];

    const attrSql = GET_JOB_ATTRIBUTES.replace(/{schema}/g, schemaName);
    const attrResult = await this.pgClient.query(attrSql, [job.id]);

    const attributes: Record<string, string> = {};
    for (const row of attrResult.rows) {
      attributes[row.field] = row.value;
    }

    return { job, attributes };
  }

  /**
   * Fetch stream message history for a job from worker_streams.
   * Returns raw activity input/output data from soft-deleted messages.
   */
  async getStreamHistory(
    jobId: string,
    options?: {
      activity?: string;
      types?: string[];
    },
  ): Promise<import('../../../../types/exporter').StreamHistoryEntry[]> {
    const {
      GET_STREAM_HISTORY_BY_JID,
      GET_STREAM_HISTORY_BY_JID_AND_TYPE,
      GET_STREAM_HISTORY_BY_JID_AND_AID,
    } = await import('./exporter-sql');
    const schemaName = this.kvsql().safeName(this.appId);

    let sql: string;
    let params: any[];

    if (options?.activity) {
      sql = GET_STREAM_HISTORY_BY_JID_AND_AID.replace(/{schema}/g, schemaName);
      params = [jobId, options.activity];
    } else if (options?.types?.length) {
      sql = GET_STREAM_HISTORY_BY_JID_AND_TYPE.replace(/{schema}/g, schemaName);
      params = [jobId, options.types];
    } else {
      sql = GET_STREAM_HISTORY_BY_JID.replace(/{schema}/g, schemaName);
      params = [jobId];
    }

    const result = await this.pgClient.query(sql, params);

    return result.rows.map((row: any) => {
      let parsed: any = {};
      try {
        parsed = JSON.parse(row.message);
      } catch {
        // message may not be valid JSON
      }
      return {
        id: parseInt(row.id),
        jid: row.jid,
        aid: row.aid,
        dad: row.dad,
        msg_type: row.msg_type,
        topic: row.topic,
        workflow_name: row.workflow_name,
        data: parsed.data || {},
        status: parsed.status,
        code: parsed.code,
        created_at: row.created_at instanceof Date
          ? row.created_at.toISOString()
          : String(row.created_at),
        reserved_at: row.reserved_at
          ? (row.reserved_at instanceof Date
            ? row.reserved_at.toISOString()
            : String(row.reserved_at))
          : undefined,
        expired_at: row.expired_at
          ? (row.expired_at instanceof Date
            ? row.expired_at.toISOString()
            : String(row.expired_at))
          : undefined,
      };
    });
  }

  /**
   * Parse a HotMesh-encoded value string.
   * Values may be prefixed with `/s` (JSON), `/d` (number), `/t` or `/f` (boolean), `/n` (null).
   */
  private parseHmshValue(raw: string): any {
    if (typeof raw !== 'string') return undefined;

    const prefix = raw.slice(0, 2);
    const rest = raw.slice(2);

    switch (prefix) {
      case '/t': return true;
      case '/f': return false;
      case '/d': return Number(rest);
      case '/n': return null;
      case '/s': return JSON.parse(rest);
      default: return raw;
    }
  }
}

export { PostgresStoreService };
