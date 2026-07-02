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
  /** Set by HotMesh.init() when `events.publish` is configured. Used by hook.ts Leg1 path. */
  eventsPublish?: (event: import('../../../../types/system_events').SystemEvent) => void | Promise<void>;

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
    // ':' (status) is NOT written to jobs_attributes — jobs.status is
    // maintained by setStatus/setStatusAndCollateGuid. The attribute row
    // was redundant, never read, and contended on every activity.
    delete hashData[':'];
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
    originId?: string,
    parentId?: string,
  ): Promise<boolean> {
    const hashKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const result = await this.kvsql().hsetnx(
      hashKey,
      ':',
      status?.toString() ?? '1',
      transaction,
      entity,
      originId,
      parentId,
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
      //pre-flight: bail early if already inactive (optimization; hincrbyfloat is the real guard)
      const status = await this.getStatus(jobId, this.appId);
      if (status <= 0) {
        throw new Error(`Job ${jobId} already completed`);
      }

      const amount = -1_000_000_000;
      const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });

      //build error symbol BEFORE opening the transaction — symbol lookup is read-only
      let errSymbol: string | undefined;
      let err: string | undefined;
      if (options.throw !== false) {
        const errKey = `metadata/err`;
        const symbolNames = [`$${topic}`];
        const symKeys = await this.getSymbolKeys(symbolNames);
        const symVals = await this.getSymbolValues();
        this.serializer.resetSymbols(symKeys, symVals, {});
        err = JSON.stringify({
          code: options.code ?? HMSH_CODE_INTERRUPT,
          message: options.reason ?? `job [${jobId}] interrupted`,
          stack: options.stack ?? '',
          job_id: jobId,
        });
        const payload = { [errKey]: amount.toString() };
        const hashData = this.serializer.package(payload, symbolNames);
        errSymbol = Object.keys(hashData)[0];
      }

      //single transaction: status decrement + optional error write + escalation cancel.
      //WHERE guard on the escalation UPDATE prevents double-cancel;
      //hincrbyfloat is the atomic idempotency proof checked post-commit.
      const txn = this.kvsql(this.transact());
      txn.hincrbyfloat(jobKey, ':', amount);
      if (errSymbol && err) {
        txn.hset(jobKey, { [errSymbol]: err });
      }
      (txn as any).addCommand(
        `UPDATE public.hmsh_escalations
         SET status = 'cancelled', updated_at = NOW()
         WHERE workflow_id = $1
           AND app_id = $2
           AND status = 'pending'
         RETURNING *`,
        [jobId, this.appId],
        'array',
        (rows: any[]) => rows,
      );
      const results = await (txn as any).exec();

      //results[0] = new status after hincrbyfloat — the atomic idempotency guard
      const newStatus = results[0] as number;
      if (newStatus <= amount) {
        throw new Error(`Job ${jobId} already completed`);
      }

      //fire the callback with any escalation rows cancelled in this same transaction;
      //no second query, no second transaction
      const cancelledEntries = (results[results.length - 1] || []) as any[];
      options.onEscalationsCancelled?.(cancelledEntries);
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
      if (rate < 0) return -1;
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
   * Single indexed lookup of the lineage columns for a job key. Returns the real
   * spawning parent (never the synthetic collator `$C` job) and the root ancestor.
   */
  async getJobLineage(jobKey: string): Promise<{
    parent_id: string | null;
    origin_id: string | null;
  } | null> {
    const { GET_JOB_LINEAGE } = await import('./exporter-sql');
    const schemaName = this.kvsql().safeName(this.appId);
    const sql = GET_JOB_LINEAGE.replace(/{schema}/g, schemaName);
    const result = await this.pgClient.query(sql, [jobKey]);
    if (result.rows.length === 0) return null;
    const { parent_id, origin_id } = result.rows[0];
    return { parent_id: parent_id ?? null, origin_id: origin_id ?? null };
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

  // ─── hmsh_escalations ────────────────────────────────────────────────────────

  private _escalationInsertSql = `
    INSERT INTO public.hmsh_escalations
      (namespace, app_id, signal_key, topic, workflow_id, task_queue, workflow_type,
       type, subtype, entity, description, role, priority,
       origin_id, parent_id, initiated_by, created_by, trace_id, span_id,
       task_id, escalation_payload, metadata, envelope, expires_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7,
       $8, $9, $10, $11, $12, $13,
       $14, $15, $16, $17, $18, $19,
       $20, $21, $22, $23, $24)
    ON CONFLICT (namespace, app_id, signal_key) WHERE signal_key IS NOT NULL DO NOTHING`;

  private _escalationInsertParams(
    params: import('../../../../types/hmsh_escalations').CreateEscalationParams,
  ): unknown[] {
    const {
      namespace, appId, signalKey, topic, workflowId, taskQueue, workflowType,
      type, subtype, entity, description, role, priority,
      originId, parentId, initiatedBy, createdBy, traceId, spanId,
      taskId, escalationPayload, metadata, envelope, expiresAt,
    } = params;
    return [
      namespace ?? 'hmsh',
      appId ?? 'hmsh',
      signalKey ?? null,
      topic ?? null,
      workflowId ?? null,
      taskQueue ?? null,
      workflowType ?? null,
      type ?? null,
      subtype ?? null,
      entity ?? null,
      description ?? null,
      role ?? null,
      priority ?? 5,
      originId ?? null,
      parentId ?? null,
      initiatedBy ?? null,
      createdBy ?? null,
      traceId ?? null,
      spanId ?? null,
      taskId ?? null,
      escalationPayload ? JSON.stringify(escalationPayload) : null,
      metadata ? JSON.stringify(metadata) : null,
      envelope ? JSON.stringify(envelope) : null,
      expiresAt ?? null,
    ];
  }

  async createEscalation(params: import('../../../../types/hmsh_escalations').CreateEscalationParams): Promise<import('../../../../types/hmsh_escalations').EscalationEntry> {
    const result = await this.pgClient.query(
      this._escalationInsertSql + ' RETURNING *',
      this._escalationInsertParams(params),
    );
    return result.rows[0];
  }

  /**
   * Enqueues the escalation INSERT into an existing Leg1 transaction so the row
   * is written atomically with the job state checkpoint. On conflict
   * (ON CONFLICT DO NOTHING) the command is a no-op, making it safe for
   * idempotent re-runs after a crash.
   */
  addEscalationToTransaction(
    params: import('../../../../types/hmsh_escalations').CreateEscalationParams,
    transaction: import('../../../../types/provider').ProviderTransaction,
  ): void {
    (transaction as any).addCommand(
      this._escalationInsertSql + ' RETURNING *',
      this._escalationInsertParams(params),
      'object',
    );
  }

  /**
   * Full-fidelity INSERT for data migration. Preserves the original `id` (UUID),
   * lifecycle state, and timestamps from the source table. Uses
   * `ON CONFLICT (id) DO NOTHING` so re-running a migration batch is safe —
   * rows that already exist are skipped and `null` is returned for them.
   */
  async createEscalationForMigration(
    params: import('../../../../types/hmsh_escalations').MigrateEscalationParams,
  ): Promise<import('../../../../types/hmsh_escalations').EscalationEntry | null> {
    const {
      id,
      namespace, appId, signalKey, topic, workflowId, taskQueue, workflowType,
      type, subtype, entity, description, role, priority,
      originId, parentId, initiatedBy, createdBy, traceId, spanId, taskId,
      escalationPayload, metadata, envelope, expiresAt,
      status, assignedTo, claimExpiresAt, claimedAt, resolvedAt,
      resolverPayload, milestones, createdAt, updatedAt,
    } = params;
    const result = await this.pgClient.query(`
      INSERT INTO public.hmsh_escalations
        (id, namespace, app_id, signal_key, topic, workflow_id, task_queue, workflow_type,
         type, subtype, entity, description, role, priority,
         origin_id, parent_id, initiated_by, created_by, trace_id, span_id, task_id,
         escalation_payload, metadata, envelope, expires_at,
         status, assigned_to, claim_expires_at, claimed_at, resolved_at,
         resolver_payload, milestones, created_at, updated_at)
      VALUES
        ($1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,
         $9,  $10, $11, $12, $13, $14,
         $15, $16, $17, $18, $19, $20, $21,
         $22, $23, $24, $25,
         $26, $27, $28, $29, $30,
         $31, $32, $33, $34)
      ON CONFLICT (id) DO NOTHING
      RETURNING *
    `, [
      id,
      namespace ?? 'hmsh',
      appId ?? 'hmsh',
      signalKey ?? null,
      topic ?? null,
      workflowId ?? null,
      taskQueue ?? null,
      workflowType ?? null,
      type ?? null,
      subtype ?? null,
      entity ?? null,
      description ?? null,
      role ?? null,
      priority ?? 5,
      originId ?? null,
      parentId ?? null,
      initiatedBy ?? null,
      createdBy ?? null,
      traceId ?? null,
      spanId ?? null,
      taskId ?? null,
      escalationPayload ? JSON.stringify(escalationPayload) : null,
      metadata ? JSON.stringify(metadata) : null,
      envelope ? JSON.stringify(envelope) : null,
      expiresAt ?? null,
      status ?? 'pending',
      assignedTo ?? null,
      claimExpiresAt ?? null,
      claimedAt ?? null,
      resolvedAt ?? null,
      resolverPayload ? JSON.stringify(resolverPayload) : null,
      milestones ? JSON.stringify(milestones) : '[]',
      createdAt ?? new Date(),
      updatedAt ?? new Date(),
    ]);
    return result.rows[0] ?? null;
  }

  async getEscalation(id: string, namespace?: string): Promise<import('../../../../types/hmsh_escalations').EscalationEntry | null> {
    const result = await this.pgClient.query(
      `SELECT * FROM public.hmsh_escalations WHERE id = $1${namespace ? ' AND namespace = $2' : ''}`,
      namespace ? [id, namespace] : [id],
    );
    return result.rows[0] ?? null;
  }

  async getEscalationBySignalKey(signalKey: string, namespace?: string): Promise<import('../../../../types/hmsh_escalations').EscalationEntry | null> {
    const result = await this.pgClient.query(
      `SELECT * FROM public.hmsh_escalations WHERE signal_key = $1${namespace ? ' AND namespace = $2' : ''}`,
      namespace ? [signalKey, namespace] : [signalKey],
    );
    return result.rows[0] ?? null;
  }

  private _escalationFilterConditions(
    params: import('../../../../types/hmsh_escalations').ListEscalationsParams,
    startIdx = 1,
  ): { conditions: string[]; values: unknown[]; idx: number } {
    const { namespace, role, roles, type, subtype, entity, status, assignedTo, workflowId, originId,
            available, priority, metadata, ids, taskId } = params;
    const conditions: string[] = [];
    const values: unknown[] = [];
    let idx = startIdx;

    if (namespace)     { conditions.push(`namespace = $${idx++}`);                   values.push(namespace); }
    if (roles?.length) { conditions.push(`role = ANY($${idx++}::text[])`);           values.push(roles); }
    else if (role)     { conditions.push(`role = $${idx++}`);                        values.push(role); }
    if (type)          { conditions.push(`type = $${idx++}`);                        values.push(type); }
    if (subtype)       { conditions.push(`subtype = $${idx++}`);                     values.push(subtype); }
    if (entity)        { conditions.push(`entity = $${idx++}`);                      values.push(entity); }
    if (status)        { conditions.push(`status = $${idx++}`);                      values.push(status); }
    if (assignedTo)    { conditions.push(`assigned_to = $${idx++}`);                 values.push(assignedTo); }
    if (workflowId)    { conditions.push(`workflow_id = $${idx++}`);                 values.push(workflowId); }
    if (originId)      { conditions.push(`origin_id = $${idx++}`);                   values.push(originId); }
    if (priority !== undefined) { conditions.push(`priority = $${idx++}`);           values.push(priority); }
    if (metadata)      { conditions.push(`metadata @> $${idx++}::jsonb`);            values.push(JSON.stringify(metadata)); }
    if (ids?.length)   { conditions.push(`id = ANY($${idx++}::uuid[])`);             values.push(ids); }
    if (taskId)        { conditions.push(`task_id = $${idx++}`);                     values.push(taskId); }
    if (available === true)  { conditions.push(`(assigned_to IS NULL OR (assigned_until IS NOT NULL AND assigned_until <= NOW()))`); }
    if (available === false) { conditions.push(`(assigned_to IS NOT NULL AND (assigned_until IS NULL OR assigned_until > NOW()))`); }

    return { conditions, values, idx };
  }

  async listEscalations(params: import('../../../../types/hmsh_escalations').ListEscalationsParams = {}): Promise<import('../../../../types/hmsh_escalations').EscalationEntry[]> {
    const { sortBy, sortOrder, orderBy: orderByParam, limit = 50, offset = 0 } = params;
    const { conditions, values, idx } = this._escalationFilterConditions(params);

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    let orderByClause: string;
    if (orderByParam?.length) {
      const allowed = new Set(['priority', 'created_at', 'updated_at', 'resolved_at', 'role', 'type']);
      const parts = orderByParam
        .filter(o => allowed.has(o.column))
        .map(o => `${o.column} ${o.direction === 'desc' ? 'DESC' : 'ASC'}`);
      orderByClause = parts.length ? `ORDER BY ${parts.join(', ')}` : 'ORDER BY priority ASC, created_at DESC';
    } else {
      const col = sortBy === 'updated_at' ? 'updated_at' : sortBy === 'created_at' ? 'created_at' : 'priority';
      const dir = sortOrder === 'desc' ? 'DESC' : 'ASC';
      orderByClause = col === 'priority'
        ? `ORDER BY priority ${dir}, created_at DESC`
        : `ORDER BY ${col} ${dir}`;
    }

    values.push(limit, offset);
    const result = await this.pgClient.query(
      `SELECT *,
         (assigned_to IS NULL OR (assigned_until IS NOT NULL AND assigned_until <= NOW())) AS available
       FROM public.hmsh_escalations ${where} ${orderByClause} LIMIT $${idx} OFFSET $${idx + 1}`,
      values,
    );
    return result.rows;
  }

  async countEscalations(params: import('../../../../types/hmsh_escalations').ListEscalationsParams = {}): Promise<number> {
    const { conditions, values } = this._escalationFilterConditions(params);
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const result = await this.pgClient.query(
      `SELECT COUNT(*)::int AS total FROM public.hmsh_escalations ${where}`,
      values,
    );
    return result.rows[0]?.total ?? 0;
  }

  async claimEscalation(params: import('../../../../types/hmsh_escalations').ClaimEscalationParams): Promise<import('../../../../types/hmsh_escalations').ClaimEscalationResult> {
    const { id, namespace, assignee = '', durationMinutes = 1 } = params;
    // all_rows: unlocked read to distinguish 'not-found' vs 'conflict'.
    // target: captures prior_assigned_to for isExtension; locks only when claimable.
    // claimed: writes both assigned_until and claim_expires_at (same value).
    const result = await this.pgClient.query(`
      WITH all_rows AS MATERIALIZED (
        SELECT id FROM public.hmsh_escalations
        WHERE id = $1 ${namespace ? 'AND namespace = $4' : ''}
      ),
      target AS MATERIALIZED (
        SELECT id, assigned_to AS prior_assigned_to FROM public.hmsh_escalations
        WHERE id = $1
          ${namespace ? 'AND namespace = $4' : ''}
          AND status = 'pending'
          AND (assigned_to IS NULL OR assigned_until IS NULL OR assigned_until <= NOW() OR assigned_to = $2)
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      ),
      claimed AS (
        UPDATE public.hmsh_escalations
        SET assigned_to      = $2,
            claimed_at       = NOW(),
            claim_expires_at = NOW() + ($3 * INTERVAL '1 minute'),
            assigned_until   = NOW() + ($3 * INTERVAL '1 minute'),
            updated_at       = NOW()
        FROM target WHERE public.hmsh_escalations.id = target.id
        RETURNING public.hmsh_escalations.*, target.prior_assigned_to
      )
      SELECT c.*, (SELECT EXISTS(SELECT 1 FROM all_rows)) AS row_exists
      FROM (VALUES(1)) AS dummy(v)
      LEFT JOIN claimed c ON true
    `, namespace ? [id, assignee, durationMinutes, namespace] : [id, assignee, durationMinutes]);
    const row = result.rows[0];
    if (row?.id) {
      const isExtension: boolean = row.prior_assigned_to === assignee;
      return { ok: true, entry: row, isExtension };
    }
    if (row?.row_exists) return { ok: false, reason: 'conflict' };
    return { ok: false, reason: 'not-found' };
  }

  async claimEscalationByMetadata(params: import('../../../../types/hmsh_escalations').ClaimByMetadataParams): Promise<import('../../../../types/hmsh_escalations').ClaimByMetadataResult> {
    const { key, value, namespace, assignee = '', durationMinutes = 1, roles, metadata: mergeMeta } = params;
    const filter = JSON.stringify({ [key]: value });
    const mergeJson = mergeMeta ? JSON.stringify(mergeMeta) : null;
    // $1=filter $2=assignee $3=duration $4=roles $5=mergeJson [$6=namespace]
    const nsClause = namespace ? 'namespace = $6 AND' : '';
    const queryParams = namespace
      ? [filter, assignee, durationMinutes, roles ?? null, mergeJson, namespace]
      : [filter, assignee, durationMinutes, roles ?? null, mergeJson];
    const result = await this.pgClient.query(`
      WITH all_candidates AS MATERIALIZED (
        SELECT id FROM public.hmsh_escalations
        WHERE ${nsClause}
              metadata @> $1::jsonb
          AND ($4::text[] IS NULL OR role = ANY($4::text[]))
      ),
      target AS MATERIALIZED (
        SELECT id, assigned_to AS prior_assigned_to FROM public.hmsh_escalations
        WHERE ${nsClause}
              metadata @> $1::jsonb
          AND ($4::text[] IS NULL OR role = ANY($4::text[]))
          AND status = 'pending'
          AND (assigned_to IS NULL OR assigned_until IS NULL OR assigned_until <= NOW() OR assigned_to = $2)
        ORDER BY priority ASC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      ),
      claimed AS (
        UPDATE public.hmsh_escalations
        SET assigned_to      = $2,
            claimed_at       = NOW(),
            claim_expires_at = NOW() + ($3 * INTERVAL '1 minute'),
            assigned_until   = NOW() + ($3 * INTERVAL '1 minute'),
            metadata         = CASE WHEN $5::jsonb IS NOT NULL
                                    THEN COALESCE(public.hmsh_escalations.metadata, '{}'::jsonb) || $5::jsonb
                                    ELSE public.hmsh_escalations.metadata END,
            updated_at       = NOW()
        FROM target WHERE public.hmsh_escalations.id = target.id
        RETURNING public.hmsh_escalations.*, target.prior_assigned_to
      )
      SELECT c.*, (SELECT COUNT(*)::int FROM all_candidates) AS candidates_exist
      FROM (VALUES(1)) AS dummy(v)
      LEFT JOIN claimed c ON true
    `, queryParams);
    const row = result.rows[0];
    const candidatesExist: number = row?.candidates_exist ?? 0;
    if (row?.id) {
      const isExtension: boolean = row.prior_assigned_to === assignee;
      return { ok: true, entry: row, candidatesExist, isExtension };
    }
    return {
      ok: false,
      reason: candidatesExist > 0 ? 'conflict' : 'not-found',
      candidatesExist,
    };
  }

  async releaseEscalation(params: import('../../../../types/hmsh_escalations').ReleaseEscalationParams): Promise<import('../../../../types/hmsh_escalations').ReleaseEscalationResult> {
    const { id, namespace, assignee } = params;
    const result = await this.pgClient.query(`
      WITH target AS MATERIALIZED (
        SELECT id, assigned_to FROM public.hmsh_escalations
        WHERE id = $1 ${namespace ? 'AND namespace = $3' : ''}
          AND status = 'pending' AND assigned_to IS NOT NULL
        FOR UPDATE
      ),
      released AS (
        UPDATE public.hmsh_escalations
        SET status = 'pending', assigned_to = NULL, assigned_until = NULL,
            claimed_at = NULL, claim_expires_at = NULL, updated_at = NOW()
        FROM target
        WHERE public.hmsh_escalations.id = target.id
          AND ($2::text IS NULL OR target.assigned_to = $2)
        RETURNING public.hmsh_escalations.*
      )
      SELECT t.id AS target_id, t.assigned_to AS current_assignee,
        row_to_json(r)::jsonb AS entry_json
      FROM target t
      LEFT JOIN released r ON r.id = t.id
    `, namespace ? [id, assignee ?? null, namespace] : [id, assignee ?? null]);
    const row = result.rows[0];
    if (!row) return { ok: false, reason: 'not-found' };
    if (!row.entry_json) return { ok: false, reason: 'wrong-assignee' };
    return { ok: true, entry: row.entry_json };
  }

  async resolveEscalation(
    params: import('../../../../types/hmsh_escalations').ResolveEscalationParams,
  ): Promise<import('../../../../types/hmsh_escalations').ResolveEscalationResult & { signalKey?: string | null; topic?: string | null }> {
    const { id, namespace, resolverPayload, metadata } = params;
    const payloadJson = resolverPayload ? JSON.stringify(resolverPayload) : null;
    // metaJson is bound at a fixed index; the CASE no-ops when null so resolution
    // can merge into the GIN-indexed metadata without disturbing the param layout.
    const metaJson = metadata ? JSON.stringify(metadata) : null;
    // Explicit transaction: FOR UPDATE locks the row; WHERE guard on UPDATE is the TOCTOU
    // barrier — a concurrent caller whose UPDATE matches 0 rows sees 'already-resolved'.
    // On crash before COMMIT neither write lands; on crash after COMMIT both are durable.
    // The resolved row's signal_key is the durable proof for recovery sweeps.
    await this.pgClient.query('BEGIN');
    try {
      const lockResult = await this.pgClient.query(
        `SELECT id, signal_key, topic, status FROM public.hmsh_escalations
         WHERE id = $1 ${namespace ? 'AND namespace = $2' : ''} FOR UPDATE`,
        namespace ? [id, namespace] : [id],
      );
      if (!lockResult.rows[0]) {
        await this.pgClient.query('ROLLBACK');
        return { ok: false, reason: 'not-found' };
      }
      const { signal_key, topic, status } = lockResult.rows[0];
      if (status === 'cancelled') {
        await this.pgClient.query('ROLLBACK');
        return { ok: false, reason: 'already-cancelled' };
      }
      const updateResult = await this.pgClient.query(
        `UPDATE public.hmsh_escalations
         SET status = 'resolved', resolved_at = NOW(), resolver_payload = $2,
             metadata = CASE WHEN $3::jsonb IS NOT NULL
                             THEN COALESCE(metadata, '{}'::jsonb) || $3::jsonb
                             ELSE metadata END,
             updated_at = NOW()
         WHERE id = $1 ${namespace ? 'AND namespace = $4' : ''} AND status = 'pending'
         RETURNING *`,
        namespace ? [id, payloadJson, metaJson, namespace] : [id, payloadJson, metaJson],
      );
      if (!updateResult.rows[0]) {
        await this.pgClient.query('ROLLBACK');
        return { ok: false, reason: 'already-resolved' };
      }
      await this.pgClient.query('COMMIT');
      return { ok: true, entry: updateResult.rows[0], signalKey: signal_key, topic };
    } catch (e) {
      await this.pgClient.query('ROLLBACK');
      throw e;
    }
  }

  async resolveEscalationByMetadata(
    params: import('../../../../types/hmsh_escalations').ResolveByMetadataParams,
  ): Promise<import('../../../../types/hmsh_escalations').ResolveEscalationResult & { signalKey?: string | null; topic?: string | null }> {
    const { key, value, namespace, resolverPayload, roles, metadata } = params;
    const filter = JSON.stringify({ [key]: value });
    const payloadJson = resolverPayload ? JSON.stringify(resolverPayload) : null;
    const metaJson = metadata ? JSON.stringify(metadata) : null;
    await this.pgClient.query('BEGIN');
    try {
      const lockResult = await this.pgClient.query(
        `SELECT id, signal_key, topic, status FROM public.hmsh_escalations
         WHERE ${namespace ? 'namespace = $3 AND' : ''}
               metadata @> $1::jsonb
           AND ($2::text[] IS NULL OR role = ANY($2::text[]))
           AND status IN ('pending', 'cancelled')
         ORDER BY priority ASC, created_at ASC
         LIMIT 1 FOR UPDATE`,
        namespace ? [filter, roles ?? null, namespace] : [filter, roles ?? null],
      );
      if (!lockResult.rows[0]) {
        await this.pgClient.query('ROLLBACK');
        return { ok: false, reason: 'not-found' };
      }
      const { id, signal_key, topic, status } = lockResult.rows[0];
      if (status === 'cancelled') {
        await this.pgClient.query('ROLLBACK');
        return { ok: false, reason: 'already-cancelled' };
      }
      const updateResult = await this.pgClient.query(
        `UPDATE public.hmsh_escalations
         SET status = 'resolved', resolved_at = NOW(), resolver_payload = $2,
             metadata = CASE WHEN $3::jsonb IS NOT NULL
                             THEN COALESCE(metadata, '{}'::jsonb) || $3::jsonb
                             ELSE metadata END,
             updated_at = NOW()
         WHERE id = $1 AND status = 'pending'
         RETURNING *`,
        [id, payloadJson, metaJson],
      );
      if (!updateResult.rows[0]) {
        await this.pgClient.query('ROLLBACK');
        return { ok: false, reason: 'already-resolved' };
      }
      await this.pgClient.query('COMMIT');
      return { ok: true, entry: updateResult.rows[0], signalKey: signal_key, topic };
    } catch (e) {
      await this.pgClient.query('ROLLBACK');
      throw e;
    }
  }

  async cancelEscalation(id: string, namespace?: string): Promise<import('../../../../types/hmsh_escalations').CancelEscalationResult> {
    const result = await this.pgClient.query(`
      WITH target AS MATERIALIZED (
        SELECT id, status FROM public.hmsh_escalations
        WHERE id = $1 ${namespace ? 'AND namespace = $2' : ''}
        LIMIT 1 FOR UPDATE
      ),
      cancelled AS (
        UPDATE public.hmsh_escalations
        SET status = 'cancelled', updated_at = NOW()
        FROM target
        WHERE public.hmsh_escalations.id = target.id
          AND target.status = 'pending'
        RETURNING public.hmsh_escalations.*
      )
      SELECT t.id, t.status AS prior_status,
        CASE
          WHEN c.id IS NOT NULL THEN 'cancelled'
          WHEN t.id IS NULL     THEN 'not-found'
          ELSE 'already-terminal'
        END AS outcome,
        row_to_json(c.*) AS entry_json
      FROM (SELECT * FROM target) t
      FULL OUTER JOIN (SELECT * FROM cancelled) c ON c.id = t.id
    `, namespace ? [id, namespace] : [id]);
    if (!result.rows[0] || result.rows[0].outcome === 'not-found') return { ok: false, reason: 'not-found' };
    if (result.rows[0].outcome === 'already-terminal') return { ok: false, reason: 'already-terminal' };
    const entry = result.rows[0].entry_json as import('../../../../types/hmsh_escalations').EscalationEntry;
    return { ok: true, entry };
  }

  async escalateEscalationToRole(params: import('../../../../types/hmsh_escalations').EscalateToRoleParams): Promise<import('../../../../types/hmsh_escalations').EscalationEntry | null> {
    const { id, targetRole, namespace } = params;
    const result = await this.pgClient.query(`
      UPDATE public.hmsh_escalations
      SET role             = $2,
          status           = 'pending',
          assigned_to      = NULL,
          assigned_until   = NULL,
          claimed_at       = NULL,
          claim_expires_at = NULL,
          updated_at       = NOW()
      WHERE id = $1 ${namespace ? 'AND namespace = $3' : ''}
        AND status = 'pending'
      RETURNING *
    `, namespace ? [id, targetRole, namespace] : [id, targetRole]);
    return result.rows[0] ?? null;
  }

  async updateEscalation(params: import('../../../../types/hmsh_escalations').UpdateEscalationParams): Promise<import('../../../../types/hmsh_escalations').EscalationEntry | null> {
    const { id, namespace, description, priority, role, taskId, metadata, envelope,
            signalKey, topic, workflowId, taskQueue, workflowType, expiresAt } = params;
    const sets: string[] = [];
    const values: unknown[] = [id];
    let idx = 2;

    if (description  !== undefined) { sets.push(`description = $${idx++}`);   values.push(description); }
    if (priority     !== undefined) { sets.push(`priority = $${idx++}`);      values.push(priority); }
    if (role         !== undefined) { sets.push(`role = $${idx++}`);          values.push(role); }
    if (taskId       !== undefined) { sets.push(`task_id = $${idx++}`);       values.push(taskId); }
    if (envelope     !== undefined) { sets.push(`envelope = $${idx++}`);      values.push(JSON.stringify(envelope)); }
    if (signalKey    !== undefined) { sets.push(`signal_key = $${idx++}`);    values.push(signalKey); }
    if (topic        !== undefined) { sets.push(`topic = $${idx++}`);         values.push(topic); }
    if (workflowId   !== undefined) { sets.push(`workflow_id = $${idx++}`);   values.push(workflowId); }
    if (taskQueue    !== undefined) { sets.push(`task_queue = $${idx++}`);    values.push(taskQueue); }
    if (workflowType !== undefined) { sets.push(`workflow_type = $${idx++}`); values.push(workflowType); }
    if (expiresAt    !== undefined) { sets.push(`expires_at = $${idx++}`);    values.push(expiresAt); }
    // Metadata is merged (not replaced) — caller patches individual keys
    if (metadata !== undefined) {
      sets.push(`metadata = COALESCE(metadata, '{}'::jsonb) || $${idx++}::jsonb`);
      values.push(JSON.stringify(metadata));
    }
    if (!sets.length) return this.getEscalation(id, namespace);
    sets.push(`updated_at = NOW()`);

    const nsClause = namespace ? ` AND namespace = $${idx++}` : '';
    if (namespace) values.push(namespace);

    const result = await this.pgClient.query(
      `UPDATE public.hmsh_escalations SET ${sets.join(', ')} WHERE id = $1${nsClause} RETURNING *`,
      values,
    );
    return result.rows[0] ?? null;
  }

  async appendEscalationMilestones(params: import('../../../../types/hmsh_escalations').AppendMilestonesParams): Promise<import('../../../../types/hmsh_escalations').EscalationEntry | null> {
    const { id, namespace, milestones } = params;
    const stamped = milestones.map(m => ({ ...m, created_at: new Date().toISOString() }));
    const result = await this.pgClient.query(`
      UPDATE public.hmsh_escalations
      SET milestones = milestones || $2::jsonb,
          updated_at = NOW()
      WHERE id = $1 ${namespace ? 'AND namespace = $3' : ''}
      RETURNING *
    `, namespace ? [id, JSON.stringify(stamped), namespace] : [id, JSON.stringify(stamped)]);
    return result.rows[0] ?? null;
  }

  async claimManyEscalations(
    params: import('../../../../types/hmsh_escalations').ClaimManyParams,
  ): Promise<{ entries: import('../../../../types/hmsh_escalations').EscalationEntry[]; skipped: number }> {
    const { ids, namespace, assignee, durationMinutes = 1 } = params;
    const result = await this.pgClient.query(
      `UPDATE public.hmsh_escalations
       SET assigned_to      = $1,
           claimed_at       = NOW(),
           claim_expires_at = NOW() + ($2 * INTERVAL '1 minute'),
           assigned_until   = NOW() + ($2 * INTERVAL '1 minute'),
           updated_at       = NOW()
       WHERE id = ANY($3::uuid[])
         ${namespace ? 'AND namespace = $4' : ''}
         AND status = 'pending'
         AND (assigned_to IS NULL OR assigned_until IS NULL OR assigned_until <= NOW() OR assigned_to = $1)
       RETURNING *`,
      namespace ? [assignee, durationMinutes, ids, namespace] : [assignee, durationMinutes, ids],
    );
    const entries = result.rows as import('../../../../types/hmsh_escalations').EscalationEntry[];
    return { entries, skipped: ids.length - entries.length };
  }

  async escalateManyEscalationsToRole(
    params: import('../../../../types/hmsh_escalations').EscalateManyToRoleParams,
  ): Promise<number> {
    const { ids, namespace, targetRole } = params;
    const result = await this.pgClient.query(
      `UPDATE public.hmsh_escalations
       SET role             = $1,
           status           = 'pending',
           assigned_to      = NULL,
           assigned_until   = NULL,
           claimed_at       = NULL,
           claim_expires_at = NULL,
           updated_at       = NOW()
       WHERE id = ANY($2::uuid[])
         ${namespace ? 'AND namespace = $3' : ''}
         AND status = 'pending'`,
      namespace ? [targetRole, ids, namespace] : [targetRole, ids],
    );
    return result.rowCount ?? 0;
  }

  async updateManyEscalationsPriority(
    params: import('../../../../types/hmsh_escalations').UpdateManyPriorityParams,
  ): Promise<number> {
    const { ids, namespace, priority } = params;
    const result = await this.pgClient.query(
      `UPDATE public.hmsh_escalations
       SET priority = $1, updated_at = NOW()
       WHERE id = ANY($2::uuid[])
         ${namespace ? 'AND namespace = $3' : ''}
         AND status = 'pending'`,
      namespace ? [priority, ids, namespace] : [priority, ids],
    );
    return result.rowCount ?? 0;
  }

  async resolveManyEscalations(
    params: import('../../../../types/hmsh_escalations').ResolveManyParams,
  ): Promise<import('../../../../types/hmsh_escalations').EscalationEntry[]> {
    const { ids, namespace, resolverPayload, metadata } = params;
    const payloadJson = resolverPayload ? JSON.stringify(resolverPayload) : null;
    const metaJson = metadata ? JSON.stringify(metadata) : null;
    const result = await this.pgClient.query(
      `UPDATE public.hmsh_escalations
       SET status           = 'resolved',
           resolved_at      = NOW(),
           resolver_payload = $1,
           metadata         = CASE WHEN $3::jsonb IS NOT NULL
                                   THEN COALESCE(metadata, '{}'::jsonb) || $3::jsonb
                                   ELSE metadata END,
           updated_at       = NOW()
       WHERE id = ANY($2::uuid[])
         ${namespace ? 'AND namespace = $4' : ''}
         AND status = 'pending'
       RETURNING *`,
      namespace ? [payloadJson, ids, metaJson, namespace] : [payloadJson, ids, metaJson],
    );
    return result.rows;
  }

  async escalationStats(
    params: import('../../../../types/hmsh_escalations').StatsEscalationsParams = {},
  ): Promise<import('../../../../types/hmsh_escalations').EscalationStats> {
    const { namespace, roles, period = '24h' } = params;
    if (roles !== undefined && roles.length === 0) {
      return { pending: 0, claimed: 0, created: 0, resolved: 0, by_role: [], by_type: [] };
    }
    const hoursMap: Record<string, number> = { '1h': 1, '24h': 24, '7d': 168, '30d': 720 };
    const hours = hoursMap[period] ?? 24;
    const queryParams: unknown[] = [hours];
    let idx = 2;
    const nsClause  = namespace ? `AND namespace = $${idx++}` : '';
    const rolClause = roles?.length ? `AND role = ANY($${idx++}::text[])` : '';
    if (namespace) queryParams.push(namespace);
    if (roles?.length) queryParams.push(roles);

    // Three bounded sources instead of one all-history scan (v0.25.0):
    //   backlog  — status='pending' partial (bounded by queue depth, never history)
    //   created  — created_at window range (all statuses)
    //   resolved — status='resolved' + resolved_at window range
    // Totals derive from the grouped CTEs (NULL role/type groups included, so
    // totals match the pre-0.25.0 all-row counts); NULL groups are excluded
    // only from the by_role/by_type rollups, as before. resolved_at is only
    // ever set alongside status='resolved' (and never survives a transition
    // back to pending), so the status predicate is a pure index enabler.
    // Roles/types with no pending rows and no window activity no longer emit
    // zero-count rollup entries.
    const result = await this.pgClient.query(`
      WITH pending_by_role AS (
        SELECT role,
          COUNT(*)::int AS pending,
          COUNT(*) FILTER (WHERE assigned_to IS NOT NULL
            AND assigned_until IS NOT NULL AND assigned_until > NOW())::int AS claimed
        FROM public.hmsh_escalations
        WHERE status = 'pending' ${nsClause} ${rolClause}
        GROUP BY role
      ),
      pending_by_type AS (
        SELECT type,
          COUNT(*)::int AS pending,
          COUNT(*) FILTER (WHERE assigned_to IS NOT NULL
            AND assigned_until IS NOT NULL AND assigned_until > NOW())::int AS claimed
        FROM public.hmsh_escalations
        WHERE status = 'pending' ${nsClause} ${rolClause}
        GROUP BY type
      ),
      created_agg AS (
        SELECT COUNT(*)::int AS created
        FROM public.hmsh_escalations
        WHERE created_at >= NOW() - ($1 * INTERVAL '1 hour') ${nsClause} ${rolClause}
      ),
      resolved_by_type AS (
        SELECT type, COUNT(*)::int AS resolved
        FROM public.hmsh_escalations
        WHERE status = 'resolved'
          AND resolved_at >= NOW() - ($1 * INTERVAL '1 hour') ${nsClause} ${rolClause}
        GROUP BY type
      )
      SELECT
        COALESCE((SELECT SUM(pending)::int  FROM pending_by_role), 0)  AS pending,
        COALESCE((SELECT SUM(claimed)::int  FROM pending_by_role), 0)  AS claimed,
        (SELECT created FROM created_agg)                              AS created,
        COALESCE((SELECT SUM(resolved)::int FROM resolved_by_type), 0) AS resolved,
        COALESCE((SELECT json_agg(json_build_object('role',role,'pending',pending,'claimed',claimed))
          FROM pending_by_role WHERE role IS NOT NULL), '[]'::json) AS by_role,
        COALESCE((SELECT json_agg(json_build_object('type',t.type,'pending',t.pending,'claimed',t.claimed,'resolved',t.resolved))
          FROM (
            SELECT COALESCE(p.type, r.type) AS type,
                   COALESCE(p.pending, 0)   AS pending,
                   COALESCE(p.claimed, 0)   AS claimed,
                   COALESCE(r.resolved, 0)  AS resolved
            FROM pending_by_type p
            FULL OUTER JOIN resolved_by_type r ON r.type = p.type
            WHERE COALESCE(p.type, r.type) IS NOT NULL
          ) t), '[]'::json) AS by_type
    `, queryParams);
    const row = result.rows[0];
    return {
      pending:  row.pending  ?? 0,
      claimed:  row.claimed  ?? 0,
      created:  row.created  ?? 0,
      resolved: row.resolved ?? 0,
      by_role:  row.by_role  ?? [],
      by_type:  row.by_type  ?? [],
    };
  }

  async listDistinctEscalationTypes(namespace?: string): Promise<string[]> {
    const result = await this.pgClient.query(
      `SELECT DISTINCT type FROM public.hmsh_escalations
       WHERE type IS NOT NULL ${namespace ? 'AND namespace = $1' : ''}
       ORDER BY type`,
      namespace ? [namespace] : [],
    );
    return result.rows.map((r: { type: string }) => r.type);
  }

  async releaseExpiredEscalations(_namespace?: string): Promise<number> {
    // No-op in the implicit claim model: availability is computed at query time from
    // assigned_until, so no background sweep is needed to release expired claims.
    return 0;
  }
}

export { PostgresStoreService };
