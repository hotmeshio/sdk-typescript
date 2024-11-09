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
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/hotmesh';
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

abstract class RedisStoreBase<
  ClientProvider extends ProviderClient,
  TransactionProvider extends ProviderTransaction,
> extends StoreService<ClientProvider, TransactionProvider> {
  commands: Record<string, string> = {};

  abstract transact(): TransactionProvider;
  abstract exec(...args: any[]): Promise<any>;
  abstract setnxex(
    key: string,
    value: string,
    expireSeconds: number,
  ): Promise<boolean>;

  constructor(storeClient: ClientProvider) {
    super(storeClient);
    this.storeClient = storeClient;
  }

  async init(
    namespace = HMNS,
    appId: string,
    logger: ILogger,
  ): Promise<HotMeshApps> {
    this.namespace = namespace;
    this.appId = appId;
    this.logger = logger;
    const settings = await this.getSettings(true);
    this.cache = new Cache(appId, settings);
    this.serializer = new Serializer();
    await this.getApp(appId);
    return this.cache.getApps();
  }

  isSuccessful(result: any): boolean {
    return result > 0 || result === 'OK' || result === true;
  }

  async delistSignalKey(key: string, target: string): Promise<void> {
    await this.storeClient[this.commands.del](`${key}:${target}`);
  }

  async zAdd(
    key: string,
    score: number | string,
    value: string | number,
    redisMulti?: TransactionProvider,
  ): Promise<any> {
    //default call signature uses 'ioredis' NPM Package format
    return await (redisMulti || this.storeClient)[this.commands.zadd](
      key,
      score,
      value,
    );
  }

  async zRangeByScoreWithScores(
    key: string,
    score: number | string,
    value: string | number,
  ): Promise<string | null> {
    const result = await this.storeClient[
      this.commands.zrangebyscore_withscores
    ](key, score, value, 'WITHSCORES');
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  async zRangeByScore(
    key: string,
    score: number | string,
    value: string | number,
  ): Promise<string | null> {
    const result = await this.storeClient[this.commands.zrangebyscore](
      key,
      score,
      value,
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
    const success = await this.exec(
      'SET',
      key,
      `${scoutType}:${formatISODate(new Date())}`,
      'NX',
      'EX',
      `${delay - 1}`,
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
    const success = await this.exec('DEL', key);
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
    return await this.storeClient[this.commands.hset](key, manifest);
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
    const response = await this.storeClient[this.commands.hsetnx](
      rangeKey,
      target,
      '?:?',
    );
    if (response) {
      //if the key didn't exist, set the inclusive range and seed metadata fields
      const upperLimit = await this.storeClient[this.commands.hincrby](
        rangeKey,
        ':cursor',
        size,
      );
      const lowerLimit = upperLimit - size;
      const inclusiveRange = `${lowerLimit}:${upperLimit - 1}`;
      await this.storeClient[this.commands.hset](
        rangeKey,
        target,
        inclusiveRange,
      );
      const metadataSeeds = this.seedSymbols(target, type, lowerLimit);
      await this.storeClient[this.commands.hset](symbolKey, metadataSeeds);
      return [lowerLimit + MDATA_SYMBOLS.SLOTS, upperLimit - 1, {} as Symbols];
    } else {
      //if the key already existed, get the lower limit and add the number of symbols
      const range = await this.storeClient[this.commands.hget](
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
          await this.storeClient[this.commands.hgetall](symbolKey);
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
    const ranges = await this.storeClient[this.commands.hgetall](rangeKey);
    const rangeKeys = Object.keys(ranges).sort();
    delete rangeKeys[':cursor'];
    const transaction = this.transact();
    for (const rangeKey of rangeKeys) {
      const symbolKey = this.mintKey(KeyType.SYMKEYS, {
        activityId: rangeKey,
        appId: this.appId,
      });
      transaction[this.commands.hgetall](symbolKey);
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
      symbols = (await this.storeClient[this.commands.hgetall](key)) as Symbols;
      this.cache.setSymbols(this.appId, activityId, symbols);
      return symbols;
    }
  }

  async addSymbols(activityId: string, symbols: Symbols): Promise<boolean> {
    if (!symbols || !Object.keys(symbols).length) return false;
    const params: KeyStoreParams = { activityId, appId: this.appId };
    const key = this.mintKey(KeyType.SYMKEYS, params);
    const success = await this.storeClient[this.commands.hset](key, symbols);
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
      symvals = await this.storeClient[this.commands.hgetall](key);
      this.cache.setSymbolValues(this.appId, symvals as Symbols);
      return symvals;
    }
  }

  async addSymbolValues(symvals: Symbols): Promise<boolean> {
    if (!symvals || !Object.keys(symvals).length) return false;
    const key = this.mintKey(KeyType.SYMVALS, { appId: this.appId });
    const success = await this.storeClient[this.commands.hset](key, symvals);
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
      const sApp = await this.storeClient[this.commands.hgetall](key);
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
    await this.storeClient[this.commands.hset](key, payload as any);
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
      await this.storeClient[this.commands.hset](key, payload as any);
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
    return await this.storeClient[this.commands.hset](key, payload as any);
  }

  async setStats(
    jobKey: string,
    jobId: string,
    dateTime: string,
    stats: StatsType,
    appVersion: AppVID,
    transaction?: TransactionProvider,
  ): Promise<any> {
    const params: KeyStoreParams = {
      appId: appVersion.id,
      jobId,
      jobKey,
      dateTime,
    };
    const privateMulti = transaction || this.transact();
    if (stats.general.length) {
      const generalStatsKey = this.mintKey(KeyType.JOB_STATS_GENERAL, params);
      for (const { target, value } of stats.general) {
        privateMulti[this.commands.hincrbyfloat](
          generalStatsKey,
          target,
          value as number,
        );
      }
    }
    for (const { target, value } of stats.index) {
      const indexParams = { ...params, facet: target };
      const indexStatsKey = this.mintKey(KeyType.JOB_STATS_INDEX, indexParams);
      privateMulti[this.commands.rpush](indexStatsKey, value.toString());
    }
    for (const { target, value } of stats.median) {
      const medianParams = { ...params, facet: target };
      const medianStatsKey = this.mintKey(
        KeyType.JOB_STATS_MEDIAN,
        medianParams,
      );
      this.zAdd(medianStatsKey, value, target, privateMulti);
    }
    if (!transaction) {
      return await privateMulti.exec();
    }
  }

  hGetAllResult(result: any) {
    //default response signature uses 'redis' NPM Package format
    return result;
  }

  async getJobStats(jobKeys: string[]): Promise<JobStatsRange> {
    const transaction = this.transact();
    for (const jobKey of jobKeys) {
      transaction[this.commands.hgetall](jobKey);
    }
    const results = await transaction.exec();
    const output: { [key: string]: JobStats } = {};
    for (const [index, result] of results.entries()) {
      const key = jobKeys[index];
      const statsHash: unknown = this.hGetAllResult(result);
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
    const transaction = this.transact();
    for (const idsKey of indexKeys) {
      transaction[this.commands.lrange](idsKey, idRange[0], idRange[1]); //0,-1 returns all ids
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
    transaction?: TransactionProvider,
  ): Promise<any> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    return await (transaction || this.storeClient)[this.commands.hincrbyfloat](
      jobKey,
      ':',
      collationKeyStatus,
    );
  }

  async getStatus(jobId: string, appId: string): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const status = await this.storeClient[this.commands.hget](jobKey, ':');
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
    transaction?: TransactionProvider,
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
    await (transaction || this.storeClient)[this.commands.hset](
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
    const jobDataArray = await this.storeClient[this.commands.hmget](
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

    const jobDataArray = await this.storeClient[this.commands.hmget](
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
    const job = await this.storeClient[this.commands.hgetall](jobKey);
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
    transaction?: TransactionProvider,
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
    return await (transaction || this.storeClient)[this.commands.hincrbyfloat](
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
    transaction?: TransactionProvider,
  ): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    return await (transaction || this.storeClient)[this.commands.hincrbyfloat](
      jobKey,
      guid,
      amount.toString(),
    );
  }

  async setStateNX(
    jobId: string,
    appId: string,
    status?: number,
  ): Promise<boolean> {
    const hashKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const result = await this.storeClient[this.commands.hsetnx](
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
      const hash = await this.storeClient[this.commands.hgetall](key);
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
    const response = await this.storeClient[this.commands.hset](key, _schemas);
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
    const status = await this.storeClient[this.commands.hset](
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
        await this.storeClient[this.commands.hgetall](key) || {};
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
      const response = await this.storeClient[this.commands.hset](
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
      const hash = await this.storeClient[this.commands.hgetall](key);
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
      const response = await this.storeClient[this.commands.hset](key, _hooks);
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
      const _hooks = await this.storeClient[this.commands.hgetall](key);
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
    transaction?: TransactionProvider,
  ): Promise<any> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const { topic, resolved, jobId } = hook;
    const signalKey = `${topic}:${resolved}`;
    await this.setnxex(
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
    const response = await this.storeClient[this.commands.get](
      `${key}:${topic}:${resolved}`,
    );
    return response ? response.toString() : undefined;
  }

  async deleteHookSignal(
    topic: string,
    resolved: string,
  ): Promise<number | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const response = await this.storeClient[this.commands.del](
      `${key}:${topic}:${resolved}`,
    );
    return response ? Number(response) : undefined;
  }

  async addTaskQueues(keys: string[]): Promise<void> {
    const transaction = this.transact();
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    for (const key of keys) {
      transaction[this.commands.zadd](
        zsetKey,
        { score: Date.now().toString(), value: key } as any,
        { NX: true },
      );
    }
    await transaction.exec();
  }

  async getActiveTaskQueue(): Promise<string | null> {
    let workItemKey = this.cache.getActiveTaskQueue(this.appId) || null;
    if (!workItemKey) {
      const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
      const result = await this.storeClient[this.commands.zrange](
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
    const didRemove = await this.storeClient[this.commands.zrem](
      zsetKey,
      workItemKey,
    );
    if (didRemove) {
      if (scrub) {
        //indexes can be designed to be self-cleaning; `engine.hookAll` exposes this option
        this.storeClient[this.commands.expire](processedKey, 0);
        this.storeClient[this.commands.expire](
          key.split(':').slice(0, 5).join(':'),
          0,
        );
      } else {
        await this.storeClient[this.commands.rename](processedKey, key);
      }
    }
    this.cache.removeWorkItem(this.appId);
  }

  async processTaskQueue(
    sourceKey: string,
    destinationKey: string,
  ): Promise<any> {
    return await this.storeClient[this.commands.lmove](
      sourceKey,
      destinationKey,
      'LEFT',
      'RIGHT',
    );
  }

  async expireJob(
    jobId: string,
    inSeconds: number,
    redisMulti?: TransactionProvider,
  ): Promise<void> {
    if (!isNaN(inSeconds) && inSeconds > 0) {
      const jobKey = this.mintKey(KeyType.JOB_STATE, {
        appId: this.appId,
        jobId,
      });
      await (redisMulti || this.storeClient)[this.commands.expire](
        jobKey,
        inSeconds,
      );
    }
  }

  async getDependencies(jobId: string): Promise<string[]> {
    const depParams = { appId: this.appId, jobId };
    const depKey = this.mintKey(KeyType.JOB_DEPENDENTS, depParams);
    return this.storeClient[this.commands.lrange](depKey, 0, -1);
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
    transaction?: TransactionProvider,
  ): Promise<void> {
    const listKey = this.mintKey(KeyType.TIME_RANGE, {
      appId: this.appId,
      timeValue: deletionTime,
    });
    //construct the composite key (the key has enough info to signal the hook)
    const timeEvent = [type, activityId, gId, dad, jobId].join(VALSEP);
    const len = await (transaction || this.storeClient)[this.commands.rpush](
      listKey,
      timeEvent,
    );
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
      const timeEvent = await this.storeClient[this.commands.lpop](pKey);
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
      await this.storeClient[this.commands.zrem](zsetKey, listKey);
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
      const result = await this.storeClient[this.commands.hincrbyfloat](
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
        await this.storeClient[this.commands.hset](jobKey, errSymbol, err);
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
    await this.storeClient[this.commands.del](jobKey);
  }

  async findJobs(
    queryString = '*',
    limit = 1000,
    batchSize = 1000,
    cursor = '0',
  ): Promise<[string, string[]]> {
    const matchKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId: queryString,
    });
    let keys: string[];
    const matchingKeys: string[] = [];
    do {
      const output = (await this.exec(
        'SCAN',
        cursor,
        'MATCH',
        matchKey,
        'COUNT',
        batchSize.toString(),
      )) as unknown as [string, string[]];
      if (Array.isArray(output)) {
        [cursor, keys] = output;
        for (const key of [...keys]) {
          matchingKeys.push(key);
        }
        if (matchingKeys.length >= limit) {
          break;
        }
      } else {
        break;
      }
    } while (cursor !== '0');
    return [cursor, matchingKeys];
  }

  async findJobFields(
    jobId: string,
    fieldMatchPattern = '*',
    limit = 1000,
    batchSize = 1000,
    cursor = '0',
  ): Promise<[string, StringStringType]> {
    let fields: string[] = [];
    const matchingFields: StringStringType = {};
    const jobKey = this.mintKey(KeyType.JOB_STATE, {
      appId: this.appId,
      jobId,
    });
    let len = 0;
    do {
      const output = (await this.exec(
        'HSCAN',
        jobKey,
        cursor,
        'MATCH',
        fieldMatchPattern,
        'COUNT',
        batchSize.toString(),
      )) as unknown as [string, string[]];
      if (Array.isArray(output)) {
        [cursor, fields] = output;
        for (let i = 0; i < fields.length; i += 2) {
          len++;
          matchingFields[fields[i]] = fields[i + 1];
        }
      } else {
        break;
      }
    } while (cursor !== '0' && len < limit);
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
      await this.storeClient[this.commands.hset](key, {
        [options.topic]: rate,
      });
    } else {
      //if no topic, update all
      const transaction = this.transact();
      transaction[this.commands.del](key);
      transaction[this.commands.hset](key, { ':': rate });
      await transaction.exec();
    }
  }

  async getThrottleRates(): Promise<StringStringType> {
    const key = this.mintKey(KeyType.THROTTLE_RATE, { appId: this.appId });
    const response = await this.storeClient[this.commands.hgetall](key);
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

export { RedisStoreBase };
