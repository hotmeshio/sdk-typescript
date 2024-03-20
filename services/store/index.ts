import {
  KeyService,
  KeyStoreParams,
  KeyType, 
  HMNS} from '../../modules/key';
import { ILogger } from '../logger';
import { MDATA_SYMBOLS, SerializerService as Serializer } from '../serializer';
import { Cache } from './cache';
import { ActivityType,  Consumes} from '../../types/activity';
import { AppVID } from '../../types/app';
import {
  HookRule,
  HookSignal } from '../../types/hook';
import {
  HotMeshApp,
  HotMeshApps,
  HotMeshSettings } from '../../types/hotmesh';
import {
  SymbolSets,
  StringStringType,
  StringAnyType,
  Symbols } from '../../types/serializer';
import {
  IdsData,
  JobStats,
  JobStatsRange,
  StatsType } from '../../types/stats';
import { Transitions } from '../../types/transition';
import { formatISODate, getSymKey } from '../../modules/utils';
import { ReclaimedMessageType } from '../../types/stream';
import { JobCompletionOptions, JobInterruptOptions } from '../../types/job';
import { HMSH_SCOUT_INTERVAL_SECONDS, HMSH_CODE_INTERRUPT } from '../../modules/enums';
import { GetStateError } from '../../modules/errors';
import { WorkListTaskType } from '../../types/task';

interface AbstractRedisClient {
  exec(): any;
}

abstract class StoreService<T, U extends AbstractRedisClient> {
  redisClient: T;
  cache: Cache;
  serializer: Serializer;
  namespace: string;
  appId: string
  logger: ILogger;
  commands: Record<string, string> = {
    set: 'set',
    setnx: 'setnx',
    del: 'del',
    expire: 'expire',
    hset: 'hset',
    hscan: 'hscan',
    hsetnx: 'hsetnx',
    hincrby: 'hincrby',
    hdel: 'hdel',
    hget: 'hget',
    hmget: 'hmget',
    hgetall: 'hgetall',
    hincrbyfloat: 'hincrbyfloat',
    zrange: 'zrange',
    zrangebyscore_withscores: 'zrangebyscore',
    zrangebyscore: 'zrangebyscore',
    zrem: 'zrem',
    zadd: 'zadd',
    lmove: 'lmove',
    llen: 'llen',
    lpop: 'lpop',
    lrange: 'lrange',
    rename: 'rename',
    rpush: 'rpush',
    scan: 'scan',
    xack: 'xack',
    xdel: 'xdel',
  };


  //todo: standardize signatures and move concrete methods to this class
  abstract getMulti(): U;
  abstract exec(...args: any[]): Promise<string|string[]|string[][]>;
  abstract publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    engineId?: string
  ): Promise<boolean>;
  abstract xgroup(
    command: 'CREATE',
    key: string,
    groupName: string,
    id: string,
    mkStream?: 'MKSTREAM'
  ): Promise<boolean>;
  abstract xadd(
    key: string,
    id: string,
    messageId: string,
    messageValue: string,
    multi?: U): Promise<string | U>;
  abstract xpending(
    key: string,
    group: string,
    start?: string,
    end?: string,
    count?: number,
    consumer?: string): Promise<[string, string, number, [string, number][]][] | [string, string, number, number] | unknown[]>;
  abstract xclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]): Promise<ReclaimedMessageType>;
  abstract xack(
    key: string,
    group: string,
    id: string,
    multi?: U
  ): Promise<number|U>;
  abstract xdel(
    key: string,
    id: string,
    multi?: U
  ): Promise<number|U>;
  abstract xlen(
    key: string,
    multi?: U
  ): Promise<number|U>;

  constructor(redisClient: T) {
    this.redisClient = redisClient;
  }

  async init(namespace = HMNS, appId: string, logger: ILogger): Promise<HotMeshApps> {
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

  async zAdd(key: string, score: number | string, value: string | number, redisMulti?: U): Promise<any> {
    //default call signature uses 'ioredis' NPM Package format
    return await (redisMulti || this.redisClient)[this.commands.zadd](key, score, value);
  }

  async zRangeByScoreWithScores(key: string, score: number | string, value: string | number): Promise<string | null> {
    const result = await this.redisClient[this.commands.zrangebyscore_withscores](key, score, value, 'WITHSCORES');
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  async zRangeByScore(key: string, score: number | string, value: string | number): Promise<string | null> {
    const result = await this.redisClient[this.commands.zrangebyscore](key, score, value);
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
  async reserveScoutRole(scoutType: 'time' | 'signal' | 'activate', delay = HMSH_SCOUT_INTERVAL_SECONDS): Promise<boolean> {
    const key = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId, scoutType });
    const success = await this.exec('SET', key, `${scoutType}:${formatISODate(new Date())}`, 'NX', 'EX', `${delay - 1}`);
    return this.isSuccessful(success);
  }

  async releaseScoutRole(scoutType: 'time' | 'signal' | 'activate'): Promise<boolean> {
    const key = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId, scoutType });
    const success = await this.exec('DEL', key);
    return this.isSuccessful(success);
  }

  async getSettings(bCreate = false): Promise<HotMeshSettings> {
    let settings = this.cache?.getSettings();
    if (settings) {
      return settings;
    } else {
      if (bCreate) {
        const packageJson = await import('../../package.json');
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
    return await this.redisClient[this.commands.hset](key, manifest);
  }

  async reserveSymbolRange(target: string, size: number, type: 'JOB' | 'ACTIVITY'): Promise<[number, number, Symbols]> {
    const rangeKey = this.mintKey(KeyType.SYMKEYS, { appId: this.appId });
    const symbolKey = this.mintKey(KeyType.SYMKEYS, { activityId: target, appId: this.appId });
    //reserve the slot in a `pending` state (range will be established in the next step)
    const response = await this.redisClient[this.commands.hsetnx](rangeKey, target, '?:?');
    if (response) {
      //if the key didn't exist, set the inclusive range and seed metadata fields
      const upperLimit = await this.redisClient[this.commands.hincrby](rangeKey, ':cursor', size);
      const lowerLimit = upperLimit - size;
      const inclusiveRange = `${lowerLimit}:${upperLimit - 1}`;
      await this.redisClient[this.commands.hset](rangeKey, target, inclusiveRange);
      const metadataSeeds = this.seedSymbols(target, type, lowerLimit);
      await this.redisClient[this.commands.hset](symbolKey, metadataSeeds);
      return [lowerLimit + MDATA_SYMBOLS.SLOTS, upperLimit - 1, {} as Symbols];
    } else {
      //if the key already existed, get the lower limit and add the number of symbols
      const range = await this.redisClient[this.commands.hget](rangeKey, target);
      const  [lowerLimitString] = range.split(':');
      const lowerLimit = parseInt(lowerLimitString, 10);
      const symbols = await this.redisClient[this.commands.hgetall](symbolKey);
      const symbolCount = Object.keys(symbols).length;
      const actualLowerLimit = lowerLimit + MDATA_SYMBOLS.SLOTS + symbolCount;
      const upperLimit = Number(lowerLimit + size - 1);
      return [actualLowerLimit, upperLimit, symbols as Symbols];
    }
  }

  async getAllSymbols(): Promise<Symbols> {
    //get hash with all reserved symbol ranges
    const rangeKey = this.mintKey(KeyType.SYMKEYS, { appId: this.appId });
    const ranges = await this.redisClient[this.commands.hgetall](rangeKey);
    const rangeKeys = Object.keys(ranges).sort();
    delete rangeKeys[':cursor'];
    const multi = this.getMulti();
    for (const rangeKey of rangeKeys) {
      const symbolKey = this.mintKey(KeyType.SYMKEYS, { activityId: rangeKey, appId: this.appId });
      multi[this.commands.hgetall](symbolKey);
    }
    const results = await multi.exec() as Array<[null, Symbols]> | Array<Symbols>;

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
          symbolSets[value as string] = key.startsWith(rangeKeys[index]) ? key : `${rangeKeys[index]}/${key}`;
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
      symbols = (await this.redisClient[this.commands.hgetall](key)) as Symbols;
      this.cache.setSymbols(this.appId, activityId, symbols);
      return symbols;
    }
  }

  async addSymbols(activityId: string, symbols: Symbols): Promise<boolean> {
    if (!symbols || !Object.keys(symbols).length) return false;
    const params: KeyStoreParams = { activityId, appId: this.appId };
    const key = this.mintKey(KeyType.SYMKEYS, params);
    const success = await this.redisClient[this.commands.hset](key, symbols);
    this.cache.deleteSymbols(this.appId, activityId);
    return success > 0;
  }

  seedSymbols(target: string, type: 'JOB'|'ACTIVITY', startIndex: number): StringStringType {
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

  seedActivitySymbols(startIndex: number, activityId: string): StringStringType {
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
      symvals = await this.redisClient[this.commands.hgetall](key);
      this.cache.setSymbolValues(this.appId, symvals as Symbols);
      return symvals;
    }
  }

  async addSymbolValues(symvals: Symbols): Promise<boolean> {
    if (!symvals || !Object.keys(symvals).length) return false;
    const key = this.mintKey(KeyType.SYMVALS, { appId: this.appId });
    const success = await this.redisClient[this.commands.hset](key, symvals);
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
      const sApp = await this.redisClient[this.commands.hgetall](key);
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
    await this.redisClient[this.commands.hset](key, payload as any);
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
        active: true
      };
      Object.entries(payload).forEach(([key, value]) => {
        payload[key] = value.toString();
      });
      await this.redisClient[this.commands.hset](key, payload as any);
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
    return await this.redisClient[this.commands.hset](key, payload as any);
  }

  /**
   * Registers the job, `jobId`, with `originJobId`. In the future,
   * when `originJobId` is interrupted/expired, the items in the
   * list (added via RPUSH) will be interrupted/expired (removed via LPOPed).
   */
  async registerJobDependency(depType: WorkListTaskType, originJobId: string, topic: string, jobId: string, gId: string, multi? : U): Promise<any> {
    const privateMulti = multi || this.getMulti();
    const dependencyParams = {
      appId: this.appId,
      jobId: originJobId,
    };
    const depKey = this.mintKey(
      KeyType.JOB_DEPENDENTS,
      dependencyParams,
    );
    //items listed as job dependencies have different relationships
    const expireTask = `${depType}::${topic}::${gId}::${jobId}`;
    privateMulti[this.commands.rpush](depKey, expireTask);
    if (!multi) {
      return await privateMulti.exec();
    }
  }

  /**
   * Ensures a `hook signal` is delisted when its parent activity/job
   * is interrupted/expired.
   */
  async registerSignalDependency(jobId: string, signalKey: string, multi? : U): Promise<any> {
    const privateMulti = multi || this.getMulti();
    const dependencyParams = { appId: this.appId, jobId };
    const dependencyKey = this.mintKey(
      KeyType.JOB_DEPENDENTS,
      dependencyParams,
    );
    //tasks have '4' segments
    const delistTask = `delist::signal::${jobId}::${signalKey}`;
    privateMulti[this.commands.rpush](
      dependencyKey,
      delistTask,
    );
    if (!multi) {
      return await privateMulti.exec();
    }
  }

  async setStats(jobKey: string, jobId: string, dateTime: string, stats: StatsType, appVersion: AppVID, multi? : U): Promise<any> {
    const params: KeyStoreParams = { appId: appVersion.id, jobId, jobKey, dateTime };
    const privateMulti = multi || this.getMulti();
    if (stats.general.length) {
      const generalStatsKey = this.mintKey(KeyType.JOB_STATS_GENERAL, params);
      for (const { target, value } of stats.general) {
        privateMulti[this.commands.hincrbyfloat](generalStatsKey, target, value as number);
      }
    }
    for (const { target, value } of stats.index) {
      const indexParams = { ...params, facet: target };
      const indexStatsKey = this.mintKey(KeyType.JOB_STATS_INDEX, indexParams);
      privateMulti[this.commands.rpush](indexStatsKey, value.toString());
    }
    for (const { target, value } of stats.median) {
      const medianParams = { ...params, facet: target };
      const medianStatsKey = this.mintKey(KeyType.JOB_STATS_MEDIAN, medianParams);
      this.zAdd(medianStatsKey, value, target, privateMulti);
    }
    if (!multi) {
      return await privateMulti.exec();
    }
  }

  hGetAllResult(result: any) {
    //default response signature uses 'redis' NPM Package format
    return result;
  }

  async getJobStats(jobKeys: string[]): Promise<JobStatsRange> {
    const multi = this.getMulti();
    for (const jobKey of jobKeys) {
      multi[this.commands.hgetall](jobKey);
    }
    const results = await multi.exec();
    const output: { [key: string]: JobStats } = {};
    for (const [index, result] of results.entries()) {
      const key = jobKeys[index];
      const statsHash: unknown = this.hGetAllResult(result);
      if (statsHash && Object.keys(statsHash).length > 0) {
        const resolvedStatsHash: JobStats = { ...statsHash as object };
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

  async getJobIds(indexKeys: string[], idRange: [number, number]): Promise<IdsData> {
    const multi = this.getMulti();
    for (const idsKey of indexKeys) {
      multi[this.commands.lrange](idsKey, idRange[0], idRange[1]); //0,-1 returns all ids
    }
    const results = await multi.exec();
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

  async setStatus(collationKeyStatus: number, jobId: string, appId: string, multi? : U): Promise<any> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    return await (multi || this.redisClient)[this.commands.hincrbyfloat](jobKey, ':', collationKeyStatus);
  }

  async getStatus(jobId: string, appId: string): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const status = await this.redisClient[this.commands.hget](jobKey, ':');
    if (status === null) {
      throw new Error(`Job ${jobId} not found`);
    }
    return Number(status);
  }

  async setState({ ...state }: StringAnyType, status: number | null, jobId: string, symbolNames: string[], dIds: StringStringType, multi? : U): Promise<string> {
    delete state['metadata/js'];
    const hashKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const symKeys = await this.getSymbolKeys(symbolNames);
    const symVals = await this.getSymbolValues();
    this.serializer.resetSymbols(symKeys, symVals, dIds);

    const hashData = this.serializer.package(state, symbolNames);
    if (status !== null) {
      hashData[':'] = status.toString();
    } else {
      delete hashData[':'];
    }
    await (multi || this.redisClient)[this.commands.hset](hashKey, hashData);
    return jobId;
  }

  /**
   * Returns custom search fields and values.
   * NOTE: The `fields` param should NOT prefix items with an underscore.
   */
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    const key = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const _fields = fields.map(field => `_${field}`);
    const jobDataArray = await this.redisClient[this.commands.hmget](key, _fields);
    const jobData: StringAnyType = {};
    fields.forEach((field, index) => {
      jobData[field] = jobDataArray[index];
    });
    return jobData;
  }

  async getState(jobId: string, consumes: Consumes, dIds: StringStringType): Promise<[StringAnyType, number] | undefined> {
    //get abbreviated field list (the symbols for the paths)
    const key = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const symbolNames = Object.keys(consumes);
    const symKeys = await this.getSymbolKeys(symbolNames);
    this.serializer.resetSymbols(symKeys, {}, dIds);
    const fields = this.serializer.abbreviate(consumes, symbolNames, [':']);

    const jobDataArray = await this.redisClient[this.commands.hmget](key, fields);
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
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const job = await this.redisClient[this.commands.hgetall](jobKey);
    if (!job) {
      throw new GetStateError(jobId);
    }
    return job;
  }

  /**
   * collate is a generic method for incrementing a value in a hash
   * in order to track their progress during processing.
   */
  async collate(jobId: string, activityId: string, amount: number, dIds: StringStringType, multi? : U): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    const collationKey = `${activityId}/output/metadata/as`; //activity state
    const symbolNames = [activityId];
    const symKeys = await this.getSymbolKeys(symbolNames);
    const symVals = await this.getSymbolValues();
    this.serializer.resetSymbols(symKeys, symVals, dIds);

    const payload = { [collationKey]: amount.toString() }
    const hashData = this.serializer.package(payload, symbolNames);
    const targetId = Object.keys(hashData)[0];
    return await (multi || this.redisClient)[this.commands.hincrbyfloat](jobKey, targetId, amount);
  }

  /**
   * synthentic collation affects those activities in the graph
   * that represent the synthetic DAG that was materialized during compilation;
   * Synthetic targeting ensures that re-entry due to failure can be distinguished from
   * purposeful re-entry.
   */
  async collateSynthetic(jobId: string, guid: string, amount: number, multi? : U): Promise<number> {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    return await (multi || this.redisClient)[this.commands.hincrbyfloat](jobKey, guid, amount);
  }

  async setStateNX(jobId: string, appId: string): Promise<boolean> {
    const hashKey = this.mintKey(KeyType.JOB_STATE, { appId, jobId });
    const result = await this.redisClient[this.commands.hsetnx](hashKey, ':', '1');
    return this.isSuccessful(result);
  }

  async getSchema(activityId: string, appVersion: AppVID): Promise<ActivityType> {
    const schema = this.cache.getSchema(appVersion.id, appVersion.version, activityId);
    if (schema) {
      return schema
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
      const params: KeyStoreParams = { appId: appVersion.id, appVersion: appVersion.version };
      const key = this.mintKey(KeyType.SCHEMAS, params);
      schemas = {};
      const hash = await this.redisClient[this.commands.hgetall](key);
      Object.entries(hash).forEach(([key, value]) => {
        schemas[key] = JSON.parse(value as string);
      });
      this.cache.setSchemas(appVersion.id, appVersion.version, schemas);
      return schemas;
    }
  }

  async setSchemas(schemas: Record<string, ActivityType>, appVersion: AppVID): Promise<any> {
    const params: KeyStoreParams = { appId: appVersion.id, appVersion: appVersion.version };
    const key = this.mintKey(KeyType.SCHEMAS, params);
    const _schemas = {...schemas} as Record<string, string>;
    Object.entries(_schemas).forEach(([key, value]) => {
      _schemas[key] = JSON.stringify(value);
    });
    const response = await this.redisClient[this.commands.hset](key, _schemas);
    this.cache.setSchemas(appVersion.id, appVersion.version, schemas);
    return response;
  }

  async setSubscriptions(subscriptions: Record<string, any>, appVersion: AppVID): Promise<boolean> {
    const params: KeyStoreParams = { appId: appVersion.id, appVersion: appVersion.version };
    const key = this.mintKey(KeyType.SUBSCRIPTIONS, params);
    const _subscriptions = {...subscriptions};
    Object.entries(_subscriptions).forEach(([key, value]) => {
      _subscriptions[key] = JSON.stringify(value);
    });
    const status = await this.redisClient[this.commands.hset](key, _subscriptions);
    this.cache.setSubscriptions(appVersion.id, appVersion.version, subscriptions);
    return this.isSuccessful(status);
  }

  async getSubscriptions(appVersion: AppVID): Promise<Record<string, string>> {
    let subscriptions = this.cache.getSubscriptions(appVersion.id, appVersion.version);
    if (subscriptions && Object.keys(subscriptions).length > 0) {
      return subscriptions;
    } else {
      const params: KeyStoreParams = { appId: appVersion.id, appVersion: appVersion.version };
      const key = this.mintKey(KeyType.SUBSCRIPTIONS, params);
      subscriptions = await this.redisClient[this.commands.hgetall](key) || {};
      Object.entries(subscriptions).forEach(([key, value]) => {
        subscriptions[key] = JSON.parse(value as string);
      });
      this.cache.setSubscriptions(appVersion.id, appVersion.version, subscriptions);
      return subscriptions;
    }
  }

  async getSubscription(topic: string, appVersion: AppVID): Promise<string | undefined> {
    const subscriptions = await this.getSubscriptions(appVersion);
    return subscriptions[topic];
  }

  async setTransitions(transitions: Record<string, any>, appVersion: AppVID): Promise<any> {
    const params: KeyStoreParams = { appId: appVersion.id, appVersion: appVersion.version };
    const key = this.mintKey(KeyType.SUBSCRIPTION_PATTERNS, params);
    const _subscriptions = {...transitions};
    Object.entries(_subscriptions).forEach(([key, value]) => {
      _subscriptions[key] = JSON.stringify(value);
    });
    if (Object.keys(_subscriptions).length !== 0) {
      const response = await this.redisClient[this.commands.hset](key, _subscriptions);
      this.cache.setTransitions(appVersion.id, appVersion.version, transitions);
      return response;
    }
  }

  async getTransitions(appVersion: AppVID): Promise<Transitions> {
    let transitions = this.cache.getTransitions(appVersion.id, appVersion.version);
    if (transitions && Object.keys(transitions).length > 0) {
      return transitions;
    } else {
      const params: KeyStoreParams = { appId: appVersion.id, appVersion: appVersion.version };
      const key = this.mintKey(KeyType.SUBSCRIPTION_PATTERNS, params);
      transitions = {};
      const hash = await this.redisClient[this.commands.hgetall](key);
      Object.entries(hash).forEach(([key, value]) => {
        transitions[key] = JSON.parse(value as string);
      });
      this.cache.setTransitions(appVersion.id, appVersion.version, transitions);
      return transitions;
    }
  }

  async setHookRules(hookRules: Record<string, HookRule[]>): Promise<any> {
    const key = this.mintKey(KeyType.HOOKS, { appId: this.appId });
    const _hooks = { };
    Object.entries(hookRules).forEach(([key, value]) => {
      _hooks[key.toString()] = JSON.stringify(value);
    });
    if (Object.keys(_hooks).length !== 0) {
      const response = await this.redisClient[this.commands.hset](key, _hooks);
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
      const _hooks = await this.redisClient[this.commands.hgetall](key);
      patterns = {};
      Object.entries(_hooks).forEach(([key, value]) => {
        patterns[key] = JSON.parse(value as string);
      });
      this.cache.setHookRules(this.appId, patterns);
      return patterns;
    }
  }

  async setHookSignal(hook: HookSignal, multi?: U): Promise<any> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const { topic, resolved, jobId} = hook; //`${activityId}::${dad}::${gId}::${jobId}`
    const signalKey = `${topic}:${resolved}`;
    const payload = { [signalKey]: jobId };
    await (multi || this.redisClient)[this.commands.hset](key, payload);
    return await this.registerSignalDependency(jobId.split('::')[3], signalKey, multi);
  }

  async getHookSignal(topic: string, resolved: string): Promise<string | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const response = await this.redisClient[this.commands.hget](key, `${topic}:${resolved}`);
    return response ? response.toString() : undefined;
  }

  async deleteHookSignal(topic: string, resolved: string): Promise<number | undefined> {
    const key = this.mintKey(KeyType.SIGNALS, { appId: this.appId });
    const response = await this.redisClient[this.commands.hdel](key, `${topic}:${resolved}`);
    return response ? Number(response) : undefined;
  }

  async addTaskQueues(keys: string[]): Promise<void> {
    const multi = this.getMulti();
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    for (const key of keys) {
      multi[this.commands.zadd](zsetKey, { score: Date.now().toString(), value: key } as any, { NX: true });
    }
    await multi.exec();
  }

  async getActiveTaskQueue(): Promise<string | null> {
    let workItemKey = this.cache.getActiveTaskQueue(this.appId) || null;
    if (!workItemKey) {
      const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
      const result = await this.redisClient[this.commands.zrange](zsetKey, 0, 0);
      workItemKey = result.length > 0 ? result[0] : null;
      if (workItemKey) {
        this.cache.setWorkItem(this.appId, workItemKey);
      }
    }
    return workItemKey;
  }

  async deleteProcessedTaskQueue(workItemKey: string, key: string, processedKey: string, scrub = false): Promise<void> {
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    const didRemove = await this.redisClient[this.commands.zrem](zsetKey, workItemKey);
    if (didRemove) {
      if (scrub) {
        //indexes can be designed to be self-cleaning; `engine.hookAll` exposes this option
        this.redisClient[this.commands.expire](processedKey, 0);
        this.redisClient[this.commands.expire](key.split(":").slice(0, 5).join(":"), 0);
      } else {
        await this.redisClient[this.commands.rename](processedKey, key);
      }
    }
    this.cache.removeWorkItem(this.appId);
  }

  async processTaskQueue(sourceKey: string, destinationKey: string): Promise<any> {
    return await this.redisClient[this.commands.lmove](sourceKey, destinationKey, 'LEFT', 'RIGHT');
  }

  async expireJob(jobId: string, inSeconds: number): Promise<void> {
    if (!isNaN(inSeconds) && inSeconds > 0) {
      const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
      await this.redisClient[this.commands.expire](jobKey, inSeconds);
    }
  }

  /**
   * register the descendants of an expired origin flow to be 
   * expired at a future date; options indicate whether this
   * is a standard `expire` or an `interrupt`
   */
  async registerDependenciesForCleanup(jobId: string, deletionTime: number, options: JobCompletionOptions): Promise<void> {
    const depParams = { appId: this.appId, jobId };
    const depKey = this.mintKey(KeyType.JOB_DEPENDENTS, depParams);
    const context = options.interrupt ? 'INTERRUPT' : 'EXPIRE';
    const depKeyContext = `::${context}::${depKey}`;
    const zsetKey = this.mintKey(KeyType.TIME_RANGE, { appId: this.appId });
    await this.zAdd(zsetKey, deletionTime.toString(), depKeyContext);
  }

  async getDependencies(jobId: string): Promise<string[]> {
    const depParams = { appId: this.appId, jobId };
    const depKey = this.mintKey(KeyType.JOB_DEPENDENTS, depParams);
    return this.redisClient[this.commands.lrange](depKey, 0, -1);
  }

  /**
   * registers a hook activity to be awakened (uses ZSET to
   * store the 'sleep group' and LIST to store the events
   * for the given sleep group. Sleep groups are
   * organized into 'n'-second blocks (LISTS))
   */
  async registerTimeHook(jobId: string, gId: string, activityId: string, type: WorkListTaskType, deletionTime: number, multi?: U): Promise<void> {
    const listKey = this.mintKey(KeyType.TIME_RANGE, { appId: this.appId, timeValue: deletionTime });
    const timeEvent = `${type}::${activityId}::${gId}::${jobId}`;
    const len = await (multi || this.redisClient)[this.commands.rpush](listKey, timeEvent);
    if (multi || len === 1) {
      const zsetKey = this.mintKey(KeyType.TIME_RANGE, { appId: this.appId });
      await this.zAdd(zsetKey, deletionTime.toString(), listKey, multi);
    }
  }

  async getNextTask(listKey?: string): Promise<[listKey: string, jobId: string, gId: string, activityId: string, type: WorkListTaskType] | boolean> {
    const zsetKey = this.mintKey(KeyType.TIME_RANGE, { appId: this.appId });
    listKey = listKey || await this.zRangeByScore(zsetKey, 0, Date.now());
    if (listKey) {
      let [pType, pKey] = this.resolveTaskKeyContext(listKey);
      const timeEvent = await this.redisClient[this.commands.lpop](pKey);
      if (timeEvent) {
        //there are task types
        //1) sleep (awaken), 2) expire (OR expire-child), 3) interrupt, 4) delist, 5) child (just an index helper; no work to do)
        let [type, activityId, gId, ...jobId] = timeEvent.split('::');
        if (type === 'delist') {
          pType = 'delist';
        } else if (type === 'child') {
          pType = 'child';
        } else if (type === 'expire-child') {
          type = 'expire'; //use the same logic as 'expire'
        }
        return [listKey, jobId.join('::'), gId, activityId, pType];
      }
      await this.redisClient[this.commands.zrem](zsetKey, listKey);
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
   * @param {string} listKey - for example `::INTERRUPT::job123` or `job123`
   */
  resolveTaskKeyContext(listKey: string): [WorkListTaskType, string] {
    if (listKey.startsWith('::INTERRUPT')) {
      return ['interrupt', listKey.split('::')[2]];
    } else if (listKey.startsWith('::EXPIRE')) {
      return ['expire', listKey.split('::')[2]];
    } else {
      return ['sleep', listKey];
    }
  }

  /**
   * Interrupts a job and sets sets a job error (410), if 'throw'!=false.
   * This method is called by the engine and not by an activity and is
   * followed by a call to execute job completion/cleanup tasks
   * associated with a job completion event.
   */
  async interrupt(topic: string, jobId: string, options: JobInterruptOptions = {}): Promise<void> {
    try {
      //verify job exists
      const status = await this.getStatus(jobId, this.appId);
      if (status <= 0) {
        //verify still active; job already completed
        throw new Error(`Job ${jobId} already completed`);
      }
      //decrement job status (:) by 1bil
      const amount = -1_000_000_000;
      const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
      const result = await this.redisClient[this.commands.hincrbyfloat](jobKey, ':', amount);
      if (result <= amount) {
        //verify active state; job already interrupted
        throw new Error(`Job ${jobId} already completed`);
      }
      //persist the error unless specifically told not to
      if (options.throw !== false) {
        const errKey = `metadata/err`;       //job errors are stored at the path `metadata/err`
        const symbolNames = [`$${topic}`];   //the symbol for `metadata/err` is in redis and stored using the job topic
        const symKeys = await this.getSymbolKeys(symbolNames);
        const symVals = await this.getSymbolValues();
        this.serializer.resetSymbols(symKeys, symVals, {});

        //persists the standard 410 error (job is `gone`)
        const err = JSON.stringify({
          code: HMSH_CODE_INTERRUPT,
          message: options.reason ?? `job [${jobId}] interrupted`,
          job_id: jobId
        });

        const payload = { [errKey]: amount.toString() }
        const hashData = this.serializer.package(payload, symbolNames);
        const errSymbol = Object.keys(hashData)[0];
        await this.redisClient[this.commands.hset](jobKey, errSymbol, err);
      }
    } catch (e) {
      if (!options.suppress) {
        throw e;
      } else {
        this.logger.debug('suppressed-interrupt', { message: e.message })
      }
    }
  }

  async scrub(jobId: string) {
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    await this.redisClient[this.commands.del](jobKey);
  }

  async findJobs(queryString: string = '*', limit: number = 1000, batchSize: number = 1000): Promise<string[]> {
    const matchKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId: queryString });
    let cursor = '0';
    let keys: string[];
    const matchingKeys: string[] = [];
    do {
      const output = await this.exec(
        'SCAN',
        cursor,
        'MATCH',
        matchKey,
        'COUNT',
        batchSize.toString(),
      ) as unknown as [string, string[]];
      if (Array.isArray(output)) {
        [cursor, keys] = output;
        for (let key of [...keys]) {
          matchingKeys.push(key);
        }
        if (matchingKeys.length >= limit) {
          break;
        }
      } else {
        break;
      }
    } while (cursor !== '0');
    return matchingKeys;
  }

  async findJobFields(jobId: string, fieldMatchPattern: string = '*', limit: number = 1000, batchSize: number = 1000): Promise<string[]> {
    let cursor = '0';
    let fields: string[] = [];
    const matchingFields: string[] = [];
    const jobKey = this.mintKey(KeyType.JOB_STATE, { appId: this.appId, jobId });
    do {
      const output = await this.exec(
        'HSCAN',
        jobKey,
        cursor,
        'MATCH',
        fieldMatchPattern,
        'COUNT',
        batchSize.toString(),
      ) as unknown as [string, string[]];
      console.log(output);
      if (Array.isArray(output)) {
        [cursor, fields] = output;
        for (let i = 0; i < fields.length; i += 2) {
          matchingFields.push(fields[i]);
          if (matchingFields.length >= limit) {
            break;
          }
        }
      } else {
        break;
      }
    } while (cursor !== '0' && matchingFields.length < limit);
    return matchingFields;
  }  
}

export { StoreService };
