import { KeyStoreParams, KeyType } from '../../modules/key';
import { ILogger } from '../logger';
import { SerializerService as Serializer } from '../serializer';
import { Consumes } from '../../types/activity';
import { AppVID } from '../../types/app';
import { HookRule, HookSignal } from '../../types/hook';
import { HotMeshApps, HotMeshSettings } from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { ThrottleOptions } from '../../types/quorum';
import {
  StringAnyType,
  Symbols,
  StringStringType,
  SymbolSets,
} from '../../types/serializer';
import { IdsData, JobStatsRange, StatsType } from '../../types/stats';
import { WorkListTaskType } from '../../types/task';

import { Cache } from './cache';

abstract class StoreService<
  Provider extends ProviderClient,
  TransactionProvider extends ProviderTransaction,
> {
  storeClient: Provider;
  namespace: string;
  appId: string;
  logger: ILogger;
  cache: Cache;
  serializer: Serializer;

  constructor(client: Provider) {
    this.storeClient = client;
  }

  abstract transact(): TransactionProvider;
  abstract init(
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<HotMeshApps>;

  //domain-level methods
  abstract mintKey(type: KeyType, params: KeyStoreParams): string;
  abstract getSettings(bCreate?: boolean): Promise<HotMeshSettings>;
  abstract setSettings(manifest: HotMeshSettings): Promise<any>;
  abstract getApp(id: string, refresh?: boolean): Promise<any>;
  abstract setApp(id: string, version: string): Promise<any>;
  abstract activateAppVersion(id: string, version: string): Promise<boolean>;
  abstract reserveScoutRole(
    scoutType: 'time' | 'signal' | 'activate',
    delay?: number,
  ): Promise<boolean>;
  abstract releaseScoutRole(
    scoutType: 'time' | 'signal' | 'activate',
  ): Promise<boolean>;
  abstract reserveSymbolRange(
    target: string,
    size: number,
    type: 'JOB' | 'ACTIVITY',
    tryCount?: number,
  ): Promise<[number, number, Symbols]>;
  abstract getSymbols(activityId: string): Promise<Symbols>;
  abstract addSymbols(activityId: string, symbols: Symbols): Promise<boolean>;
  abstract getSymbolValues(): Promise<Symbols>;
  abstract addSymbolValues(symvals: Symbols): Promise<boolean>;
  abstract getSymbolKeys(symbolNames: string[]): Promise<SymbolSets>;
  abstract setStats(
    jobKey: string,
    jobId: string,
    dateTime: string,
    stats: StatsType,
    appVersion: AppVID,
    transaction?: TransactionProvider,
  ): Promise<any>;
  abstract getJobStats(jobKeys: string[]): Promise<JobStatsRange>;
  abstract getJobIds(
    indexKeys: string[],
    idRange: [number, number],
  ): Promise<IdsData>;
  abstract setStatus(
    collationKeyStatus: number,
    jobId: string,
    appId: string,
    transaction?: TransactionProvider,
  ): Promise<any>;
  abstract getStatus(jobId: string, appId: string): Promise<number>;
  abstract setStateNX(
    jobId: string,
    appId: string,
    status?: number,
    entity?: string,
  ): Promise<boolean>;
  abstract setState(
    state: StringAnyType,
    status: number | null,
    jobId: string,
    symbolNames: string[],
    dIds: StringStringType,
    transaction?: TransactionProvider,
  ): Promise<string>;
  abstract getQueryState(
    jobId: string,
    fields: string[],
  ): Promise<StringAnyType>;
  abstract getState(
    jobId: string,
    consumes: Consumes,
    dIds: StringStringType,
  ): Promise<[StringAnyType, number] | undefined>;
  abstract getRaw(jobId: string): Promise<StringStringType>;
  abstract collate(
    jobId: string,
    activityId: string,
    amount: number,
    dIds: StringStringType,
    transaction?: TransactionProvider,
  ): Promise<number>;
  abstract collateSynthetic(
    jobId: string,
    guid: string,
    amount: number,
    transaction?: TransactionProvider,
  ): Promise<number>;
  abstract getSchema(activityId: string, appVersion: AppVID): Promise<any>;
  abstract getSchemas(appVersion: AppVID): Promise<Record<string, any>>;
  abstract setSchemas(
    schemas: Record<string, any>,
    appVersion: AppVID,
  ): Promise<any>;
  abstract setSubscriptions(
    subscriptions: Record<string, any>,
    appVersion: AppVID,
  ): Promise<boolean>;
  abstract getSubscriptions(appVersion: AppVID): Promise<Record<string, any>>;
  abstract getSubscription(
    topic: string,
    appVersion: AppVID,
  ): Promise<string | undefined>;
  abstract setTransitions(
    transitions: Record<string, any>,
    appVersion: AppVID,
  ): Promise<any>;
  abstract getTransitions(appVersion: AppVID): Promise<any>;
  abstract setHookRules(hookRules: Record<string, HookRule[]>): Promise<any>;
  abstract getHookRules(): Promise<Record<string, HookRule[]>>;
  abstract getAllSymbols(): Promise<Symbols>;
  abstract setHookSignal(
    hook: HookSignal,
    transaction?: TransactionProvider,
  ): Promise<any>;
  abstract getHookSignal(
    topic: string,
    resolved: string,
  ): Promise<string | undefined>;
  abstract deleteHookSignal(
    topic: string,
    resolved: string,
  ): Promise<number | undefined>;
  abstract addTaskQueues(keys: string[]): Promise<void>;
  abstract getActiveTaskQueue(): Promise<string | null>;
  abstract deleteProcessedTaskQueue(
    workItemKey: string,
    key: string,
    processedKey: string,
    scrub?: boolean,
  ): Promise<void>;
  abstract processTaskQueue(
    sourceKey: string,
    destinationKey: string,
  ): Promise<any>;
  abstract expireJob(
    jobId: string,
    inSeconds: number,
    txProvider?: TransactionProvider,
  ): Promise<void>;
  abstract getDependencies(jobId: string): Promise<string[]>;
  abstract delistSignalKey(key: string, target: string): Promise<void>;
  abstract registerTimeHook(
    jobId: string,
    gId: string,
    activityId: string,
    type: WorkListTaskType,
    deletionTime: number,
    dad: string,
    transaction?: TransactionProvider,
  ): Promise<void>;
  abstract getNextTask(
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
  >;
  abstract interrupt(
    topic: string,
    jobId: string,
    options: { [key: string]: any },
  ): Promise<void>;
  abstract scrub(jobId: string): Promise<void>;
  abstract findJobs(
    queryString?: string,
    limit?: number,
    batchSize?: number,
    cursor?: string,
  ): Promise<[string, string[]]>;
  abstract findJobFields(
    jobId: string,
    fieldMatchPattern?: string,
    limit?: number,
    batchSize?: number,
    cursor?: string,
  ): Promise<[string, StringStringType]>;
  abstract setThrottleRate(options: ThrottleOptions): Promise<void>;
  abstract getThrottleRates(): Promise<StringStringType>;
  abstract getThrottleRate(topic: string): Promise<number>;
}

export { StoreService };
