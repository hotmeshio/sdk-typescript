/**
 * EngineService — the workflow execution engine.
 *
 * Consumes stream messages from the router and dispatches them
 * to the appropriate activity handler (Trigger, Worker, Hook, …).
 *
 * Each section delegates to a purpose-specific module inside `engine/`.
 * Open the module when you need implementation detail; read this file
 * when you need the big picture.
 *
 * Lifecycle (maps to modules):
 *   1. INIT       → init.ts       (channel setup, router, config)
 *   2. VERSION    → version.ts    (app version resolution, caching)
 *   3. SCHEMA     → schema.ts     (activity lookup, handler factory)
 *   4. COMPILE    → compiler.ts   (YAML plan & deploy)
 *   5. REPORT     → reporting.ts  (stats, IDs, query resolution)
 *   6. DISPATCH   → dispatch.ts   (stream message → activity handler)
 *   7. COMPLETION → completion.ts (parent notify, cleanup, expiry)
 *   8. SIGNAL     → signal.ts     (webhook/timehook delivery, fan-out)
 *   9. PUB/SUB    → pubsub.ts     (topic messaging, subscriptions)
 *  10. STATE      → state.ts      (job state retrieval, export)
 */

import { KeyType } from '../../modules/key';
import { HMSH_OTT_WAIT_TIME } from '../../modules/enums';
import { restoreHierarchy } from '../../modules/utils';
import { ExporterService } from '../exporter';
import { ILogger } from '../logger';
import { Router } from '../router';
import { SearchService } from '../search';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import { TaskService } from '../task';
import { AppVID } from '../../types/app';
import { ActivityType } from '../../types/activity';
import { CacheMode } from '../../types/cache';
import { ExportOptions, JobExport } from '../../types/exporter';
import {
  JobState,
  JobData,
  JobOutput,
  JobStatus,
  JobInterruptOptions,
  JobCompletionOptions,
  ExtensionType,
} from '../../types/job';
import {
  HotMeshApps,
  HotMeshConfig,
  HotMeshManifest,
  HotMeshSettings,
} from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { JobMessageCallback } from '../../types/quorum';
import { StringAnyType, StringStringType } from '../../types/serializer';
import {
  GetStatsOptions,
  IdsResponse,
  JobStatsInput,
  StatsResponse,
} from '../../types/stats';
import {
  StreamCode,
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../types/stream';
import { WorkListTaskType } from '../../types/task';

import * as Init from './init';
import * as Version from './version';
import * as Schema from './schema';
import * as Compiler from './compiler';
import * as Reporting from './reporting';
import * as Dispatch from './dispatch';
import * as Completion from './completion';
import * as Signal from './signal';
import * as PubSub from './pubsub';
import * as State from './state';

class EngineService {

  // ── identity & runtime ────────────────────────────────────────────

  namespace: string;
  apps: HotMeshApps | null;
  appId: string;
  guid: string;
  inited: string;

  // ── services ──────────────────────────────────────────────────────

  exporter: ExporterService | null;
  /** @hidden */
  search: SearchService<ProviderClient> | null;
  /** @hidden */
  store: StoreService<ProviderClient, ProviderTransaction> | null;
  /** @hidden */
  stream: StreamService<ProviderClient, ProviderTransaction> | null;
  /** @hidden */
  subscribe: SubService<ProviderClient> | null;
  /** @hidden */
  router: Router<typeof this.stream> | null;
  /** @hidden */
  taskService: TaskService | null;
  logger: ILogger;

  // ── execution state ───────────────────────────────────────────────

  cacheMode: CacheMode = 'cache';
  untilVersion: string | null = null;
  jobCallbacks: Record<string, JobMessageCallback> = {};

  // ═════════════════════════════════════════════════════════════════
  //  1. INIT — bootstrap channels, router, and services
  //     → see init.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  constructor() {}

  /**
   * @private
   */
  static async init(
    namespace: string,
    appId: string,
    guid: string,
    config: HotMeshConfig,
    logger: ILogger,
  ): Promise<EngineService> {
    if (config.engine) {
      const instance = new EngineService();
      Init.verifyEngineFields(config);

      instance.namespace = namespace;
      instance.appId = appId;
      instance.guid = guid;
      instance.logger = logger;

      await Init.initSearchChannel(instance, config.engine.store);
      await Init.initStoreChannel(instance, config.engine.store);
      await Init.initSubChannel(
        instance,
        config.engine.sub,
        config.engine.pub ?? config.engine.store,
      );
      await Init.initStreamChannel(
        instance,
        config.engine.stream,
        config.engine.store,
      );

      instance.router = await Init.initRouter(instance, config);
      await Init.registerStreamConsumer(instance, config);
      Init.initServices(instance);

      return instance;
    }
  }

  /**
   * @private
   */
  async getSettings(): Promise<HotMeshSettings> {
    return Init.getSettings(this);
  }

  // ═════════════════════════════════════════════════════════════════
  //  2. VERSION — app version resolution and cache management
  //     → see version.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async getVID(vid?: AppVID): Promise<AppVID> {
    return Version.getVID(this, vid);
  }

  /**
   * @private
   */
  setCacheMode(cacheMode: CacheMode, untilVersion: string) {
    Version.setCacheMode(this, cacheMode, untilVersion);
  }

  // ═════════════════════════════════════════════════════════════════
  //  3. SCHEMA — activity schema lookup and handler instantiation
  //     → see schema.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async initActivity(
    topic: string,
    data: JobData = {},
    context?: JobState,
  ) {
    return Schema.initActivity(this, topic, data, context);
  }

  /**
   * @private
   */
  async getSchema(
    topic: string,
  ): Promise<[activityId: string, schema: ActivityType]> {
    return Schema.getSchema(this, topic);
  }

  /**
   * @private
   */
  isPrivate(topic: string): boolean {
    return Schema.isPrivate(topic);
  }

  // ═════════════════════════════════════════════════════════════════
  //  4. COMPILE — YAML app plan & deploy
  //     → see compiler.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async plan(pathOrYAML: string): Promise<HotMeshManifest> {
    return Compiler.plan(this, pathOrYAML);
  }

  /**
   * @private
   */
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return Compiler.deploy(this, pathOrYAML);
  }

  // ═════════════════════════════════════════════════════════════════
  //  5. REPORT — stats queries and job-ID lookups
  //     → see reporting.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return Reporting.getStats(this, topic, query);
  }

  /**
   * @private
   */
  async getIds(
    topic: string,
    query: JobStatsInput,
    queryFacets: string[] = [],
  ): Promise<IdsResponse> {
    return Reporting.getIds(this, topic, query, queryFacets);
  }

  /**
   * @private
   */
  async resolveQuery(
    topic: string,
    query: JobStatsInput,
  ): Promise<GetStatsOptions> {
    return Reporting.resolveQuery(this, topic, query);
  }

  // ═════════════════════════════════════════════════════════════════
  //  6. DISPATCH — stream message routing to activity handlers
  //     → see dispatch.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async processStreamMessage(streamData: StreamDataResponse): Promise<void> {
    return Dispatch.processStreamMessage(this, streamData);
  }

  // ═════════════════════════════════════════════════════════════════
  //  7. COMPLETION — parent notification, cleanup, and expiry
  //     → see completion.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async execAdjacentParent(
    context: JobState,
    jobOutput: JobOutput,
    emit = false,
    transaction?: ProviderTransaction,
  ): Promise<string> {
    return Completion.execAdjacentParent(this, context, jobOutput, emit, transaction);
  }

  /**
   * @private
   */
  hasParentJob(context: JobState, checkSevered = false): boolean {
    return Completion.hasParentJob(context, checkSevered);
  }

  /**
   * @private
   */
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    return Completion.interrupt(this, topic, jobId, options);
  }

  /**
   * @private
   */
  async scrub(jobId: string) {
    return Completion.scrub(this, jobId);
  }

  /**
   * @private
   */
  async runJobCompletionTasks(
    context: JobState,
    options: JobCompletionOptions = {},
    transaction?: ProviderTransaction,
  ): Promise<string | void> {
    return Completion.runJobCompletionTasks(this, context, options, transaction);
  }

  // ═════════════════════════════════════════════════════════════════
  //  8. SIGNAL — webhook/timehook delivery and fan-out
  //     → see signal.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async signal(
    topic: string,
    data: JobData,
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
    transaction?: ProviderTransaction,
  ): Promise<string> {
    return Signal.signal(this, topic, data, status, code, transaction);
  }

  /**
   * @private
   */
  async hookTime(
    jobId: string,
    gId: string,
    topicOrActivity: string,
    type?: WorkListTaskType,
  ): Promise<string | void> {
    return Signal.hookTime(this, jobId, gId, topicOrActivity, type);
  }

  /**
   * @private
   */
  async signalAll(
    hookTopic: string,
    data: JobData,
    keyResolver: JobStatsInput,
    queryFacets: string[] = [],
  ): Promise<string[]> {
    return Signal.signalAll(this, hookTopic, data, keyResolver, queryFacets);
  }

  /**
   * @private
   */
  async routeToSubscribers(topic: string, message: JobOutput) {
    return Signal.routeToSubscribers(this, topic, message);
  }

  /**
   * @private
   */
  async processWebHooks() {
    return Signal.processWebHooks(this, this.signal.bind(this));
  }

  /**
   * @private
   */
  async processTimeHooks() {
    return Signal.processTimeHooks(this, this.hookTime.bind(this));
  }

  /**
   * @private
   */
  async throttle(delayInMillis: number) {
    return Signal.throttle(this, delayInMillis);
  }

  // ═════════════════════════════════════════════════════════════════
  //  9. PUB/SUB — topic messaging, subscriptions, callbacks
  //     → see pubsub.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async pub(
    topic: string,
    data: JobData,
    context?: JobState,
    extended?: ExtensionType,
  ): Promise<string> {
    return PubSub.pub(this, topic, data, context, extended);
  }

  /**
   * @private
   */
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return PubSub.sub(this, topic, callback);
  }

  /**
   * @private
   */
  async unsub(topic: string): Promise<void> {
    return PubSub.unsub(this, topic);
  }

  /**
   * @private
   */
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return PubSub.psub(this, wild, callback);
  }

  /**
   * @private
   */
  async punsub(wild: string): Promise<void> {
    return PubSub.punsub(this, wild);
  }

  /**
   * @private
   */
  async pubsub(
    topic: string,
    data: JobData,
    context?: JobState | null,
    timeout = HMSH_OTT_WAIT_TIME,
  ): Promise<JobOutput> {
    return PubSub.pubsub(this, topic, data, context, timeout);
  }

  /**
   * @private
   */
  async pubOneTimeSubs(
    context: JobState,
    jobOutput: JobOutput,
    emit = false,
    transaction?: ProviderTransaction,
  ) {
    if (PubSub.hasOneTimeSubscription(context)) {
      const message = {
        type: 'job',
        topic: context.metadata.jid,
        job: restoreHierarchy(jobOutput) as JobOutput,
      };
      await this.subscribe.publish(
        KeyType.QUORUM,
        message,
        this.appId,
        context.metadata.ngn,
        transaction,
      );
    }
  }

  /**
   * @private
   */
  async pubPermSubs(
    context: JobState,
    jobOutput: JobOutput,
    emit = false,
    transaction?: ProviderTransaction,
  ) {
    const topic = await PubSub.getPublishesTopic(this, context);
    if (topic) {
      const message = {
        type: 'job',
        topic,
        job: restoreHierarchy(jobOutput) as JobOutput,
      };
      await this.subscribe.publish(
        KeyType.QUORUM,
        message,
        this.appId,
        `${topic}.${context.metadata.jid}`,
        transaction,
      );
    }
  }

  /**
   * @private
   */
  async getPublishesTopic(context: JobState): Promise<string> {
    return PubSub.getPublishesTopic(this, context);
  }

  /**
   * @private
   */
  async add(streamData: StreamData | StreamDataResponse): Promise<string> {
    return PubSub.add(this, streamData);
  }

  /**
   * @private
   */
  registerJobCallback(jobId: string, jobCallback: JobMessageCallback) {
    PubSub.registerJobCallback(this, jobId, jobCallback);
  }

  /**
   * @private
   */
  delistJobCallback(jobId: string) {
    PubSub.removeJobCallback(this, jobId);
  }

  /**
   * @private
   */
  hasOneTimeSubscription(context: JobState): boolean {
    return PubSub.hasOneTimeSubscription(context);
  }

  // ═════════════════════════════════════════════════════════════════
  //  10. STATE — job state retrieval, export, and compression
  //      → see state.ts
  // ═════════════════════════════════════════════════════════════════

  /**
   * @private
   */
  async export(jobId: string, options: ExportOptions = {}): Promise<JobExport> {
    return State.exportJob(this, jobId, options);
  }

  /**
   * @private
   */
  async getRaw(jobId: string): Promise<StringStringType> {
    return State.getRaw(this, jobId);
  }

  /**
   * @private
   */
  async getStatus(jobId: string): Promise<JobStatus> {
    return State.getStatus(this, jobId);
  }

  /**
   * @private
   */
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return State.getState(this, topic, jobId);
  }

  /**
   * @private
   */
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return State.getQueryState(this, jobId, fields);
  }

  /**
   * @private
   * @deprecated
   */
  async compress(terms: string[]): Promise<boolean> {
    return State.compress(this, terms);
  }
}

export { EngineService };
