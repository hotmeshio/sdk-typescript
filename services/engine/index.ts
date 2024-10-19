import { KeyType, VALSEP } from '../../modules/key';
import {
  HMSH_OTT_WAIT_TIME,
  HMSH_CODE_SUCCESS,
  HMSH_CODE_PENDING,
  HMSH_CODE_TIMEOUT,
  HMSH_EXPIRE_JOB_SECONDS,
  HMSH_QUORUM_DELAY_MS,
} from '../../modules/enums';
import {
  formatISODate,
  getSubscriptionTopic,
  guid,
  identifyRedisType,
  polyfill,
  restoreHierarchy,
  sleepFor,
} from '../../modules/utils';
import Activities from '../activities';
import { Await } from '../activities/await';
import { Cycle } from '../activities/cycle';
import { Hook } from '../activities/hook';
import { Interrupt } from '../activities/interrupt';
import { Signal } from '../activities/signal';
import { Worker } from '../activities/worker';
import { Trigger } from '../activities/trigger';
import { CompilerService } from '../compiler';
import { ExporterService } from '../exporter';
import { ILogger } from '../logger';
import { ReporterService } from '../reporter';
import { Router } from '../router';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import { TaskService } from '../task';
import { AppVID } from '../../types/app';
import { ActivityMetadata, ActivityType, Consumes } from '../../types/activity';
import { CacheMode } from '../../types/cache';
import { JobExport } from '../../types/exporter';
import {
  JobState,
  JobData,
  JobMetadata,
  JobOutput,
  PartialJobState,
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
import {
  JobMessage,
  JobMessageCallback,
  SubscriptionCallback,
} from '../../types/quorum';
import { RedisClient, RedisMulti } from '../../types/redis';
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
  StreamDataType,
  StreamError,
  StreamRole,
  StreamStatus,
} from '../../types/stream';
import { WorkListTaskType } from '../../types/task';
import { StreamServiceFactory } from '../stream/factory';
import { SubServiceFactory } from '../sub/factory';
import { StoreServiceFactory } from '../store/factory';
import { SearchService } from '../search';
import { SearchServiceFactory } from '../search/factory';

class EngineService {
  namespace: string;
  apps: HotMeshApps | null;
  appId: string;
  guid: string;
  exporter: ExporterService | null;
  router: Router | null;
  search: SearchService<RedisClient> | null;
  store: StoreService<RedisClient, RedisMulti> | null;
  stream: StreamService<RedisClient, RedisMulti> | null;
  subscribe: SubService<RedisClient, RedisMulti> | null;
  taskService: TaskService | null;
  logger: ILogger;
  cacheMode: CacheMode = 'cache';
  untilVersion: string | null = null;
  jobCallbacks: Record<string, JobMessageCallback> = {};
  reporting = false;
  jobId = 1;
  inited: string;

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
      instance.verifyEngineFields(config);

      instance.namespace = namespace;
      instance.appId = appId;
      instance.guid = guid;
      instance.logger = logger;

      await instance.initSearchChannel(config.engine.store);
      await instance.initStoreChannel(config.engine.store);
      await instance.initSubChannel(config.engine.sub, config.engine.store);
      await instance.initStreamChannel(config.engine.stream, config.engine.store);

      instance.router = await instance.initRouter(config);
      const streamName = instance.store.mintKey(KeyType.STREAMS, { appId: instance.appId });
      instance.router.consumeMessages(
        streamName,
        'ENGINE',
        instance.guid,
        instance.processStreamMessage.bind(instance),
      );

      instance.taskService = new TaskService(instance.store, logger);
      instance.exporter = new ExporterService(
        instance.appId,
        instance.store,
        logger,
      );
      instance.inited = formatISODate(new Date());
      return instance;
    }
  }

  /**
   * @private
   */
  verifyEngineFields(config: HotMeshConfig) {
    if (
      !identifyRedisType(config.engine.store) ||
      !identifyRedisType(config.engine.stream) ||
      !identifyRedisType(config.engine.sub)
    ) {
      throw new Error('engine config must reference 3 redis client instances');
    }
  }

  /**
   * @private
   */
  async initSearchChannel(search: RedisClient, store?: RedisClient) {
    this.search = await SearchServiceFactory.init(
      search,
      store,
      this.namespace,
      this.appId,
      this.logger,
    );
  }

  /**
   * @private
   */
  async initStoreChannel(store: RedisClient) {
    this.store = await StoreServiceFactory.init(
      store,
      this.namespace,
      this.appId,
      this.logger,
    );
  }

  /**
   * @private
   */
  async initSubChannel(sub: RedisClient, store: RedisClient) {
    this.subscribe = await SubServiceFactory.init(
      sub,
      store,
      this.namespace,
      this.appId,
      this.guid,
      this.logger,
    );
  }

  /**
   * @private
   */
  async initStreamChannel(stream: RedisClient, store: RedisClient) {
    this.stream = await StreamServiceFactory.init(
      stream,
      store,
      this.namespace,
      this.appId,
      this.logger,
    );
  }

  /**
   * @private
   */
  async initRouter(config: HotMeshConfig): Promise<Router> {
    const throttle = await this.store.getThrottleRate(':');

    return new Router(
      {
        namespace: this.namespace,
        appId: this.appId,
        guid: this.guid,
        role: StreamRole.ENGINE,
        reclaimDelay: config.engine.reclaimDelay,
        reclaimCount: config.engine.reclaimCount,
        throttle,
        readonly: config.engine.readonly,
      },
      this.stream,
      this.store,
      this.logger,
    );
  }

  /**
   * resolves the distributed executable version using a delay
   * to allow deployment race conditions to resolve
   * @private
   */
  async fetchAndVerifyVID(vid: AppVID, count = 0): Promise<AppVID> {
    if (isNaN(Number(vid.version))) {
      const app = await this.store.getApp(vid.id, true);
      if (!isNaN(Number(app.version))) {
        if (!this.apps) this.apps = {};
        this.apps[vid.id] = app;
        return { id: vid.id, version: app.version };
      } else if (count < 10) {
        await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
        return await this.fetchAndVerifyVID(vid, count + 1);
      } else {
        this.logger.error('engine-vid-resolution-error', {
          id: vid.id,
          guid: this.guid,
        });
      }
    }
    return vid;
  }

  async getVID(vid?: AppVID): Promise<AppVID> {
    if (this.cacheMode === 'nocache') {
      const app = await this.store.getApp(this.appId, true);
      if (app.version.toString() === this.untilVersion.toString()) {
        //new version is deployed; OK to cache again
        if (!this.apps) this.apps = {};
        this.apps[this.appId] = app;
        this.setCacheMode('cache', app.version.toString());
      }
      return { id: this.appId, version: app.version };
    } else if (!this.apps && vid) {
      this.apps = {};
      this.apps[this.appId] = vid;
      return vid;
    } else {
      return await this.fetchAndVerifyVID({
        id: this.appId,
        version: this.apps?.[this.appId].version,
      });
    }
  }

  /**
   * @private
   */
  setCacheMode(cacheMode: CacheMode, untilVersion: string) {
    this.logger.info(`engine-executable-cache`, {
      mode: cacheMode,
      [cacheMode === 'cache' ? 'target' : 'until']: untilVersion,
    });
    this.cacheMode = cacheMode;
    this.untilVersion = untilVersion;
  }

  /**
   * @private
   */
  async routeToSubscribers(topic: string, message: JobOutput) {
    const jobCallback = this.jobCallbacks[message.metadata.jid];
    if (jobCallback) {
      this.delistJobCallback(message.metadata.jid);
      jobCallback(topic, message);
    }
  }

  /**
   * @private
   */
  async processWebHooks() {
    this.taskService.processWebHooks(this.hook.bind(this));
  }

  /**
   * @private
   */
  async processTimeHooks() {
    this.taskService.processTimeHooks(this.hookTime.bind(this));
  }

  /**
   * @private
   */
  async throttle(delayInMillis: number) {
    try {
      this.router.setThrottle(delayInMillis);
    } catch (e) {
      this.logger.error('engine-throttle-error', { error: e });
    }
  }

  // ************* METADATA/MODEL METHODS *************
  /**
   * @private
   */
  async initActivity(
    topic: string,
    data: JobData = {},
    context?: JobState,
  ): Promise<Await | Cycle | Hook | Signal | Trigger | Worker | Interrupt> {
    const [activityId, schema] = await this.getSchema(topic);
    const ActivityHandler =
      Activities[polyfill.resolveActivityType(schema.type)];
    if (ActivityHandler) {
      const utc = formatISODate(new Date());
      const metadata: ActivityMetadata = {
        aid: activityId,
        atp: schema.type,
        stp: schema.subtype,
        ac: utc,
        au: utc,
      };
      const hook = null;
      return new ActivityHandler(schema, data, metadata, hook, this, context);
    } else {
      throw new Error(`activity type ${schema.type} not found`);
    }
  }
  async getSchema(
    topic: string,
  ): Promise<[activityId: string, schema: ActivityType]> {
    const app = (await this.store.getApp(this.appId)) as AppVID;
    if (!app) {
      throw new Error(`no app found for id ${this.appId}`);
    }
    if (this.isPrivate(topic)) {
      //private subscriptions use the schema id (.activityId)
      const activityId = topic.substring(1);
      const schema = await this.store.getSchema(
        activityId,
        await this.getVID(app),
      );
      return [activityId, schema];
    } else {
      //public subscriptions use a topic (a.b.c) that is associated with a schema id
      const activityId = await this.store.getSubscription(
        topic,
        await this.getVID(app),
      );
      if (activityId) {
        const schema = await this.store.getSchema(
          activityId,
          await this.getVID(app),
        );
        return [activityId, schema];
      }
    }
    throw new Error(
      `no subscription found for topic ${topic} in app ${this.appId} for app version ${app.version}`,
    );
  }
  /**
   * @private
   */
  async getSettings(): Promise<HotMeshSettings> {
    return await this.store.getSettings();
  }
  /**
   * @private
   */
  isPrivate(topic: string) {
    return topic.startsWith('.');
  }

  // ************* COMPILER METHODS *************
  /**
   * @private
   */
  async plan(pathOrYAML: string): Promise<HotMeshManifest> {
    const compiler = new CompilerService(this.store, this.stream, this.logger);
    return await compiler.plan(pathOrYAML);
  }
  /**
   * @private
   */
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    const compiler = new CompilerService(this.store, this.stream, this.logger);
    return await compiler.deploy(pathOrYAML);
  }

  // ************* REPORTER METHODS *************
  /**
   * @private
   */
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    const { id, version } = await this.getVID();
    const reporter = new ReporterService(
      { id, version },
      this.store,
      this.logger,
    );
    const resolvedQuery = await this.resolveQuery(topic, query);
    return await reporter.getStats(resolvedQuery);
  }
  /**
   * @private
   */
  async getIds(
    topic: string,
    query: JobStatsInput,
    queryFacets = [],
  ): Promise<IdsResponse> {
    const { id, version } = await this.getVID();
    const reporter = new ReporterService(
      { id, version },
      this.store,
      this.logger,
    );
    const resolvedQuery = await this.resolveQuery(topic, query);
    return await reporter.getIds(resolvedQuery, queryFacets);
  }
  /**
   * @private
   */
  async resolveQuery(
    topic: string,
    query: JobStatsInput,
  ): Promise<GetStatsOptions> {
    const trigger = (await this.initActivity(topic, query.data)) as Trigger;
    await trigger.getState();
    return {
      end: query.end,
      start: query.start,
      range: query.range,
      granularity: trigger.resolveGranularity(),
      key: trigger.resolveJobKey(trigger.createInputContext()),
      sparse: query.sparse,
    } as GetStatsOptions;
  }

  // ****************** STREAM RE-ENTRY POINT *****************
  /**
   * @private
   */
  async processStreamMessage(streamData: StreamDataResponse): Promise<void> {
    this.logger.debug('engine-process', {
      jid: streamData.metadata.jid,
      gid: streamData.metadata.gid,
      dad: streamData.metadata.dad,
      aid: streamData.metadata.aid,
      status: streamData.status || StreamStatus.SUCCESS,
      code: streamData.code || 200,
      type: streamData.type,
    });
    const context: PartialJobState = {
      metadata: {
        guid: streamData.metadata.guid,
        jid: streamData.metadata.jid,
        gid: streamData.metadata.gid,
        dad: streamData.metadata.dad,
        aid: streamData.metadata.aid,
      },
      data: streamData.data,
    };
    if (streamData.type === StreamDataType.TIMEHOOK) {
      //TIMEHOOK AWAKEN
      const activityHandler = (await this.initActivity(
        `.${streamData.metadata.aid}`,
        context.data,
        context as JobState,
      )) as Hook;
      await activityHandler.processTimeHookEvent(streamData.metadata.jid);
    } else if (streamData.type === StreamDataType.WEBHOOK) {
      //WEBHOOK AWAKEN (SIGNAL IN)
      const activityHandler = (await this.initActivity(
        `.${streamData.metadata.aid}`,
        context.data,
        context as JobState,
      )) as Hook;
      await activityHandler.processWebHookEvent(
        streamData.status,
        streamData.code,
      );
    } else if (streamData.type === StreamDataType.TRANSITION) {
      //TRANSITION (ADJACENT ACTIVITY)
      const activityHandler = (await this.initActivity(
        `.${streamData.metadata.aid}`,
        context.data,
        context as JobState,
      )) as Hook; //todo: `as Activity` (type is more generic)
      await activityHandler.process();
    } else if (streamData.type === StreamDataType.AWAIT) {
      //TRIGGER JOB
      context.metadata = {
        ...context.metadata,
        pj: streamData.metadata.jid,
        pg: streamData.metadata.gid,
        pd: streamData.metadata.dad,
        pa: streamData.metadata.aid,
        px: streamData.metadata.await === false, //sever the parent connection (px)
        trc: streamData.metadata.trc,
        spn: streamData.metadata.spn,
      };
      const activityHandler = (await this.initActivity(
        streamData.metadata.topic,
        streamData.data,
        context as JobState,
      )) as Trigger;
      await activityHandler.process();
    } else if (streamData.type === StreamDataType.RESULT) {
      //AWAIT RESULT
      const activityHandler = (await this.initActivity(
        `.${context.metadata.aid}`,
        streamData.data,
        context as JobState,
      )) as Await;
      await activityHandler.processEvent(streamData.status, streamData.code);
    } else {
      //WORKER RESULT
      const activityHandler = (await this.initActivity(
        `.${streamData.metadata.aid}`,
        streamData.data,
        context as JobState,
      )) as Worker;
      await activityHandler.processEvent(
        streamData.status,
        streamData.code,
        'output',
      );
    }
    this.logger.debug('engine-process-end', {
      jid: streamData.metadata.jid,
      gid: streamData.metadata.gid,
      aid: streamData.metadata.aid,
    });
  }

  // ***************** `AWAIT` ACTIVITY RETURN RESPONSE ****************
  /**
   * @private
   */
  async execAdjacentParent(
    context: JobState,
    jobOutput: JobOutput,
    emit = false,
  ): Promise<string> {
    if (this.hasParentJob(context)) {
      //errors are stringified `StreamError` objects
      const error = this.resolveError(jobOutput.metadata);
      const spn =
        context['$self']?.output?.metadata?.l2s ||
        context['$self']?.output?.metadata?.l1s;
      const streamData: StreamData = {
        metadata: {
          guid: guid(),
          jid: context.metadata.pj,
          gid: context.metadata.pg,
          dad: context.metadata.pd,
          aid: context.metadata.pa,
          trc: context.metadata.trc,
          spn,
        },
        type: StreamDataType.RESULT,
        data: jobOutput.data,
      };
      if (error && error.code) {
        streamData.status = StreamStatus.ERROR;
        streamData.data = error;
        streamData.code = error.code;
        streamData.stack = error.stack;
      } else if (emit) {
        streamData.status = StreamStatus.PENDING;
        streamData.code = HMSH_CODE_PENDING;
      } else {
        streamData.status = StreamStatus.SUCCESS;
        streamData.code = HMSH_CODE_SUCCESS;
      }
      return (await this.router?.publishMessage(null, streamData)) as string;
    }
  }
  /**
   * @private
   */
  hasParentJob(context: JobState, checkSevered = false): boolean {
    if (checkSevered) {
      return Boolean(
        context.metadata.pj && context.metadata.pa && !context.metadata.px,
      );
    }
    return Boolean(context.metadata.pj && context.metadata.pa);
  }
  /**
   * @private
   */
  resolveError(metadata: JobMetadata): StreamError | undefined {
    if (metadata && metadata.err) {
      return JSON.parse(metadata.err) as StreamError;
    }
  }

  // ****************** `INTERRUPT` ACTIVE JOBS *****************
  /**
   * @private
   */
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    //immediately interrupt the job, going directly to the data source
    await this.store.interrupt(topic, jobId, options);

    //now that the job is interrupted, we can clean up
    const context = (await this.getState(topic, jobId)) as JobState;
    const completionOpts: JobCompletionOptions = {
      interrupt: options.descend,
      expire: options.expire,
    };
    return (await this.runJobCompletionTasks(
      context,
      completionOpts,
    )) as string;
  }

  // ****************** `SCRUB` CLEAN COMPLETED JOBS *****************
  /**
   * @private
   */
  async scrub(jobId: string) {
    //todo: do not allow scrubbing of non-existent or actively running job
    await this.store.scrub(jobId);
  }

  // ****************** `HOOK` ACTIVITY RE-ENTRY POINT *****************
  /**
   * @private
   */
  async hook(
    topic: string,
    data: JobData,
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
  ): Promise<string> {
    const hookRule = await this.taskService.getHookRule(topic);
    const [aid] = await this.getSchema(`.${hookRule.to}`);
    const streamData: StreamData = {
      type: StreamDataType.WEBHOOK,
      status,
      code,
      metadata: {
        guid: guid(),
        aid,
        topic,
      },
      data,
    };
    return (await this.router.publishMessage(null, streamData)) as string;
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
    if (type === 'interrupt' || type === 'expire') {
      return await this.interrupt(topicOrActivity, jobId, {
        suppress: true,
        expire: 1,
      });
    }
    const [aid, ...dimensions] = topicOrActivity.split(',');
    const dad = `,${dimensions.join(',')}`;
    const streamData: StreamData = {
      type: StreamDataType.TIMEHOOK,
      metadata: {
        guid: guid(),
        jid: jobId,
        gid: gId,
        dad,
        aid,
      },
      data: { timestamp: Date.now() },
    };
    await this.router.publishMessage(null, streamData);
  }
  /**
   * @private
   */
  async hookAll(
    hookTopic: string,
    data: JobData,
    keyResolver: JobStatsInput,
    queryFacets: string[] = [],
  ): Promise<string[]> {
    const config = await this.getVID();
    const hookRule = await this.taskService.getHookRule(hookTopic);
    if (hookRule) {
      const subscriptionTopic = await getSubscriptionTopic(
        hookRule.to,
        this.store,
        config,
      );
      const resolvedQuery = await this.resolveQuery(
        subscriptionTopic,
        keyResolver,
      );
      const reporter = new ReporterService(config, this.store, this.logger);
      const workItems = await reporter.getWorkItems(resolvedQuery, queryFacets);
      if (workItems.length) {
        const taskService = new TaskService(this.store, this.logger);
        await taskService.enqueueWorkItems(
          workItems.map((workItem) =>
            [
              hookTopic,
              workItem,
              keyResolver.scrub || false,
              JSON.stringify(data),
            ].join(VALSEP),
          ),
        );
        this.subscribe.publish(
          KeyType.QUORUM,
          { type: 'work', originator: this.guid },
          this.appId,
        );
      }
      return workItems;
    } else {
      throw new Error(`unable to find hook rule for topic ${hookTopic}`);
    }
  }

  // ********************** PUB/SUB ENTRY POINT **********************
  /**
   * @private
   */
  async pub(
    topic: string,
    data: JobData,
    context?: JobState,
    extended?: ExtensionType,
  ): Promise<string> {
    const activityHandler = await this.initActivity(topic, data, context);
    if (activityHandler) {
      return await activityHandler.process(extended);
    } else {
      throw new Error(`unable to process activity for topic ${topic}`);
    }
  }
  /**
   * @private
   */
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    const subscriptionCallback: SubscriptionCallback = async (
      topic: string,
      message: { topic: string; job: JobOutput },
    ) => {
      callback(message.topic, message.job);
    };
    return await this.subscribe.subscribe(
      KeyType.QUORUM,
      subscriptionCallback,
      this.appId,
      topic,
    );
  }
  /**
   * @private
   */
  async unsub(topic: string): Promise<void> {
    return await this.subscribe.unsubscribe(KeyType.QUORUM, this.appId, topic);
  }
  /**
   * @private
   */
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    const subscriptionCallback: SubscriptionCallback = async (
      topic: string,
      message: { topic: string; job: JobOutput },
    ) => {
      callback(message.topic, message.job);
    };
    return await this.subscribe.psubscribe(
      KeyType.QUORUM,
      subscriptionCallback,
      this.appId,
      wild,
    );
  }
  /**
   * @private
   */
  async punsub(wild: string): Promise<void> {
    return await this.subscribe.punsubscribe(KeyType.QUORUM, this.appId, wild);
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
    context = {
      metadata: {
        ngn: this.guid,
        trc: context?.metadata?.trc,
        spn: context?.metadata?.spn,
      },
    } as JobState;
    const jobId = await this.pub(topic, data, context);
    return new Promise((resolve, reject) => {
      this.registerJobCallback(jobId, (topic: string, output: JobOutput) => {
        if (output.metadata.err) {
          const error = JSON.parse(output.metadata.err) as StreamError;
          reject({
            ...error,
            job_id: output.metadata.jid,
          });
        } else {
          resolve(output);
        }
      });
      setTimeout(() => {
        //note: job is still active (the subscriber timed out)
        this.delistJobCallback(jobId);
        reject({
          code: HMSH_CODE_TIMEOUT,
          message: 'timeout',
          job_id: jobId,
        } as StreamError);
      }, timeout);
    });
  }
  /**
   * @private
   */
  async pubOneTimeSubs(context: JobState, jobOutput: JobOutput, emit = false) {
    //todo: subscriber should query for the job...only publish minimum context needed
    if (this.hasOneTimeSubscription(context)) {
      const message: JobMessage = {
        type: 'job',
        topic: context.metadata.jid,
        job: restoreHierarchy(jobOutput) as JobOutput,
      };
      this.subscribe.publish(
        KeyType.QUORUM,
        message,
        this.appId,
        context.metadata.ngn,
      );
    }
  }
  /**
   * @private
   */
  async getPublishesTopic(context: JobState): Promise<string> {
    const config = await this.getVID();
    const activityId =
      context.metadata.aid || context['$self']?.output?.metadata?.aid;
    const schema = await this.store.getSchema(activityId, config);
    return schema.publishes;
  }
  /**
   * @private
   */
  async pubPermSubs(context: JobState, jobOutput: JobOutput, emit = false) {
    const topic = await this.getPublishesTopic(context);
    if (topic) {
      const message: JobMessage = {
        type: 'job',
        topic,
        job: restoreHierarchy(jobOutput) as JobOutput,
      };
      this.subscribe.publish(
        KeyType.QUORUM,
        message,
        this.appId,
        `${topic}.${context.metadata.jid}`,
      );
    }
  }
  /**
   * @private
   */
  async add(streamData: StreamData | StreamDataResponse): Promise<string> {
    return (await this.router.publishMessage(null, streamData)) as string;
  }

  /**
   * @private
   */
  registerJobCallback(jobId: string, jobCallback: JobMessageCallback) {
    this.jobCallbacks[jobId] = jobCallback;
  }
  /**
   * @private
   */
  delistJobCallback(jobId: string) {
    delete this.jobCallbacks[jobId];
  }
  /**
   * @private
   */
  hasOneTimeSubscription(context: JobState): boolean {
    return Boolean(context.metadata.ngn);
  }

  // ********** JOB COMPLETION/CLEANUP (AND JOB EMIT) ***********
  /**
   * @private
   */
  async runJobCompletionTasks(
    context: JobState,
    options: JobCompletionOptions = {},
  ): Promise<string | void> {
    //'emit' indicates the job is still active
    const isAwait = this.hasParentJob(context, true);
    const isOneTimeSub = this.hasOneTimeSubscription(context);
    const topic = await this.getPublishesTopic(context);
    let msgId: string;
    if (isAwait || isOneTimeSub || topic) {
      const jobOutput = await this.getState(
        context.metadata.tpc,
        context.metadata.jid,
      );
      msgId = await this.execAdjacentParent(context, jobOutput, options.emit);
      this.pubOneTimeSubs(context, jobOutput, options.emit);
      this.pubPermSubs(context, jobOutput, options.emit);
    }
    if (!options.emit) {
      this.taskService.registerJobForCleanup(
        context.metadata.jid,
        this.resolveExpires(context, options),
        options,
      );
    }
    return msgId;
  }

  /**
   * Job hash expiration is typically reliant on the metadata field
   * if the activity concludes normally. However, if the job is `interrupted`,
   * it will be expired immediately.
   * @private
   */
  resolveExpires(context: JobState, options: JobCompletionOptions): number {
    return options.expire ?? context.metadata.expire ?? HMSH_EXPIRE_JOB_SECONDS;
  }

  // ****** GET JOB STATE/COLLATION STATUS BY ID *********
  /**
   * @private
   */
  async export(jobId: string): Promise<JobExport> {
    return await this.exporter.export(jobId);
  }
  /**
   * @private
   */
  async getRaw(jobId: string): Promise<StringStringType> {
    return await this.store.getRaw(jobId);
  }
  /**
   * @private
   */
  async getStatus(jobId: string): Promise<JobStatus> {
    const { id: appId } = await this.getVID();
    return await this.store.getStatus(jobId, appId);
  }
  /**
   * @private
   */
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    const jobSymbols = await this.store.getSymbols(`$${topic}`);
    const consumes: Consumes = {
      [`$${topic}`]: Object.keys(jobSymbols),
    };
    //job data exists at the 'zero' dimension; pass an empty object
    const dIds = {} as StringStringType;
    const output = await this.store.getState(jobId, consumes, dIds);
    if (!output) {
      throw new Error(`not found ${jobId}`);
    }
    const [state, status] = output;
    const stateTree = restoreHierarchy(state) as JobOutput;
    if (status && stateTree.metadata) {
      stateTree.metadata.js = status;
    }
    return stateTree;
  }
  /**
   * @private
   */
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return await this.store.getQueryState(jobId, fields);
  }

  /**
   * @private
   * @deprecated
   */
  async compress(terms: string[]): Promise<boolean> {
    const existingSymbols = await this.store.getSymbolValues();
    const startIndex = Object.keys(existingSymbols).length;
    const maxIndex = Math.pow(52, 2) - 1;
    const newSymbols = SerializerService.filterSymVals(
      startIndex,
      maxIndex,
      existingSymbols,
      new Set(terms),
    );
    return await this.store.addSymbolValues(newSymbols);
  }
}

export { EngineService };
