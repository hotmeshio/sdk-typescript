import { KeyType } from '../../modules/key';
import {
  HMSH_OTT_WAIT_TIME,
  HMSH_CODE_SUCCESS,
  HMSH_CODE_PENDING,
  HMSH_CODE_TIMEOUT, 
  HMSH_EXPIRE_JOB_SECONDS } from '../../modules/enums';
import {
  formatISODate,
  getSubscriptionTopic,
  guid,
  identifyRedisType,
  polyfill,
  restoreHierarchy } from '../../modules/utils';
import Activities from '../activities';
import { Await } from '../activities/await';
import { Cycle } from '../activities/cycle';
import { Hook } from '../activities/hook';
import { Interrupt } from '../activities/interrupt';
import { Signal } from '../activities/signal';
import { Worker } from '../activities/worker';
import { Trigger } from '../activities/trigger';
import { CompilerService } from '../compiler';
import { ILogger } from '../logger';
import { ReporterService } from '../reporter';
import { Router } from '../router';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import { RedisStoreService as RedisStore } from '../store/clients/redis';
import { IORedisStoreService as IORedisStore } from '../store/clients/ioredis';
import { StreamService } from '../stream';
import { RedisStreamService as RedisStream } from '../stream/clients/redis';
import { IORedisStreamService as IORedisStream } from '../stream/clients/ioredis';
import { SubService } from '../sub';
import { IORedisSubService as IORedisSub } from '../sub/clients/ioredis';
import { RedisSubService as RedisSub } from '../sub/clients/redis';
import { TaskService } from '../task';
import { AppVID } from '../../types/app';
import {
  ActivityMetadata,
  ActivityType,
  Consumes } from '../../types/activity';
import { CacheMode } from '../../types/cache';
import { RedisClientType as IORedisClientType } from '../../types/ioredisclient';
import {
  JobState,
  JobData,
  JobMetadata,
  JobOutput,
  PartialJobState,
  JobStatus, 
  JobInterruptOptions,
  JobCompletionOptions } from '../../types/job';
import {
  HotMeshApps,
  HotMeshConfig,
  HotMeshManifest,
  HotMeshSettings } from '../../types/hotmesh';
import { 
  JobMessage,
  JobMessageCallback,
  SubscriptionCallback } from '../../types/quorum';
import { RedisClient, RedisMulti } from '../../types/redis';
import { RedisClientType } from '../../types/redisclient';
import { StringAnyType, StringStringType } from '../../types/serializer';
import {
  GetStatsOptions,
  IdsResponse,
  JobStatsInput,
  StatsResponse
} from '../../types/stats';
import {
  StreamCode,
  StreamData,
  StreamDataResponse,
  StreamDataType,
  StreamError,
  StreamRole,
  StreamStatus } from '../../types/stream';
import { WorkListTaskType } from '../../types/task';

class EngineService {
  namespace: string;
  apps: HotMeshApps | null;
  appId: string;
  guid: string;
  router: Router | null;
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

  static async init(namespace: string, appId: string, guid: string, config: HotMeshConfig, logger: ILogger): Promise<EngineService> {
    if (config.engine) {
      const instance = new EngineService();
      instance.verifyEngineFields(config);

      instance.namespace = namespace;
      instance.appId = appId;
      instance.guid = guid;
      instance.logger = logger;

      await instance.initStoreChannel(config.engine.store);
      await instance.initSubChannel(config.engine.sub);
      await instance.initStreamChannel(config.engine.stream);
      instance.router = instance.initRouter(config);

      instance.router.consumeMessages(
        instance.stream.mintKey(
          KeyType.STREAMS,
          { appId: instance.appId },
        ),
        'ENGINE',
        instance.guid,
        instance.processStreamMessage.bind(instance)
      );

      //the task service is used by the engine to process `webhooks` and `timehooks`
      instance.taskService = new TaskService(instance.store, logger);

      return instance;
    }
  }

  verifyEngineFields(config: HotMeshConfig) {
    if (!identifyRedisType(config.engine.store) || 
      !identifyRedisType(config.engine.stream) ||
      !identifyRedisType(config.engine.sub)) {
      throw new Error('engine config must reference 3 redis client instances');
    }
  }

  async initStoreChannel(store: RedisClient) {
    if (identifyRedisType(store) === 'redis') {
      this.store = new RedisStore(store as RedisClientType);
    } else {
      this.store = new IORedisStore(store as IORedisClientType);
    }
    await this.store.init(
      this.namespace,
      this.appId,
      this.logger
    );
  }

  async initSubChannel(sub: RedisClient) {
    if (identifyRedisType(sub) === 'redis') {
      this.subscribe = new RedisSub(sub as RedisClientType);
    } else {
      this.subscribe = new IORedisSub(sub as IORedisClientType);
    }
    await this.subscribe.init(
      this.namespace,
      this.appId,
      this.guid,
      this.logger
    );
  }

  async initStreamChannel(stream: RedisClient) {
    if (identifyRedisType(stream) === 'redis') {
      this.stream = new RedisStream(stream as RedisClientType);
    } else {
      this.stream = new IORedisStream(stream as IORedisClientType);
    }
    await this.stream.init(
      this.namespace,
      this.appId,
      this.logger
    );
  }

  initRouter(config: HotMeshConfig): Router {
    return new Router(
      {
        namespace: this.namespace,
        appId: this.appId,
        guid: this.guid,
        role: StreamRole.ENGINE,
        reclaimDelay: config.engine.reclaimDelay,
        reclaimCount: config.engine.reclaimCount,
      },
      this.stream,
      this.store,
      this.logger,
    );
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
      return { id: this.appId, version: this.apps?.[this.appId].version };
    }
  }

  setCacheMode(cacheMode: CacheMode, untilVersion: string) {
    this.logger.info(`engine-rule-cache-updated`, { mode: cacheMode, until: untilVersion });
    this.cacheMode = cacheMode;
    this.untilVersion = untilVersion;
  }

  async routeToSubscribers(topic: string, message: JobOutput) {
    const jobCallback = this.jobCallbacks[message.metadata.jid];
    if (jobCallback) {
      this.delistJobCallback(message.metadata.jid);
      jobCallback(topic, message);
    }
  }

  async processWebHooks() {
    this.taskService.processWebHooks((this.hook).bind(this));
  }

  async processTimeHooks() {
    this.taskService.processTimeHooks((this.hookTime).bind(this));
  }

  async throttle(delayInMillis: number) {
    this.router.setThrottle(delayInMillis);
  }

  // ************* METADATA/MODEL METHODS *************
  async initActivity(topic: string, data: JobData = {}, context?: JobState): Promise<Await|Cycle|Hook|Signal|Trigger|Worker|Interrupt> {
    const [activityId, schema] = await this.getSchema(topic);
    const ActivityHandler = Activities[polyfill.resolveActivityType(schema.type)];
    if (ActivityHandler) {
      const utc = formatISODate(new Date());
      const metadata: ActivityMetadata = {
        aid: activityId,
        atp: schema.type,
        stp: schema.subtype,
        ac: utc,
        au: utc
      };
      const hook = null;
      return new ActivityHandler(schema, data, metadata, hook, this, context);
    } else {
      throw new Error(`activity type ${schema.type} not found`);
    }
  }
  async getSchema(topic: string): Promise<[activityId: string, schema: ActivityType]> {
    const app = await this.store.getApp(this.appId) as AppVID;
    if (!app) {
      throw new Error(`no app found for id ${this.appId}`);
    }
    if (this.isPrivate(topic)) {
      //private subscriptions use the schema id (.activityId)
      const activityId = topic.substring(1)
      const schema = await this.store.getSchema(activityId, await this.getVID(app));
      return [activityId, schema];
    } else {
      //public subscriptions use a topic (a.b.c) that is associated with a schema id
      const activityId = await this.store.getSubscription(topic, await this.getVID(app));
      if (activityId) {
        const schema = await this.store.getSchema(activityId, await this.getVID(app));
        return [activityId, schema];
      }
    }
    throw new Error(`no subscription found for topic ${topic} in app ${this.appId} for app version ${app.version}`);
  }
  async getSettings(): Promise<HotMeshSettings> {
    return await this.store.getSettings();
  }
  isPrivate(topic: string) {
    return topic.startsWith('.');
  }

  // ************* COMPILER METHODS *************
  async plan(pathOrYAML: string): Promise<HotMeshManifest> {
    const compiler = new CompilerService(this.store, this.logger);
    return await compiler.plan(pathOrYAML);
  }
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    const compiler = new CompilerService(this.store, this.logger);
    return await compiler.deploy(pathOrYAML);
  }

  // ************* REPORTER METHODS *************
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    const { id, version } = await this.getVID();
    const reporter = new ReporterService({ id, version }, this.store, this.logger);
    const resolvedQuery = await this.resolveQuery(topic, query);
    return await reporter.getStats(resolvedQuery);
  }
  async getIds(topic: string, query: JobStatsInput, queryFacets = []): Promise<IdsResponse> {
    const { id, version } = await this.getVID();
    const reporter = new ReporterService({ id, version }, this.store, this.logger);
    const resolvedQuery = await this.resolveQuery(topic, query);
    return await reporter.getIds(resolvedQuery, queryFacets);
  }
  async resolveQuery(topic: string, query: JobStatsInput): Promise<GetStatsOptions> {
    const trigger = await this.initActivity(topic, query.data) as Trigger;
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
  async processStreamMessage(streamData: StreamDataResponse): Promise<void> {
    this.logger.debug('engine-process-stream-message', {
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
      const activityHandler = await this.initActivity(
        `.${streamData.metadata.aid}`,
        context.data,
        context as JobState,
        ) as Hook;
      await activityHandler.processTimeHookEvent(streamData.metadata.jid);
    } else if (streamData.type === StreamDataType.WEBHOOK) {
      //WEBHOOK AWAKEN (SIGNAL IN)
      const activityHandler = await this.initActivity(
        `.${streamData.metadata.aid}`,
        context.data,
        context as JobState,
        ) as Hook;
      await activityHandler.processWebHookEvent(
        streamData.status,
        streamData.code
      );
    } else if (streamData.type === StreamDataType.TRANSITION) {
      //TRANSITION (ADJACENT ACTIVITY)
      const activityHandler = await this.initActivity(
        `.${streamData.metadata.aid}`,
        context.data,
        context as JobState,
      ) as Hook; //todo: `as Activity` (type is more generic)
      await activityHandler.process();
    } else if (streamData.type === StreamDataType.AWAIT) {
      //TRIGGER JOB
      context.metadata = {
        ...context.metadata,
        pj: streamData.metadata.jid,
        pg: streamData.metadata.gid,
        pd: streamData.metadata.dad,
        pa: streamData.metadata.aid,
        trc: streamData.metadata.trc,
        spn: streamData.metadata.spn,
       };
      const activityHandler = await this.initActivity(
        streamData.metadata.topic,
        streamData.data,
        context as JobState
      ) as Trigger;
      await activityHandler.process();
    } else if (streamData.type === StreamDataType.RESULT) {
      //AWAIT RESULT
      const activityHandler = await this.initActivity(
        `.${context.metadata.aid}`,
        streamData.data,
        context as JobState,
      ) as Await;
      await activityHandler.processEvent(
        streamData.status,
        streamData.code,
      );
    } else {
      //WORKER RESULT
      const activityHandler = await this.initActivity(
        `.${streamData.metadata.aid}`,
        streamData.data,
        context as JobState,
      ) as Worker;
      await activityHandler.processEvent(
        streamData.status,
        streamData.code,
        'output'
      );
    }
    this.logger.debug('engine-process-stream-message-end', {
      jid: streamData.metadata.jid,
      gid: streamData.metadata.gid,
      aid: streamData.metadata.aid
    });
  }

  // ***************** `AWAIT` ACTIVITY RETURN RESPONSE ****************
  async execAdjacentParent(context: JobState, jobOutput: JobOutput, emit = false): Promise<string> {
    if (this.hasParentJob(context)) {
      //errors are stringified `StreamError` objects
      const error = this.resolveError(jobOutput.metadata);
      const spn = context['$self']?.output?.metadata?.l2s || context['$self']?.output?.metadata?.l1s;
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
  hasParentJob(context: JobState): boolean {
    return Boolean(context.metadata.pj && context.metadata.pa);
  }
  resolveError(metadata: JobMetadata): StreamError | undefined {
    if (metadata && metadata.err) {
      return JSON.parse(metadata.err) as StreamError;
    } 
  }

  // ****************** `INTERRUPT` ACTIVE JOBS *****************
  async interrupt(topic: string, jobId: string, options: JobInterruptOptions = {}): Promise<string> {
    await this.store.interrupt(topic, jobId, options);
    const context = await this.getState(topic, jobId) as JobState;
    const completionOpts: JobCompletionOptions = {
      interrupt: options.descend,
      expire: options.expire,
    };
    return await this.runJobCompletionTasks(context, completionOpts) as string;
  }

  // ****************** `SCRUB` CLEAN COMPLETED JOBS *****************
  async scrub(jobId: string) {
    //todo: do not allow scrubbing of non-existent or actively running job
    await this.store.scrub(jobId);
  }

  // ****************** `HOOK` ACTIVITY RE-ENTRY POINT *****************
  async hook(topic: string, data: JobData, status: StreamStatus = StreamStatus.SUCCESS, code: StreamCode = 200): Promise<string> {
    const hookRule = await this.taskService.getHookRule(topic);
    const [aid] = await this.getSchema(`.${hookRule.to}`);
    const streamData: StreamData = {
      type: StreamDataType.WEBHOOK,
      status,
      code,
      metadata: {
        guid: guid(),
        aid,
        topic
      },
      data,
    };
    return await this.router.publishMessage(null, streamData) as string;
  }
  async hookTime(jobId: string, gId: string, topicOrActivity: string, type?: WorkListTaskType): Promise<string | void> {
    if (type === 'interrupt' || type === 'expire') {
      return await this.interrupt(
        topicOrActivity,
        jobId,
        { suppress: true, expire: 1 },
      );
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
  async hookAll(hookTopic: string, data: JobData, keyResolver: JobStatsInput, queryFacets: string[] = []): Promise<string[]> {
    const config = await this.getVID();
    const hookRule = await this.taskService.getHookRule(hookTopic);
    if (hookRule) {
      const subscriptionTopic = await getSubscriptionTopic(hookRule.to, this.store, config)
      const resolvedQuery = await this.resolveQuery(subscriptionTopic, keyResolver);
      const reporter = new ReporterService(config, this.store, this.logger);
      const workItems = await reporter.getWorkItems(resolvedQuery, queryFacets);
      if (workItems.length) {
        const taskService = new TaskService(this.store, this.logger);
        await taskService.enqueueWorkItems(
          workItems.map(
            workItem => `${hookTopic}::${workItem}::${keyResolver.scrub || false}::${JSON.stringify(data)}`
        ));
        this.store.publish(
          KeyType.QUORUM,
          { type: 'work', originator: this.guid },
          this.appId
        );
      }
      return workItems;
    } else {
      throw new Error(`unable to find hook rule for topic ${hookTopic}`);
    }
  }


  // ********************** PUB/SUB ENTRY POINT **********************
  //publish (returns just the job id)
  async pub(topic: string, data: JobData, context?: JobState): Promise<string> {
    const activityHandler = await this.initActivity(topic, data, context);
    if (activityHandler) {
      return await activityHandler.process();
    } else {
      throw new Error(`unable to process activity for topic ${topic}`);
    }
  }
  //subscribe to all jobs for a topic
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    const subscriptionCallback: SubscriptionCallback = async (topic: string, message: {topic: string, job: JobOutput}) => {
      callback(message.topic, message.job);
    };
    return await this.subscribe.subscribe(KeyType.QUORUM, subscriptionCallback, this.appId, topic);
  }
  //unsubscribe to all jobs for a topic
  async unsub(topic: string): Promise<void> {
    return await this.subscribe.unsubscribe(KeyType.QUORUM, this.appId, topic);
  }
  //subscribe to all jobs for a wildcard topic
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    const subscriptionCallback: SubscriptionCallback = async (topic: string, message: {topic: string, job: JobOutput}) => {
      callback(message.topic, message.job);
    };
    return await this.subscribe.psubscribe(KeyType.QUORUM, subscriptionCallback, this.appId, wild);
  }
  //unsubscribe to all jobs for a wildcard topic
  async punsub(wild: string): Promise<void> {
    return await this.subscribe.punsubscribe(KeyType.QUORUM, this.appId, wild);
  }
  //publish and await (returns the job and data (if ready)); throws error with jobid if not
  async pubsub(topic: string, data: JobData, context?: JobState | null, timeout = HMSH_OTT_WAIT_TIME): Promise<JobOutput> {
    context = {
      metadata: {
        ngn: this.guid,
        trc: context?.metadata?.trc,
        spn: context?.metadata?.spn
      }
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
          job_id: jobId
        } as StreamError);
      }, timeout);
    });
  }
  async pubOneTimeSubs(context: JobState, jobOutput: JobOutput, emit = false) {
    //todo: subscriber should query for the job...only publish minimum context needed
    if (this.hasOneTimeSubscription(context)) {
      const message: JobMessage = {
        type: 'job',
        topic: context.metadata.jid,
        job: restoreHierarchy(jobOutput) as JobOutput,
      };
      this.store.publish(KeyType.QUORUM, message, this.appId, context.metadata.ngn);
    }
  }
  async getPublishesTopic(context: JobState): Promise<string> {
    const config = await this.getVID();
    const activityId = context.metadata.aid || context['$self']?.output?.metadata?.aid;
    const schema = await this.store.getSchema(activityId, config);
    return schema.publishes;
  }
  async pubPermSubs(context: JobState, jobOutput: JobOutput, emit = false) {
    const topic = await this.getPublishesTopic(context);
    if (topic) {
      const message: JobMessage = {
        type: 'job',
        topic,
        job: restoreHierarchy(jobOutput) as JobOutput,
      };
      this.store.publish(KeyType.QUORUM, message, this.appId, `${topic}.${context.metadata.jid}`);
    }
  }
  async add(streamData: StreamData|StreamDataResponse): Promise<string> {
    return await this.router.publishMessage(null, streamData) as string;
  }

  registerJobCallback(jobId: string, jobCallback: JobMessageCallback) {
    this.jobCallbacks[jobId] = jobCallback;
  }
  delistJobCallback(jobId: string) {
    delete this.jobCallbacks[jobId];
  }
  hasOneTimeSubscription(context: JobState): boolean {
    return Boolean(context.metadata.ngn);
  }


  // ********** JOB COMPLETION/CLEANUP (AND JOB EMIT) ***********
  async runJobCompletionTasks(context: JobState, options: JobCompletionOptions = {}): Promise<string | void> {
    //'emit' indicates the job is still active
    const isAwait = this.hasParentJob(context);
    const isOneTimeSub = this.hasOneTimeSubscription(context);
    const topic = await this.getPublishesTopic(context);
    let msgId: string;
    if (isAwait || isOneTimeSub || topic) {
      const jobOutput = await this.getState(
        context.metadata.tpc,
        context.metadata.jid,
      );
      msgId = await this.execAdjacentParent(
        context,
        jobOutput,
        options.emit,
       );
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
   */
  resolveExpires(context: JobState, options: JobCompletionOptions): number {
    return options.expire ?? context.metadata.expire ?? HMSH_EXPIRE_JOB_SECONDS;
  }


  // ****** GET JOB STATE/COLLATION STATUS BY ID *********
  async getStatus(jobId: string): Promise<JobStatus> {
    const { id: appId } = await this.getVID();
    return this.store.getStatus(jobId, appId);
  }
  //todo: add 'options' parameter;
  //      (e.g, if {dimensions:true}, use hscan to deliver
  //      the full set of dimensional job data)
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    const jobSymbols = await this.store.getSymbols(`$${topic}`);
    const consumes: Consumes = {
      [`$${topic}`]: Object.keys(jobSymbols)
    }
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
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return await this.store.getQueryState(jobId, fields);
  }

  async compress(terms: string[]): Promise<boolean> {
    const existingSymbols = await this.store.getSymbolValues();
    const startIndex = Object.keys(existingSymbols).length;
    const maxIndex = Math.pow(52, 2) - 1;
    const newSymbols = SerializerService.filterSymVals(startIndex, maxIndex, existingSymbols, new Set(terms));
    return await this.store.addSymbolValues(newSymbols);
  }
}

export { EngineService };
