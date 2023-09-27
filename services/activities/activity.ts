import { GetStateError } from '../../modules/errors';
import {
  formatISODate,
  getValueByPath,
  restoreHierarchy } from '../../modules/utils';
import { CollatorService } from '../collator';
import { DimensionService } from '../dimension';
import { EngineService } from '../engine';
import { ILogger } from '../logger';
import { MapperService } from '../mapper';
import { Pipe } from '../pipe';
import { MDATA_SYMBOLS } from '../serializer';
import { StoreSignaler } from '../signaler/store';
import { StoreService } from '../store';
import { TelemetryService } from '../telemetry';
import { 
  ActivityData,
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
  Consumes } from '../../types/activity';
import { JobState, JobStatus } from '../../types/job';
import {
  MultiResponseFlags,
  RedisClient,
  RedisMulti } from '../../types/redis';
import { StringAnyType, StringScalarType } from '../../types/serializer';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamStatus } from '../../types/stream';
import { TransitionRule } from '../../types/transition';

/**
 * The base class for all activities
 */
class Activity {
  config: ActivityType;
  data: ActivityData;
  hook: ActivityData;
  metadata: ActivityMetadata;
  store: StoreService<RedisClient, RedisMulti>
  context: JobState;
  engine: EngineService;
  logger: ILogger;
  status: StreamStatus = StreamStatus.SUCCESS;
  code: StreamCode = 200;
  leg: ActivityLeg;
  adjacencyList: StreamData[];
  adjacentIndex = 0; //can be updated by leg2 using 'as' metadata hincrby output

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState) {
      this.config = config;
      this.data = data;
      this.metadata = metadata;
      this.hook = hook;
      this.engine = engine;
      this.context = context || { data: {}, metadata: {} } as JobState;
      this.logger = engine.logger;
      this.store = engine.store;
  }

  //********  INITIAL ENTRY POINT (A)  ********//
  async process(): Promise<string> {
    this.logger.debug('activity-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    let telemetry: TelemetryService;
    try {
      this.setLeg(1);
      await CollatorService.notarizeEntry(this);

      await this.getState();
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
      let multiResponse: MultiResponseFlags;

      const multi = this.store.getMulti();
      if (this.doesHook()) {
        //sleep and wait to awaken upon a signal
        await this.registerHook(multi);
        this.mapJobData();
        await this.setState(multi);
        await CollatorService.authorizeReentry(this, multi);

        await this.setStatus(0, multi);
        await multi.exec();
        telemetry.mapActivityAttributes();
      } else {
        //end the activity and transition to its children
        this.adjacencyList = await this.filterAdjacent();
        this.mapJobData();
        await this.setState(multi);
        await CollatorService.notarizeEarlyCompletion(this, multi);

        await this.setStatus(this.adjacencyList.length - 1, multi);
        multiResponse = await multi.exec() as MultiResponseFlags;
        telemetry.mapActivityAttributes();
        const jobStatus = this.resolveStatus(multiResponse);
        const attrs: StringScalarType = { 'app.job.jss': jobStatus };
        const messageIds = await this.transition(this.adjacencyList, jobStatus);
        if (messageIds.length) {
          attrs['app.activity.mids'] = messageIds.join(',')
        }
        telemetry.setActivityAttributes(attrs);
      }

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof GetStateError) {
        this.logger.error('activity-get-state-error', error);
      } else {
        this.logger.error('activity-process-error', error);
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('activity-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }

  setLeg(leg: ActivityLeg): void {
    this.leg = leg;
  }

  //********  SIGNALER RE-ENTRY POINT (B)  ********//
  doesHook(): boolean {
    return !!(this.config.hook?.topic || this.config.sleep);
  }

  async registerHook(multi?: RedisMulti): Promise<string | void> {
    if (this.config.hook?.topic) {
      const signaler = new StoreSignaler(this.store, this.logger);
      return await signaler.registerWebHook(this.config.hook.topic, this.context, multi);
    } else if (this.config.sleep) {
      const durationInSeconds = Pipe.resolve(this.config.sleep, this.context);
      const jobId = this.context.metadata.jid;
      const activityId = this.metadata.aid;
      await this.engine.task.registerTimeHook(jobId, activityId, 'sleep', durationInSeconds);
      return jobId;
    }
  }

  async processWebHookEvent(): Promise<JobStatus | void> {
    this.logger.debug('engine-process-web-hook-event', {
      topic: this.config.hook.topic,
      aid: this.metadata.aid
    });
    const signaler = new StoreSignaler(this.store, this.logger);
    const data = { ...this.data };
    const jobId = await signaler.processWebHookSignal(this.config.hook.topic, data);
    if (jobId) {
      await this.processHookEvent(jobId);
      await signaler.deleteWebHookSignal(this.config.hook.topic, data);
    } //else => already resolved
  }

  async processTimeHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('engine-process-time-hook-event', {
      jid: jobId,
      aid: this.metadata.aid
    });
    return await this.processHookEvent(jobId);
  }

  //todo: hooks are currently singletons. but they can support
  //      dimensional threads like `await` and `worker` do.
  //      Copy code from those activities to support cyclical
  //      timehook and eventhook inputs by adding a 'pending'
  //      flag to hooks that allows for repeated signals
  async processHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('activity-process-hook-event', { jobId });
    let telemetry: TelemetryService;
    try {
      this.setLeg(2);
      await this.getState(jobId);
      const aState = await CollatorService.notarizeReentry(this);
      this.adjacentIndex = CollatorService.getDimensionalIndex(aState);

      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
      this.bindActivityData('hook');
      this.mapJobData();
      this.adjacencyList = await this.filterAdjacent();

      const multi = this.engine.store.getMulti();
      await this.setState(multi);
      await CollatorService.notarizeCompletion(this, multi);

      await this.setStatus(this.adjacencyList.length - 1, multi);
      const multiResponse = await multi.exec() as MultiResponseFlags;

      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);
      const attrs: StringScalarType = { 'app.job.jss': jobStatus };
      const messageIds = await this.transition(this.adjacencyList, jobStatus);
      if (messageIds.length) {
        attrs['app.activity.mids'] = messageIds.join(',')
      }
      telemetry.setActivityAttributes(attrs);
      return jobStatus as number;
    } catch (error) {
      this.logger.error('engine-process-hook-event-error', error);
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
    }
  }

  resolveStatus(multiResponse: MultiResponseFlags): number {
    const activityStatus = multiResponse[multiResponse.length - 1];
    if (Array.isArray(activityStatus)) {
      return Number(activityStatus[1]);
    } else {
      return Number(activityStatus);
    }
  }

  mapJobData(): void {
    if(this.config.job?.maps) {
      const mapper = new MapperService(this.config.job.maps, this.context);
      this.context.data = mapper.mapRules();
    }
  }

  mapInputData(): void {
    if(this.config.input?.maps) {
      const mapper = new MapperService(this.config.input.maps, this.context);
      this.context.data = mapper.mapRules();
    }
  }

  async registerTimeout(): Promise<void> {
    //set timeout in support of hook and/or duplex
  }

  bindActivityError(data: Record<string, unknown>): void {
    //todo: map activity error data into the job error (if defined)
    //      map job status via: (500: [3**, 4**, 5**], 202: [$pending])
    this.context.metadata.err = JSON.stringify(data);
  }

  async getTriggerConfig(): Promise<ActivityType> {
    return await this.store.getSchema(
      this.config.trigger,
      await this.engine.getVID()
    );
  }

  getJobStatus(): null | number {
    return null;
  }

  async setStatus(amount: number, multi?: RedisMulti): Promise<void> {
    const { id: appId } = await this.engine.getVID();
    await this.store.setStatus(
      amount,
      this.context.metadata.jid,
      appId,
      multi
    );
  }

  authorizeEntry(state: StringAnyType): string[] {
    //pre-authorize activity state to allow entry for adjacent activities
    return this.adjacencyList?.map((streamData) => {
      const { metadata: { aid } } = streamData;
      state[`${aid}/output/metadata/as`] = CollatorService.getSeed();
      return aid;
    }) ?? [];
  }

  bindDimensionalAddress(state: StringAnyType) {
    const { aid, dad } = this.metadata;
    state[`${aid}/output/metadata/dad`] = dad;
  }

  async setState(multi?: RedisMulti): Promise<string> {
    const { id: appId } = await this.engine.getVID();
    const jobId = this.context.metadata.jid;
    this.bindJobMetadata();
    this.bindActivityMetadata();
    let state: StringAnyType = {};
    await this.bindJobState(state);
    const presets = this.authorizeEntry(state);
    this.bindDimensionalAddress(state);
    this.bindActivityState(state);
    //symbolNames holds symkeys
    const symbolNames = [
      `$${this.config.subscribes}`,
      this.metadata.aid,
      ...presets
    ];
    return await this.store.setState(state, this.getJobStatus(), jobId, appId, symbolNames, multi);
  }

  bindJobMetadata(): void {
    //both legs of the most recently run activity (1 and 2) modify ju (job_updated)
    this.context.metadata.ju = formatISODate(new Date());
  }

  bindActivityMetadata(): void {
    const self: StringAnyType = this.context['$self'];
    if (!self.output.metadata) {
      self.output.metadata = {};
    }
    if (this.status === StreamStatus.ERROR) {
      self.output.metadata.err = JSON.stringify(this.data);
    }
    self.output.metadata.ac = 
      self.output.metadata.au = formatISODate(new Date());
    self.output.metadata.atp = this.config.type;
    if (this.config.subtype) {
      self.output.metadata.stp = this.config.subtype;
    }
    self.output.metadata.aid = this.metadata.aid;
  }

  async bindJobState(state: StringAnyType): Promise<void> {
    const triggerConfig = await this.getTriggerConfig();
    const PRODUCES = [
      ...(triggerConfig.PRODUCES || []),
      ...this.bindJobMetadataPaths()
    ];
    for (const path of PRODUCES) {
      const value = getValueByPath(this.context, path);
      if (value !== undefined) {
        state[path] = value;
      }
    }
    TelemetryService.bindJobTelemetryToState(state, this.config, this.context);
  }

  bindActivityState(state: StringAnyType,): void {
    const produces = [
      ...this.config.produces,
      ...this.bindActivityMetadataPaths()
    ];
    for (const path of produces) {
      const prefixedPath = `${this.metadata.aid}/${path}`;
      const value = getValueByPath(this.context, prefixedPath);
      if (value !== undefined) {
        state[prefixedPath] = value;
      } 
    }
    TelemetryService.bindActivityTelemetryToState(state, this.config, this.metadata, this.context, this.leg);
  }

  bindJobMetadataPaths(): string[] {
    return MDATA_SYMBOLS.JOB_UPDATE.KEYS.map((key) => `metadata/${key}`);
  }

  bindActivityMetadataPaths(): string[] {
    const keys_to_save = this.leg === 1 ? 'ACTIVITY': 'ACTIVITY_UPDATE'
    return MDATA_SYMBOLS[keys_to_save].KEYS.map((key) => `output/metadata/${key}`);
  }

  async getState(jobId?: string) {
    //assemble list of paths necessary to create 'job state' from the 'symbol hash'
    const jobSymbolHashName = `$${this.config.subscribes}`;
    const consumes: Consumes = {
      [jobSymbolHashName]: MDATA_SYMBOLS.JOB.KEYS.map((key) => `metadata/${key}`)
    };
    for (let [activityId, paths] of Object.entries(this.config.consumes)) {
       if(activityId === '$job') {
        for (const path of paths) {
          consumes[jobSymbolHashName].push(path);
        }
      } else {
        if (activityId === '$self') {
          activityId = this.metadata.aid;
        }
        if (!consumes[activityId]) {
          consumes[activityId] = [];
        }
        for (const path of paths) {
          consumes[activityId].push(`${activityId}/${path}`);
        }
      }
    }
    TelemetryService.addTargetTelemetryPaths(consumes, this.config, this.metadata, this.leg);
    const { dad, jid } = this.context.metadata;
    jobId = jobId || jid;
    //`state` is a flat hash
    const [state, status] = await this.store.getState(jobId, consumes);
    //`context` is a tree
    this.context = restoreHierarchy(state) as JobState;
    this.initDimensionalAddress(dad);
    this.initSelf(this.context);
    this.initPolicies(this.context);
  }

  initDimensionalAddress(dad: string): void {
    this.metadata.dad = dad;
  }

  initSelf(context: StringAnyType): JobState {
    const activityId = this.metadata.aid;
    if (!context[activityId]) {
      context[activityId] = { };
    }
    const self = context[activityId];
    if (!self.output) {
      self.output = { };
    }
    if (!self.input) {
      self.input = { };
    }
    if (!self.hook) {
      self.hook = { };
    }
    context['$self'] = self;
    context['$job'] = context; //NEVER call STRINGIFY! (circular)
    return context as JobState;
  }

  initPolicies(context: JobState) {
    context.metadata.expire = this.config.expire;
  }

  bindActivityData(type: 'output' | 'hook'): void {
    this.context[this.metadata.aid][type].data = this.data;
  }

  async filterAdjacent(): Promise<StreamData[]> {
    const adjacencyList: StreamData[] = [];
    const transitions = await this.store.getTransitions(await this.engine.getVID());
    const transition = transitions[`.${this.metadata.aid}`];
    const adjacentSuffix = DimensionService.getSeed(this.adjacentIndex);
    if (transition) {
      for (const toActivityId in transition) {
        const transitionRule: boolean | TransitionRule = transition[toActivityId];
        if (MapperService.evaluate(transitionRule, this.context, this.code)) {
          adjacencyList.push({
            metadata: {
              jid: this.context.metadata.jid,
              dad: `${this.metadata.dad}${adjacentSuffix}`,
              aid: toActivityId,
              spn: this.context['$self'].output.metadata?.l2s,
              trc: this.context.metadata.trc,
            },
            type: StreamDataType.TRANSITION,
            data: {}
          });
        }
      }
    }
    return adjacencyList;
  }

  async transition(adjacencyList: StreamData[], jobStatus: JobStatus): Promise<string[]> {
    let mIds: string[] = [];
    if (adjacencyList.length) {
      const multi = this.store.getMulti();
      for (const execSignal of adjacencyList) {
        await this.engine.streamSignaler?.publishMessage(null, execSignal, multi);
      }
      mIds = (await multi.exec()) as string[];
    } else if (jobStatus <= 0) {
      await this.engine.runJobCompletionTasks(this.context);
    }
    return mIds;
  }
}

export { Activity, ActivityType };
