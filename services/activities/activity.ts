import { CollationError, GetStateError } from '../../modules/errors';
import {
  formatISODate,
  getValueByPath,
  restoreHierarchy } from '../../modules/utils';
import { CollatorService } from '../collator';
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
import { HookRule } from '../../types/hook';

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
        this.mapOutputData();
        this.mapJobData();
        await this.setState(multi);
        await CollatorService.authorizeReentry(this, multi);

        await this.setStatus(0, multi);
        await multi.exec();
        telemetry.mapActivityAttributes();
      } else {
        //end the activity and transition to its children
        this.adjacencyList = await this.filterAdjacent();
        this.mapOutputData();
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
        this.logger.error('activity-get-state-error', { error });
      } else {
        this.logger.error('activity-process-error', { error });
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

  //********  SIGNAL RE-ENTRY POINT  ********//
  doesHook(): boolean {
    return !!(this.config.hook?.topic || this.config.sleep);
  }

  async getHookRule(topic: string): Promise<HookRule | undefined> {
    const rules = await this.store.getHookRules();
    return rules?.[topic]?.[0] as HookRule;
  }

  async registerHook(multi?: RedisMulti): Promise<string | void> {
    if (this.config.hook?.topic) {
      const signaler = new StoreSignaler(this.store, this.logger);
      return await signaler.registerWebHook(this.config.hook.topic, this.context, multi);
    } else if (this.config.sleep) {
      const durationInSeconds = Pipe.resolve(this.config.sleep, this.context);
      const jobId = this.context.metadata.jid;
      const activityId = this.metadata.aid;
      const dId = this.metadata.dad;
      await this.engine.task.registerTimeHook(jobId, `${activityId}${dId||''}`, 'sleep', durationInSeconds);
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

  async processHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('activity-process-hook-event', { jobId });
    let telemetry: TelemetryService;
    try {
      this.setLeg(2);
      await this.getState(jobId);
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
      const aState = await CollatorService.notarizeReentry(this);
      this.adjacentIndex = CollatorService.getDimensionalIndex(aState);

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
      if (error instanceof CollationError && error.fault === 'inactive') {
        this.logger.info('process-hook-event-inactive-error', { error });
        return;
      }
      this.logger.error('engine-process-hook-event-error', { error });
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
    }
  }

  //********  DUPLEX RE-ENTRY POINT  ********//
  async processEvent(status: StreamStatus = StreamStatus.SUCCESS, code: StreamCode = 200): Promise<void> {
    this.setLeg(2);
    const jid = this.context.metadata.jid;
    const aid = this.metadata.aid;
    this.status = status;
    this.code = code;
    this.logger.debug('activity-process-event', { topic: this.config.subtype, jid, aid, status, code });
    let telemetry: TelemetryService;
    try {
      await this.getState();
      const aState = await CollatorService.notarizeReentry(this);
      this.adjacentIndex = CollatorService.getDimensionalIndex(aState);

      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      let isComplete = CollatorService.isActivityComplete(this.context.metadata.js);

      if (isComplete) {
        this.logger.warn('activity-process-event-duplicate', { jid, aid });
        this.logger.debug('activity-process-event-duplicate-resolution', { resolution: 'Increase HotMesh config `reclaimDelay` timeout.' });
        return;
      }

      telemetry.startActivitySpan(this.leg);
      let multiResponse: MultiResponseFlags;
      if (status === StreamStatus.PENDING) {
        multiResponse = await this.processPending(telemetry);
      } else if (status === StreamStatus.SUCCESS) {
        multiResponse = await this.processSuccess(telemetry);
      } else {
        multiResponse = await this.processError(telemetry);
      }
      this.transitionAdjacent(multiResponse, telemetry);
    } catch (error) {
      if (error instanceof CollationError && error.fault === 'inactive') {
        this.logger.info('process-event-inactive-error', { error });
        return;
      }
      this.logger.error('activity-process-event-error', { error });
      telemetry && telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry && telemetry.endActivitySpan();
      this.logger.debug('activity-process-event-end', { jid, aid });
    }
  }

  async processPending(telemetry: TelemetryService): Promise<MultiResponseFlags> {
    this.bindActivityData('output');
    this.adjacencyList = await this.filterAdjacent();
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeContinuation(this, multi);

    await this.setStatus(this.adjacencyList.length, multi);
    return await multi.exec() as MultiResponseFlags;
  }

  async processSuccess(telemetry: TelemetryService): Promise<MultiResponseFlags> {
    this.bindActivityData('output');
    this.adjacencyList = await this.filterAdjacent();
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(this.adjacencyList.length - 1, multi);
    return await multi.exec() as MultiResponseFlags;
  }

  async processError(telemetry: TelemetryService): Promise<MultiResponseFlags> {
    this.bindActivityError(this.data);
    this.adjacencyList = await this.filterAdjacent();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(this.adjacencyList.length - 1, multi);
    return await multi.exec() as MultiResponseFlags;
  }

  async transitionAdjacent(multiResponse: MultiResponseFlags, telemetry: TelemetryService): Promise<void> {
    telemetry.mapActivityAttributes();
    const jobStatus = this.resolveStatus(multiResponse);
    const attrs: StringScalarType = { 'app.job.jss': jobStatus };
    const messageIds = await this.transition(this.adjacencyList, jobStatus);
    if (messageIds.length) {
      attrs['app.activity.mids'] = messageIds.join(',')
    }
    telemetry.setActivityAttributes(attrs);
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

  mapOutputData(): void {
    //activity YAML may include output map data that produces/extends activity output data.
    if(this.config.output?.maps) {
      const mapper = new MapperService(this.config.output.maps, this.context);
      const actOutData = mapper.mapRules();
      const activityId = this.metadata.aid;
      const data = { ...this.context[activityId].output, ...actOutData };
      this.context[activityId].output.data = data;
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
    const dad = this.resolveDad();
    state[`${this.metadata.aid}/output/metadata/dad`] = dad;
  }

  async setState(multi?: RedisMulti): Promise<string> {
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
    const dIds = CollatorService.getDimensionsById([...this.config.ancestors, this.metadata.aid], this.resolveDad());
    return await this.store.setState(state, this.getJobStatus(), jobId, symbolNames, dIds, multi);
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
    //todo: verify leg2 never overwrites leg1 `ac`
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
    let { dad, jid } = this.context.metadata;
    jobId = jobId || jid;
    const dIds = CollatorService.getDimensionsById([...this.config.ancestors, this.metadata.aid], dad);
    //`state` is a flat hash; context is a tree
    const [state, status] = await this.store.getState(jobId, consumes, dIds);
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

  resolveDad(): string {
    let dad = this.metadata.dad;
    if (this.adjacentIndex > 0) {
      //if adjacent index > 0 the activity is cycling; replace last index with cycle index
      dad = `${dad.substring(0, dad.lastIndexOf(','))},${this.adjacentIndex}`
    }
    return dad;
  }

  resolveAdjacentDad(): string {
    //concat self and child dimension (all children (leg 1) begin life at 0)
    return `${this.resolveDad()}${CollatorService.getDimensionalSeed(0)}`;
  };

  async filterAdjacent(): Promise<StreamData[]> {
    const adjacencyList: StreamData[] = [];
    const transitions = await this.store.getTransitions(await this.engine.getVID());
    const transition = transitions[`.${this.metadata.aid}`];
    //resolve the dimensional address for adjacent children
    const adjacentDad = this.resolveAdjacentDad();
    if (transition) {
      for (const toActivityId in transition) {
        const transitionRule: boolean | TransitionRule = transition[toActivityId];
        if (MapperService.evaluate(transitionRule, this.context, this.code)) {
          adjacencyList.push({
            metadata: {
              jid: this.context.metadata.jid,
              dad: adjacentDad,
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
     if (jobStatus <= 0 || this.config.emit) {
      //activity should not send 'emit' if the job is truly over
      const isTrueEmit = jobStatus > 0;
      await this.engine.runJobCompletionTasks(this.context, isTrueEmit);
    }

    if (adjacencyList.length && jobStatus > 0) {
      const multi = this.store.getMulti();
      for (const execSignal of adjacencyList) {
        await this.engine.streamSignaler?.publishMessage(null, execSignal, multi);
      }
      mIds = (await multi.exec()) as string[];
    }
    return mIds;
  }
}

export { Activity, ActivityType };
