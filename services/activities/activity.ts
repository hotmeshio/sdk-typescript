import { HMSH_EXPIRE_DURATION } from '../../modules/enums';
import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import {
  deepCopy,
  formatISODate,
  getValueByPath,
  guid,
  restoreHierarchy,
} from '../../modules/utils';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { ILogger } from '../logger';
import { MapperService } from '../mapper';
import { Pipe } from '../pipe';
import { MDATA_SYMBOLS } from '../serializer';
import { StoreService } from '../store';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
  Consumes,
} from '../../types/activity';
import { JobState, JobStatus } from '../../types/job';
import { MultiResponseFlags, RedisClient, RedisMulti } from '../../types/redis';
import { StringAnyType, StringScalarType } from '../../types/serializer';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamStatus,
} from '../../types/stream';
import { TransitionRule } from '../../types/transition';

/**
 * The base class for all activities
 */
class Activity {
  config: ActivityType;
  data: ActivityData;
  hook: ActivityData;
  metadata: ActivityMetadata;
  store: StoreService<RedisClient, RedisMulti>;
  context: JobState;
  engine: EngineService;
  logger: ILogger;
  status: StreamStatus = StreamStatus.SUCCESS;
  code: StreamCode = 200;
  leg: ActivityLeg;
  adjacencyList: StreamData[];
  adjacentIndex = 0;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState,
  ) {
    this.config = config;
    this.data = data;
    this.metadata = metadata;
    this.hook = hook;
    this.engine = engine;
    this.context = context || ({ data: {}, metadata: {} } as JobState);
    this.logger = engine.logger;
    this.store = engine.store;
  }

  setLeg(leg: ActivityLeg): void {
    this.leg = leg;
  }

  /**
   * A job is assumed to be complete when its status (a semaphore)
   * reaches `0`. A different threshold can be set in the
   * activity YAML, in support of Dynamic Activation Control.
   */
  mapStatusThreshold(): number {
    if (this.config.statusThreshold !== undefined) {
      const threshold = Pipe.resolve(this.config.statusThreshold, this.context);
      if (threshold !== undefined && !isNaN(Number(threshold))) {
        return threshold;
      }
    }
    return 0;
  }

  /**
   * Upon entering leg 1 of a duplexed activity
   */
  async verifyEntry() {
    this.setLeg(1);
    await this.getState();
    const threshold = this.mapStatusThreshold();
    try {
      CollatorService.assertJobActive(
        this.context.metadata.js,
        this.context.metadata.jid,
        this.metadata.aid,
        threshold,
      );
    } catch (error) {
      await CollatorService.notarizeEntry(this);
      if (threshold > 0) {
        if (this.context.metadata.js === threshold) {
          //conclude job EXACTLY ONCE
          const status = await this.setStatus(-threshold);
          if (Number(status) === 0) {
            await this.engine.runJobCompletionTasks(this.context);
          }
        }
      } else {
        throw error;
      }
      return;
    }
    await CollatorService.notarizeEntry(this);
  }

  /**
   * Upon entering leg 2 of a duplexed activity
   */
  async verifyReentry(): Promise<number> {
    const guid = this.context.metadata.guid;
    this.setLeg(2);
    await this.getState();
    CollatorService.assertJobActive(
      this.context.metadata.js,
      this.context.metadata.jid,
      this.metadata.aid,
    );
    return await CollatorService.notarizeReentry(this, guid);
  }

  //********  DUPLEX RE-ENTRY POINT  ********//
  async processEvent(
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
    type: 'hook' | 'output' = 'output',
  ): Promise<void> {
    this.setLeg(2);
    const jid = this.context.metadata.jid;
    if (!jid) {
      this.logger.error('activity-process-event-error', {
        message: 'job id is undefined',
      });
      return;
    }
    const aid = this.metadata.aid;
    this.status = status;
    this.code = code;
    this.logger.debug('activity-process-event', {
      topic: this.config.subtype,
      jid,
      aid,
      status,
      code,
    });
    let telemetry: TelemetryService;

    try {
      const collationKey = await this.verifyReentry();

      this.adjacentIndex = CollatorService.getDimensionalIndex(collationKey);
      telemetry = new TelemetryService(
        this.engine.appId,
        this.config,
        this.metadata,
        this.context,
      );
      telemetry.startActivitySpan(this.leg);
      let multiResponse: MultiResponseFlags;

      if (status === StreamStatus.PENDING) {
        multiResponse = await this.processPending(type);
      } else if (status === StreamStatus.SUCCESS) {
        multiResponse = await this.processSuccess(type);
      } else {
        multiResponse = await this.processError();
      }
      this.transitionAdjacent(multiResponse, telemetry);
    } catch (error) {
      if (error instanceof CollationError) {
        this.logger.info(`process-event-${error.fault}-error`, { ...error });
        return;
      } else if (error instanceof InactiveJobError) {
        this.logger.info('process-event-inactive-job-error', { ...error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { ...error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.info('process-event-get-job-error', { ...error });
        return;
      }
      this.logger.error('activity-process-event-error', {
        ...error,
        message: error.message,
        stack: error.stack,
        name: error.name,
      });
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('activity-process-event-end', { jid, aid });
    }
  }

  async processPending(type: 'hook' | 'output'): Promise<MultiResponseFlags> {
    this.bindActivityData(type);
    this.adjacencyList = await this.filterAdjacent();
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeContinuation(this, multi);

    await this.setStatus(this.adjacencyList.length, multi);
    return (await multi.exec()) as MultiResponseFlags;
  }

  async processSuccess(type: 'hook' | 'output'): Promise<MultiResponseFlags> {
    this.bindActivityData(type);
    this.adjacencyList = await this.filterAdjacent();
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(this.adjacencyList.length - 1, multi);
    return (await multi.exec()) as MultiResponseFlags;
  }

  async processError(): Promise<MultiResponseFlags> {
    this.bindActivityError(this.data);
    this.adjacencyList = await this.filterAdjacent();
    if (!this.adjacencyList.length) {
      this.bindJobError(this.data);
    }
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(this.adjacencyList.length - 1, multi);
    return (await multi.exec()) as MultiResponseFlags;
  }

  async transitionAdjacent(
    multiResponse: MultiResponseFlags,
    telemetry: TelemetryService,
  ): Promise<void> {
    telemetry.mapActivityAttributes();
    const jobStatus = this.resolveStatus(multiResponse);
    const attrs: StringScalarType = { 'app.job.jss': jobStatus };
    //adjacencyList membership has already been set at this point (according to activity status)
    const messageIds = await this.transition(this.adjacencyList, jobStatus);
    if (messageIds?.length) {
      attrs['app.activity.mids'] = messageIds.join(',');
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
    if (this.config.job?.maps) {
      const mapper = new MapperService(
        deepCopy(this.config.job.maps),
        this.context,
      );
      const output = mapper.mapRules();
      if (output) {
        for (const key in output) {
          const f1 = key.indexOf('[');
          //keys with array notation suffix `somekey[]` represent
          //dynamically-keyed mappings whose `value` must be moved to the output.
          //The `value` must be an object with keys appropriate to the
          //notation type: `somekey[0] (array)`, `somekey[-] (mark)`, OR `somekey[_] (search)`
          if (f1 > -1) {
            const amount = key.substring(f1 + 1).split(']')[0];
            if (!isNaN(Number(amount))) {
              const left = key.substring(0, f1);
              output[left] = output[key];
              delete output[key];
            } else if (amount === '-' || amount === '_') {
              const obj = output[key];
              Object.keys(obj).forEach((newKey) => {
                output[newKey] = obj[newKey];
              });
            }
          }
        }
      }
      this.context.data = output;
    }
  }

  mapInputData(): void {
    if (this.config.input?.maps) {
      const mapper = new MapperService(
        deepCopy(this.config.input.maps),
        this.context,
      );
      this.context.data = mapper.mapRules();
    }
  }

  mapOutputData(): void {
    //activity YAML may include output map data that produces/extends activity output data.
    if (this.config.output?.maps) {
      const mapper = new MapperService(
        deepCopy(this.config.output.maps),
        this.context,
      );
      const actOutData = mapper.mapRules();
      const activityId = this.metadata.aid;
      const data = { ...this.context[activityId].output, ...actOutData };
      this.context[activityId].output.data = data;
    }
  }

  async registerTimeout(): Promise<void> {
    //set timeout in support of hook and/or duplex
  }

  /**
   * Any StreamMessage with a status of ERROR is bound to the activity
   */
  bindActivityError(data: Record<string, unknown>): void {
    const md = this.context[this.metadata.aid].output.metadata;
    md.err = JSON.stringify(this.data);
    //(temporary...useful for mapping error parts in the app.yaml)
    md.$error = { ...data, is_stream_error: true };
  }

  /**
   * unhandled activity errors (activities that return an ERROR StreamMessage
   * status and have no adjacent children to transition to) are bound to the job
   */
  bindJobError(data: Record<string, unknown>): void {
    this.context.metadata.err = JSON.stringify({
      ...data,
      is_stream_error: true,
    });
  }

  async getTriggerConfig(): Promise<ActivityType> {
    return await this.store.getSchema(
      this.config.trigger,
      await this.engine.getVID(),
    );
  }

  getJobStatus(): null | number {
    return null;
  }

  async setStatus(amount: number, multi?: RedisMulti): Promise<void | any> {
    const { id: appId } = await this.engine.getVID();
    return await this.store.setStatus(
      amount,
      this.context.metadata.jid,
      appId,
      multi,
    );
  }

  authorizeEntry(state: StringAnyType): string[] {
    //pre-authorize activity state to allow entry for adjacent activities
    return (
      this.adjacencyList?.map((streamData) => {
        const {
          metadata: { aid },
        } = streamData;
        state[`${aid}/output/metadata/as`] = CollatorService.getSeed();
        return aid;
      }) ?? []
    );
  }

  bindDimensionalAddress(state: StringAnyType) {
    const dad = this.resolveDad();
    state[`${this.metadata.aid}/output/metadata/dad`] = dad;
  }

  async setState(multi?: RedisMulti): Promise<string> {
    const jobId = this.context.metadata.jid;
    this.bindJobMetadata();
    this.bindActivityMetadata();
    const state: StringAnyType = {};
    await this.bindJobState(state);
    const presets = this.authorizeEntry(state);
    this.bindDimensionalAddress(state);
    this.bindActivityState(state);
    //symbolNames holds symkeys
    const symbolNames = [
      `$${this.config.subscribes}`,
      this.metadata.aid,
      ...presets,
    ];
    const dIds = CollatorService.getDimensionsById(
      [...this.config.ancestors, this.metadata.aid],
      this.resolveDad(),
    );
    return await this.store.setState(
      state,
      this.getJobStatus(),
      jobId,
      symbolNames,
      dIds,
      multi,
    );
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
    const ts = formatISODate(new Date());
    self.output.metadata.ac = ts;
    self.output.metadata.au = ts;
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
      ...this.bindJobMetadataPaths(),
    ];
    for (const path of PRODUCES) {
      const value = getValueByPath(this.context, path);
      if (value !== undefined) {
        state[path] = value;
      }
    }
    for (const key in this.context?.data ?? {}) {
      if (key.startsWith('-') || key.startsWith('_')) {
        state[key] = this.context.data[key];
      }
    }
    TelemetryService.bindJobTelemetryToState(state, this.config, this.context);
  }

  bindActivityState(state: StringAnyType): void {
    const produces = [
      ...this.config.produces,
      ...this.bindActivityMetadataPaths(),
    ];
    for (const path of produces) {
      const prefixedPath = `${this.metadata.aid}/${path}`;
      const value = getValueByPath(this.context, prefixedPath);
      if (value !== undefined) {
        state[prefixedPath] = value;
      }
    }
    TelemetryService.bindActivityTelemetryToState(
      state,
      this.config,
      this.metadata,
      this.context,
      this.leg,
    );
  }

  bindJobMetadataPaths(): string[] {
    return MDATA_SYMBOLS.JOB_UPDATE.KEYS.map((key) => `metadata/${key}`);
  }

  bindActivityMetadataPaths(): string[] {
    const keys_to_save = this.leg === 1 ? 'ACTIVITY' : 'ACTIVITY_UPDATE';
    return MDATA_SYMBOLS[keys_to_save].KEYS.map(
      (key) => `output/metadata/${key}`,
    );
  }

  async getState() {
    const gid = this.context.metadata.gid;
    const jobSymbolHashName = `$${this.config.subscribes}`;
    const consumes: Consumes = {
      [jobSymbolHashName]: MDATA_SYMBOLS.JOB.KEYS.map(
        (key) => `metadata/${key}`,
      ),
    };
    for (let [activityId, paths] of Object.entries(this.config.consumes)) {
      if (activityId === '$job') {
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
    TelemetryService.addTargetTelemetryPaths(
      consumes,
      this.config,
      this.metadata,
      this.leg,
    );
    const { dad, jid } = this.context.metadata;
    const dIds = CollatorService.getDimensionsById(
      [...this.config.ancestors, this.metadata.aid],
      dad || '',
    );
    //`state` is a unidimensional hash; context is a tree
    const [state, _status] = await this.store.getState(jid, consumes, dIds);
    this.context = restoreHierarchy(state) as JobState;
    this.assertGenerationalId(this.context?.metadata?.gid, gid);
    this.initDimensionalAddress(dad);
    this.initSelf(this.context);
    this.initPolicies(this.context);
  }

  /**
   * if the job is created/deleted/created with the same key,
   * the 'gid' ensures no stale messages (such as sleep delays)
   * enter the workstream. Any message with a mismatched gid
   * belongs to a prior job and can safely be ignored/dropped.
   */
  assertGenerationalId(jobGID: string, msgGID?: string) {
    if (msgGID !== jobGID) {
      throw new GenerationalError(
        jobGID,
        msgGID,
        this.context?.metadata?.jid ?? '',
        this.context?.metadata?.aid ?? '',
        this.context?.metadata?.dad ?? '',
      );
    }
  }

  initDimensionalAddress(dad: string): void {
    this.metadata.dad = dad;
  }

  initSelf(context: StringAnyType): JobState {
    const activityId = this.metadata.aid;
    if (!context[activityId]) {
      context[activityId] = {};
    }
    const self = context[activityId];
    if (!self.output) {
      self.output = {};
    }
    if (!self.input) {
      self.input = {};
    }
    if (!self.hook) {
      self.hook = {};
    }
    if (!self.output.metadata) {
      self.output.metadata = {};
    }
    //prebind the updated timestamp (mappings need the time)
    self.output.metadata.au = formatISODate(new Date());
    context['$self'] = self;
    context['$job'] = context; //NEVER call STRINGIFY! (now circular)
    return context as JobState;
  }

  initPolicies(context: JobState) {
    const expire = Pipe.resolve(
      this.config.expire ?? HMSH_EXPIRE_DURATION,
      context,
    );
    context.metadata.expire = expire;
    if (this.config.persistent != undefined) {
      const persistent = Pipe.resolve(this.config.persistent ?? false, context);
      context.metadata.persistent = persistent;
    }
  }

  bindActivityData(type: 'output' | 'hook'): void {
    this.context[this.metadata.aid][type].data = this.data;
  }

  resolveDad(): string {
    let dad = this.metadata.dad;
    if (this.adjacentIndex > 0) {
      //if adjacent index > 0 the activity is cycling; replace last index with cycle index
      dad = `${dad.substring(0, dad.lastIndexOf(','))},${this.adjacentIndex}`;
    }
    return dad;
  }

  resolveAdjacentDad(): string {
    //concat self and child dimension (all children (leg 1) begin life at 0)
    return `${this.resolveDad()}${CollatorService.getDimensionalSeed(0)}`;
  }

  async filterAdjacent(): Promise<StreamData[]> {
    const adjacencyList: StreamData[] = [];
    const transitions = await this.store.getTransitions(
      await this.engine.getVID(),
    );
    const transition = transitions[`.${this.metadata.aid}`];
    //resolve the dimensional address for adjacent children
    const adjacentDad = this.resolveAdjacentDad();
    if (transition) {
      for (const toActivityId in transition) {
        const transitionRule: boolean | TransitionRule =
          transition[toActivityId];
        if (MapperService.evaluate(transitionRule, this.context, this.code)) {
          adjacencyList.push({
            metadata: {
              guid: guid(),
              jid: this.context.metadata.jid,
              gid: this.context.metadata.gid,
              dad: adjacentDad,
              aid: toActivityId,
              spn: this.context['$self'].output.metadata?.l2s,
              trc: this.context.metadata.trc,
            },
            type: StreamDataType.TRANSITION,
            data: {},
          });
        }
      }
    }
    return adjacencyList;
  }

  isJobComplete(jobStatus: JobStatus): boolean {
    return jobStatus <= 0;
  }

  shouldEmit(): boolean {
    if (this.config.emit) {
      return Pipe.resolve(this.config.emit, this.context) === true;
    }
    return false;
  }

  /**
   * emits the job completed event while leaving the job active, allowing
   * a `main` thread to exit while other threads continue to run.
   * @private
   */
  shouldPersistJob(): boolean {
    if (this.config.persist !== undefined) {
      return Pipe.resolve(this.config.persist, this.context) === true;
    }
    return false;
  }

  async transition(
    adjacencyList: StreamData[],
    jobStatus: JobStatus,
  ): Promise<string[]> {
    if (this.jobWasInterrupted(jobStatus)) {
      return;
    }
    let mIds: string[] = [];

    if (
      this.shouldEmit() ||
      this.isJobComplete(jobStatus) ||
      this.shouldPersistJob()
    ) {
      await this.engine.runJobCompletionTasks(this.context, {
        emit: !this.isJobComplete(jobStatus) && !this.shouldPersistJob(),
      });
    }
    if (adjacencyList.length && !this.isJobComplete(jobStatus)) {
      const multi = this.store.getMulti();
      for (const execSignal of adjacencyList) {
        await this.engine.router?.publishMessage(null, execSignal, multi);
      }
      mIds = (await multi.exec()) as string[];
    }
    return mIds;
  }

  /**
   * A job with a vale < -100_000_000 is considered interrupted,
   * as the interruption event decrements the job status by 1billion.
   */
  jobWasInterrupted(jobStatus: JobStatus): boolean {
    return jobStatus < -100_000_000;
  }
}

export { Activity, ActivityType };
