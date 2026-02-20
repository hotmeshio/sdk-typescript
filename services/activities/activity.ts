import {
  HMSH_CODE_MEMFLOW_MAXED,
  HMSH_EXPIRE_DURATION,
} from '../../modules/enums';
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
import {
  ProviderClient,
  ProviderTransaction,
  TransactionResultList,
} from '../../types/provider';
import { JobState, JobStatus } from '../../types/job';
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
  store: StoreService<ProviderClient, ProviderTransaction>;
  context: JobState;
  engine: EngineService;
  logger: ILogger;
  status: StreamStatus = StreamStatus.SUCCESS;
  code: StreamCode = 200;
  leg: ActivityLeg;
  adjacencyList: StreamData[];
  adjacentIndex = 0;
  guidLedger = 0;

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
        if (this.context.metadata.js <= threshold) {
          //Dynamic Activation Control: convergent claim — only the
          //activity whose HINCRBY reaches exactly 0 runs completion.
          const status = await this.setStatus(-threshold);
          if (Number(status) === 0) {
            const txn = this.store.transact();
            await this.engine.runJobCompletionTasks(this.context, {}, txn);
            await txn.exec();
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
   * Upon entering leg 2 of a duplexed activity.
   * Increments both the activity ledger (+1) and GUID ledger (+1).
   * Stores the GUID ledger value for step-level resume decisions.
   */
  async verifyReentry(): Promise<number> {
    const msgGuid = this.context.metadata.guid;
    this.setLeg(2);
    await this.getState();
    this.context.metadata.guid = msgGuid;
    CollatorService.assertJobActive(
      this.context.metadata.js,
      this.context.metadata.jid,
      this.metadata.aid,
    );
    const [activityLedger, guidLedger] =
      await CollatorService.notarizeLeg2Entry(this, msgGuid);
    this.guidLedger = guidLedger;
    return activityLedger;
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

      //bind data per status type
      if (status === StreamStatus.ERROR) {
        this.bindActivityError(this.data);
        this.adjacencyList = await this.filterAdjacent();
        if (!this.adjacencyList.length) {
          this.bindJobError(this.data);
        }
      } else {
        this.bindActivityData(type);
        this.adjacencyList = await this.filterAdjacent();
      }
      this.mapJobData();

      //When an unrecoverable error has no matching transitions
      //(e.g., code 500 from raw errors after retries exhausted),
      //mark the job as terminally errored so the step protocol
      //can force completion via the isErrorTerminal path.
      if (status === StreamStatus.ERROR && !this.adjacencyList?.length) {
        if (!this.context.data) this.context.data = {};
        this.context.data.done = true;
        this.context.data.$error = {
          message: this.data?.message || 'unknown error',
          code: HMSH_CODE_MEMFLOW_MAXED,
          stack: this.data?.stack,
        };
      }

      //determine step parameters
      const delta =
        status === StreamStatus.PENDING
          ? this.adjacencyList.length
          : this.adjacencyList.length - 1;
      const shouldFinalize = status !== StreamStatus.PENDING;

      //execute 3-step protocol
      const thresholdHit = await this.executeStepProtocol(
        delta,
        shouldFinalize,
      );

      //telemetry
      telemetry.mapActivityAttributes();
      telemetry.setActivityAttributes({});
    } catch (error) {
      if (error instanceof CollationError) {
        this.logger.info(`process-event-${error.fault}-error`, { error });
        return;
      } else if (error instanceof InactiveJobError) {
        this.logger.info('process-event-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.info('process-event-get-job-error', { error });
        return;
      }
      this.logger.error('activity-process-event-error', {
        error,
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

  /**
   * Executes the 3-step Leg2 protocol using GUID ledger for
   * crash-safe resume. Each step bundles durable writes with
   * its concluding digit update in a single transaction.
   *
   * @returns true if this transition caused the job to complete
   */
  async executeStepProtocol(
    delta: number,
    shouldFinalize: boolean,
  ): Promise<boolean> {
    const msgGuid = this.context.metadata.guid;
    const threshold = this.mapStatusThreshold();
    const { id: appId } = await this.engine.getVID();

    //Step 1: Save work (skip if GUID 10B already set)
    if (!CollatorService.isGuidStep1Done(this.guidLedger)) {
      const txn1 = this.store.transact();
      await this.setState(txn1);
      await CollatorService.notarizeStep1(this, msgGuid, txn1);
      await txn1.exec();
    }

    //Step 2: Spawn children + semaphore + edge capture (skip if GUID 1B already set)
    let thresholdHit = false;
    if (!CollatorService.isGuidStep2Done(this.guidLedger)) {
      const txn2 = this.store.transact();
      //queue step markers first
      await CollatorService.notarizeStep2(this, msgGuid, txn2);
      //queue child publications
      for (const child of this.adjacencyList) {
        await this.engine.router?.publishMessage(null, child, txn2);
      }
      //queue semaphore update + edge capture LAST (so result is at end)
      await this.store.setStatusAndCollateGuid(
        delta,
        threshold,
        this.context.metadata.jid,
        appId,
        msgGuid,
        CollatorService.WEIGHTS.GUID_SNAPSHOT,
        txn2,
      );
      const results = (await txn2.exec()) as TransactionResultList;
      thresholdHit = this.resolveThresholdHit(results);
      this.logger.debug('step-protocol-step2-complete', {
        jid: this.context.metadata.jid,
        aid: this.metadata.aid,
        delta,
        threshold,
        thresholdHit,
        lastResult: results[results.length - 1],
        resultCount: results.length,
      });
    } else {
      //Step 2 already done; check GUID snapshot for edge
      thresholdHit = CollatorService.isGuidJobClosed(this.guidLedger);
    }

    //Step 3: Job completion tasks (edge hit OR emit/persist, skip if GUID 100M already set)
    //When an activity marks the job done with an unrecoverable error
    //(e.g., stopper after max retries), force completion even when the
    //semaphore threshold isn't hit (the signaler's +1 contribution
    //prevents threshold 0 from matching).
    const isErrorTerminal = !thresholdHit
      && this.context.data?.done === true
      && !!this.context.data?.$error;
    const needsCompletion = thresholdHit || this.shouldEmit() || this.shouldPersistJob() || isErrorTerminal;
    if (needsCompletion && !CollatorService.isGuidStep3Done(this.guidLedger)) {
      const txn3 = this.store.transact();
      const options = (thresholdHit || isErrorTerminal) ? {} : { emit: !this.shouldPersistJob() };
      await this.engine.runJobCompletionTasks(this.context, options, txn3);
      await CollatorService.notarizeStep3(this, msgGuid, txn3);
      const shouldFinalizeNow = (thresholdHit || isErrorTerminal) ? shouldFinalize : this.shouldPersistJob();
      if (shouldFinalizeNow) {
        await CollatorService.notarizeFinalize(this, txn3);
      }
      await txn3.exec();
    } else if (needsCompletion) {
      this.logger.debug('step-protocol-step3-skipped-already-done', {
        jid: this.context.metadata.jid,
        aid: this.metadata.aid,
      });
    } else {
      this.logger.debug('step-protocol-no-threshold', {
        jid: this.context.metadata.jid,
        aid: this.metadata.aid,
        thresholdHit,
      });
    }

    return thresholdHit;
  }

  /**
   * Extracts the thresholdHit value from transaction results.
   * The setStatusAndCollateGuid result is the last item.
   */
  resolveThresholdHit(results: TransactionResultList): boolean {
    const last = results[results.length - 1];
    const value = Array.isArray(last) ? last[1] : last;
    return Number(value) === 1;
  }

  /**
   * Extracts the job status from the last result of a transaction.
   * Used by subclass Leg1 process methods for telemetry.
   */
  resolveStatus(multiResponse: TransactionResultList): number {
    const activityStatus = multiResponse[multiResponse.length - 1];
    if (Array.isArray(activityStatus)) {
      return Number(activityStatus[1]);
    } else {
      return Number(activityStatus);
    }
  }

  /**
   * Leg1 entry verification for Category B activities (Leg1-only with children).
   * Returns true if this is a resume (Leg1 already completed on a prior attempt).
   * On resume, loads the GUID ledger for step-level resume decisions.
   */
  async verifyLeg1Entry(): Promise<boolean> {
    const msgGuid = this.context.metadata.guid;
    this.setLeg(1);
    await this.getState();
    this.context.metadata.guid = msgGuid;
    const threshold = this.mapStatusThreshold();
    try {
      CollatorService.assertJobActive(
        this.context.metadata.js,
        this.context.metadata.jid,
        this.metadata.aid,
        threshold,
      );
    } catch (error) {
      if (error instanceof InactiveJobError && threshold > 0) {
        //Dynamic Activation Control: threshold met, close the job
        await CollatorService.notarizeEntry(this);
        if (this.context.metadata.js === threshold) {
          //conclude job EXACTLY ONCE
          const status = await this.setStatus(-threshold);
          if (Number(status) === 0) {
            await this.engine.runJobCompletionTasks(this.context);
          }
        }
      }
      throw error;
    }
    try {
      await CollatorService.notarizeEntry(this);
      return false;
    } catch (error) {
      if (error instanceof CollationError && error.fault === 'duplicate') {
        if (this.config.cycle) {
          //Cycle re-entry: Leg1 already complete from prior iteration.
          //Increment Leg2 counter to derive the new dimensional index,
          //so children run in a fresh dimensional plane.
          const [activityLedger, guidLedger] =
            await CollatorService.notarizeLeg2Entry(this, msgGuid);
          this.adjacentIndex =
            CollatorService.getDimensionalIndex(activityLedger);
          this.guidLedger = guidLedger;
          return false;
        }
        //100B is set — Leg1 work already committed. Load GUID for step resume.
        const guidValue = await this.store.collateSynthetic(
          this.context.metadata.jid,
          msgGuid,
          0,
        );
        this.guidLedger = guidValue;
        return true;
      }
      throw error;
    }
  }

  /**
   * Executes the 3-step Leg1 protocol for Category B activities
   * (Leg1-only with children, e.g., Hook passthrough, Signal, Interrupt-another).
   * Uses the incoming Leg1 message GUID as the GUID ledger key.
   *
   * Step A: setState + notarizeLeg1Completion + step1 markers (transaction 1)
   * Step B: publish children + step2 markers + setStatusAndCollateGuid (transaction 2)
   * Step C: if edge → runJobCompletionTasks + step3 markers + finalize (transaction 3)
   *
   * @returns true if this transition caused the job to complete
   */
  async executeLeg1StepProtocol(
    delta: number,
  ): Promise<boolean> {
    const msgGuid = this.context.metadata.guid;
    const threshold = this.mapStatusThreshold();
    const { id: appId } = await this.engine.getVID();

    //Step A: Save work + Leg1 completion marker
    if (!CollatorService.isGuidStep1Done(this.guidLedger)) {
      const txn1 = this.store.transact();
      await this.setState(txn1);
      if (this.adjacentIndex === 0) {
        //First entry: mark Leg1 complete. On cycle re-entry
        //(adjacentIndex > 0), Leg1 is already complete and the
        //Leg2 counter was already incremented by notarizeLeg2Entry.
        await CollatorService.notarizeLeg1Completion(this, txn1);
      }
      await CollatorService.notarizeStep1(this, msgGuid, txn1);
      await txn1.exec();
    }

    //Step B: Spawn children + semaphore + edge capture
    let thresholdHit = false;
    if (!CollatorService.isGuidStep2Done(this.guidLedger)) {
      const txn2 = this.store.transact();
      await CollatorService.notarizeStep2(this, msgGuid, txn2);
      for (const child of this.adjacencyList) {
        await this.engine.router?.publishMessage(null, child, txn2);
      }
      await this.store.setStatusAndCollateGuid(
        delta,
        threshold,
        this.context.metadata.jid,
        appId,
        msgGuid,
        CollatorService.WEIGHTS.GUID_SNAPSHOT,
        txn2,
      );
      const results = (await txn2.exec()) as TransactionResultList;
      thresholdHit = this.resolveThresholdHit(results);
      this.logger.debug('leg1-step-protocol-stepB-complete', {
        jid: this.context.metadata.jid,
        aid: this.metadata.aid,
        delta,
        threshold,
        thresholdHit,
        lastResult: results[results.length - 1],
      });
    } else {
      thresholdHit = CollatorService.isGuidJobClosed(this.guidLedger);
    }

    //Step C: Job completion tasks (edge hit OR emit/persist)
    //When an activity marks the job done with an unrecoverable error
    //(e.g., stopper after max retries), force completion even when the
    //semaphore threshold isn't hit.
    const isErrorTerminal = !thresholdHit
      && this.context.data?.done === true
      && !!this.context.data?.$error;
    const needsCompletion = thresholdHit || this.shouldEmit() || this.shouldPersistJob() || isErrorTerminal;
    if (needsCompletion && !CollatorService.isGuidStep3Done(this.guidLedger)) {
      const txn3 = this.store.transact();
      const options = (thresholdHit || isErrorTerminal) ? {} : { emit: !this.shouldPersistJob() };
      await this.engine.runJobCompletionTasks(this.context, options, txn3);
      await CollatorService.notarizeStep3(this, msgGuid, txn3);
      await CollatorService.notarizeFinalize(this, txn3);
      await txn3.exec();
    }

    return thresholdHit;
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

  async setStatus(
    amount: number,
    transaction?: ProviderTransaction,
  ): Promise<void | any> {
    const { id: appId } = await this.engine.getVID();
    return await this.store.setStatus(
      amount,
      this.context.metadata.jid,
      appId,
      transaction,
    );
  }

  authorizeEntry(_state: StringAnyType): string[] {
    //seed writes removed: child activities increment from 0 (null field).
    //FINALIZE (200T) sets pos 1 directly to 2 without needing a 100T base.
    return [];
  }

  bindDimensionalAddress(state: StringAnyType) {
    const dad = this.resolveDad();
    state[`${this.metadata.aid}/output/metadata/dad`] = dad;
  }

  async setState(transaction?: ProviderTransaction): Promise<string> {
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
      transaction,
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

  /**
   * A job with a vale < -100_000_000 is considered interrupted,
   * as the interruption event decrements the job status by 1billion.
   */
  jobWasInterrupted(jobStatus: JobStatus): boolean {
    return jobStatus < -100_000_000;
  }
}

export { Activity, ActivityType };
