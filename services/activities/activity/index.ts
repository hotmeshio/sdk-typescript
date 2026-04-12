import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../../modules/errors';
import { EngineService } from '../../engine';
import { ILogger } from '../../logger';
import { StoreService } from '../../store';
import { TelemetryService } from '../../telemetry';
import {
  ActivityData,
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
} from '../../../types/activity';
import {
  ProviderClient,
  ProviderTransaction,
  TransactionResultList,
} from '../../../types/provider';
import { JobState, JobStatus } from '../../../types/job';
import { StringAnyType } from '../../../types/serializer';
import {
  StreamCode,
  StreamData,
  StreamStatus,
} from '../../../types/stream';

import * as Mapping from './mapping';
import * as Context from './context';
import * as State from './state';
import * as Transition from './transition';
import * as Verify from './verify';
import * as Protocol from './protocol';
import * as Process from './process';

/**
 * Base class for every node in the workflow DAG.
 *
 * An activity's lifecycle flows top-to-bottom through this file:
 * enter → load state → map data → persist → transition → complete.
 *
 * Each section delegates to a purpose-specific module inside `activity/`.
 * Open the module when you need implementation detail; read this file
 * when you need the big picture.
 */
class Activity {

  // ── identity & runtime ────────────────────────────────────────────
  //
  // `config`  YAML descriptor for this activity node
  // `data`    inbound payload (input on Leg1, response on Leg2)
  // `hook`    webhook/sleep payload (Hook activities only)
  // `metadata` routing envelope (aid, jid, gid, dad)
  //
  config: ActivityType;
  data: ActivityData;
  hook: ActivityData;
  metadata: ActivityMetadata;

  // ── services ──────────────────────────────────────────────────────

  /** @hidden */
  store: StoreService<ProviderClient, ProviderTransaction>;
  engine: EngineService;
  logger: ILogger;

  // ── execution state ───────────────────────────────────────────────
  //
  // `context`       full job tree (loaded from store on every entry)
  // `status/code`   result of the most recent leg (SUCCESS/ERROR, 200/500…)
  // `leg`           which phase: 1 = dispatch, 2 = response
  // `adjacencyList` children to transition to after this activity
  // `adjacentIndex` dimensional offset (>0 when cycling)
  // `guidLedger`    crash-recovery bookmark for 3-step protocol resume
  //
  context: JobState;
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

  // ═════════════════════════════════════════════════════════════════
  //  1. ENTER — verify this activity is allowed to run
  //     Guards against stale messages, duplicate deliveries,
  //     and inactive jobs. Sets the execution leg.
  //     → see verify.ts
  // ═════════════════════════════════════════════════════════════════

  /** Leg 1 entry for duplex activities (Worker, Await, Cycle) */
  async verifyEntry(): Promise<void> {
    return Verify.verifyEntry(this);
  }

  /** Leg 2 re-entry for duplex activities; returns the activity ledger */
  async verifyReentry(): Promise<number> {
    return Verify.verifyReentry(this);
  }

  /** Leg 1 entry for Category B (Hook passthrough, Signal, Interrupt-target).
   *  Returns true when resuming from a prior crash (Leg1 already committed). */
  async verifyLeg1Entry(): Promise<boolean> {
    return Verify.verifyLeg1Entry(this);
  }

  // ═════════════════════════════════════════════════════════════════
  //  2. LOAD / INITIALIZE — hydrate job state from the store
  //     Reads the hash, restores the context tree, validates
  //     the generational ID, and initializes $self / policies.
  //     → see state.ts (getState) and context.ts
  // ═════════════════════════════════════════════════════════════════

  /** Load job state from the store into `this.context` */
  async getState(): Promise<void> {
    return State.getState(this);
  }

  /** Bootstrap $self, $job refs and output.metadata.au timestamp */
  initSelf(ctx: StringAnyType): JobState {
    return Context.initSelf(this, ctx);
  }

  /** Resolve expire and persistent policies from config */
  initPolicies(ctx: JobState): void {
    Context.initPolicies(this, ctx);
  }

  /** Set the dimensional address for this activity */
  initDimensionalAddress(dad: string): void {
    Context.initDimensionalAddress(this, dad);
  }

  /** Reject stale messages from a prior job generation */
  assertGenerationalId(jobGID: string, msgGID?: string): void {
    Context.assertGenerationalId(jobGID, msgGID, this.context);
  }

  // ═════════════════════════════════════════════════════════════════
  //  3. MAP — transform data between activities and the job
  //     Evaluates @pipe expressions in the YAML `maps` config
  //     to move data in (input), out (output), and to the job.
  //     → see mapping.ts
  // ═════════════════════════════════════════════════════════════════

  /** Map data INTO this activity (from other activities' outputs) */
  mapInputData(): void {
    Mapping.mapInputData(this);
  }

  /** Transform this activity's own output via output.maps */
  mapOutputData(): void {
    Mapping.mapOutputData(this);
  }

  /** Promote activity data to the shared job state via job.maps */
  mapJobData(): void {
    Mapping.mapJobData(this);
  }

  /** Resolve the Dynamic Activation Control threshold (default: 0) */
  mapStatusThreshold(): number {
    return Mapping.mapStatusThreshold(this);
  }

  // ═════════════════════════════════════════════════════════════════
  //  4. BIND — prepare context fields for persistence
  //     Stamps metadata, flattens the context tree into key/value
  //     pairs, and attaches error or hook data.
  //     → see state.ts (bind*) and context.ts (bindActivity*)
  // ═════════════════════════════════════════════════════════════════

  /** Stamp job-level `ju` (job_updated) timestamp */
  bindJobMetadata(): void {
    State.bindJobMetadata(this);
  }

  /** Stamp activity-level timestamps, type, and subtype */
  bindActivityMetadata(): void {
    State.bindActivityMetadata(this);
  }

  /** Flatten job-level context paths into store-ready key/values */
  async bindJobState(state: StringAnyType): Promise<void> {
    return State.bindJobState(this, state);
  }

  /** Flatten activity-level context paths into store-ready key/values */
  bindActivityState(state: StringAnyType): void {
    State.bindActivityState(this, state);
  }

  /** Write the dimensional address into the state payload */
  bindDimensionalAddress(state: StringAnyType): void {
    State.bindDimensionalAddress(this, state);
  }

  /** Attach inbound data (output or hook) to the activity's context slot */
  bindActivityData(type: 'output' | 'hook'): void {
    Context.bindActivityData(this, type);
  }

  /** Attach an error response to the activity's context slot */
  bindActivityError(data: Record<string, unknown>): void {
    Context.bindActivityError(this, data);
  }

  /** Promote an unhandled activity error to the job level */
  bindJobError(data: Record<string, unknown>): void {
    Context.bindJobError(this, data);
  }

  // ═════════════════════════════════════════════════════════════════
  //  5. PERSIST — write state to the store
  //     Orchestrates bind → flatten → store.setState in one call.
  //     Also handles the job semaphore (setStatus).
  //     → see state.ts
  // ═════════════════════════════════════════════════════════════════

  /** Persist the full activity + job state in a single store call */
  async setState(txn?: ProviderTransaction): Promise<string> {
    return State.setState(this, txn);
  }

  /** Increment/decrement the job semaphore */
  async setStatus(amount: number, txn?: ProviderTransaction): Promise<void | any> {
    return State.setStatus(this, amount, txn);
  }

  // ═════════════════════════════════════════════════════════════════
  //  6. TRANSITION — resolve which children run next
  //     Evaluates transition rules, checks job completion,
  //     and builds the adjacency list for the step protocol.
  //     → see transition.ts and context.ts (resolveDad)
  // ═════════════════════════════════════════════════════════════════

  /** Build the list of child activities to transition to */
  async filterAdjacent(): Promise<StreamData[]> {
    return Transition.filterAdjacent(this);
  }

  /** Current dimensional address (accounts for cycle offset) */
  resolveDad(): string {
    return Context.resolveDad(this);
  }

  /** Dimensional address for children (appends seed `,0`) */
  resolveAdjacentDad(): string {
    return Context.resolveAdjacentDad(this);
  }

  /** Did the semaphore reach its threshold? (from transaction results) */
  resolveThresholdHit(results: TransactionResultList): boolean {
    return Transition.resolveThresholdHit(results);
  }

  /** Extract the job status from the last transaction result */
  resolveStatus(multi: TransactionResultList): number {
    return Transition.resolveStatus(multi);
  }

  /** Should this activity emit to the graph's publishes topic? */
  shouldEmit(): boolean {
    return Transition.shouldEmit(this);
  }

  /** Should this activity emit completion while keeping the job alive? */
  shouldPersistJob(): boolean {
    return Transition.shouldPersistJob(this);
  }

  isJobComplete(s: JobStatus): boolean {
    return Transition.isJobComplete(s);
  }

  jobWasInterrupted(s: JobStatus): boolean {
    return Transition.jobWasInterrupted(s);
  }

  // ═════════════════════════════════════════════════════════════════
  //  7. COMMIT — crash-safe 3-step protocols
  //     Each protocol runs three atomic transactions:
  //     save work → spawn children + semaphore → completion tasks.
  //     GUID ledger digits allow crash-resume at any step.
  //     → see protocol.ts
  // ═════════════════════════════════════════════════════════════════

  /** Leg 2 protocol: save → spawn → complete (duplex activities) */
  async executeStepProtocol(delta: number, shouldFinalize: boolean): Promise<boolean> {
    return Protocol.executeStepProtocol(this, delta, shouldFinalize);
  }

  /** Leg 1 protocol: save → spawn → complete (Category B activities) */
  async executeLeg1StepProtocol(delta: number): Promise<boolean> {
    return Protocol.executeLeg1StepProtocol(this, delta);
  }

  // ═════════════════════════════════════════════════════════════════
  //  8. DUPLEX RE-ENTRY — handle Leg 2 responses
  //     Called when a worker/await/hook returns its result.
  //     Binds the response, maps job data, and runs the protocol.
  //     → see process.ts
  // ═════════════════════════════════════════════════════════════════

  async processEvent(
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
    type: 'hook' | 'output' = 'output',
  ): Promise<void> {
    return Process.processEvent(this, status, code, type);
  }

  // ═════════════════════════════════════════════════════════════════
  //  OVERRIDABLE — stubs replaced by specific activity types
  // ═════════════════════════════════════════════════════════════════

  setLeg(leg: ActivityLeg): void { this.leg = leg; }
  async registerTimeout(): Promise<void> { /* hook/duplex timeout */ }
  getJobStatus(): null | number { return null; }
  authorizeEntry(_state: StringAnyType): string[] { return []; }
  bindSearchData(_options?: any): void { /* Trigger */ }
  bindMarkerData(_options?: any): void { /* Trigger */ }

  /** Job metadata paths to persist (Trigger overrides with full JOB set) */
  bindJobMetadataPaths(): string[] {
    return State.bindJobMetadataPaths();
  }

  /** Activity metadata paths to persist (Trigger overrides; leg-aware) */
  bindActivityMetadataPaths(): string[] {
    return State.bindActivityMetadataPaths(this.leg);
  }

  async getTriggerConfig(): Promise<ActivityType> {
    return await this.store.getSchema(
      this.config.trigger,
      await this.engine.getVID(),
    );
  }

  // ═════════════════════════════════════════════════════════════════
  //  ERROR — shared catch handler for subclass process() methods
  //  Swallows expected concurrency errors (stale job, duplicate
  //  delivery, generational mismatch) and re-throws the rest.
  // ═════════════════════════════════════════════════════════════════

  handleProcessError(
    error: Error,
    telemetry: TelemetryService | undefined,
    label: string,
  ): void {
    if (error instanceof InactiveJobError) {
      this.logger.error(`${label}-inactive-job-error`, { error });
      return;
    } else if (error instanceof GenerationalError) {
      this.logger.info('process-event-generational-job-error', { error });
      return;
    } else if (error instanceof GetStateError) {
      this.logger.error(`${label}-get-state-error`, { error });
      return;
    } else if (error instanceof CollationError) {
      if (error.fault === 'duplicate') {
        this.logger.info(`${label}-collation-overage`, {
          job_id: this.context.metadata.jid,
          guid: this.context.metadata.guid,
        });
        return;
      }
      this.logger.error(`${label}-collation-error`, { error });
    } else {
      this.logger.error(`${label}-process-error`, { error });
    }
    telemetry?.setActivityError(error.message);
    throw error;
  }
}

export { Activity, ActivityType };
