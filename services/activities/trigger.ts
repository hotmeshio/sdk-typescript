import { DuplicateJobError } from '../../modules/errors';
import {
  formatISODate,
  getTimeSeries,
  guid,
} from '../../modules/utils';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { ReporterService } from '../reporter';
import { MDATA_SYMBOLS } from '../serializer';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  TriggerActivity,
} from '../../types/activity';
import { JobState, ExtensionType } from '../../types/job';
import {
  ProviderTransaction,
  TransactionResultList,
} from '../../types/provider';
import { StringScalarType } from '../../types/serializer';
import { MapperService } from '../mapper';

import { Activity } from './activity';

class Trigger extends Activity {
  config: TriggerActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState,
  ) {
    super(config, data, metadata, hook, engine, context);
  }

  async process(options?: ExtensionType): Promise<string> {
    this.logger.debug('trigger-process', {
      subscribes: this.config.subscribes,
    });
    let telemetry: TelemetryService;
    try {
      this.setLeg(2);
      await this.getState();

      telemetry = new TelemetryService(
        this.engine.appId,
        this.config,
        this.metadata,
        this.context,
      );
      telemetry.startJobSpan();
      telemetry.startActivitySpan(this.leg);
      this.mapJobData();
      this.adjacencyList = await this.filterAdjacent();
      const initialStatus = this.initStatus(options, this.adjacencyList.length);
      //config.entity is a pipe expression; if 'entity' exists, it will resolve
      const resolvedEntity = new MapperService(
        { entity: this.config.entity },
        this.context,
      ).mapRules()?.entity as string;
      const msgGuid = this.context.metadata.guid || guid();
      this.context.metadata.guid = msgGuid;
      const { id: appId } = await this.engine.getVID();

      //═══ Step 1: Inception (atomic job creation + GUID seed) ═══
      const txn1 = this.store.transact();
      await this.store.setStateNX(
        this.context.metadata.jid,
        appId,
        initialStatus,
        options?.entity || resolvedEntity,
        txn1,
      );
      await this.store.collateSynthetic(
        this.context.metadata.jid,
        msgGuid,
        CollatorService.WEIGHTS.STEP1_WORK,
        txn1,
      );
      const results1 = (await txn1.exec()) as TransactionResultList;
      const jobCreated = Number(results1[0]) > 0;
      const guidValue = Number(results1[1]);

      if (!jobCreated) {
        if (guidValue > CollatorService.WEIGHTS.STEP1_WORK) {
          //crash recovery: GUID was seeded on a prior attempt; resume
          this.guidLedger = guidValue;
          this.logger.info('trigger-crash-recovery', {
            job_id: this.context.metadata.jid,
            guid: msgGuid,
            guidLedger: guidValue,
          });
        } else {
          //true duplicate: another process owns this job
          throw new DuplicateJobError(this.context.metadata.jid);
        }
      }

      await this.setStatus(initialStatus);
      this.bindSearchData(options);
      this.bindMarkerData(options);

      //═══ Step 2: Work (state + children + GUID Step 2) ═══
      if (!CollatorService.isGuidStep2Done(this.guidLedger)) {
        const txn2 = this.store.transact();
        await this.setState(txn2);
        await this.setStats(txn2);
        if (options?.pending) {
          await this.setExpired(options.pending, txn2);
        }
        //publish children (unless pending or job already complete)
        if (isNaN(options?.pending) && this.adjacencyList.length && initialStatus > 0) {
          for (const child of this.adjacencyList) {
            await this.engine.router?.publishMessage(null, child, txn2);
          }
        }
        await CollatorService.notarizeStep2(this, msgGuid, txn2);
        await txn2.exec();
      }

      //best-effort parent notification
      this.execAdjacentParent();

      //═══ Step 3: Completion (if job immediately complete) ═══
      //NOTE: runJobCompletionTasks is non-transactional here because
      //the trigger runs inline within pub(). Subscribers register AFTER
      //pub() returns, so pub notifications must be fire-and-forget to
      //avoid a race. The GUID marker commits in its own transaction.
      const jobStatus = Number(this.context.metadata.js);
      const needsCompletion =
        this.shouldEmit() ||
        this.isJobComplete(jobStatus) ||
        this.shouldPersistJob();
      if (needsCompletion && !CollatorService.isGuidStep3Done(this.guidLedger)) {
        await this.engine.runJobCompletionTasks(
          this.context,
          { emit: !this.isJobComplete(jobStatus) && !this.shouldPersistJob() },
        );
        //NOTE: notarizeStep3 is fire-and-forget for the trigger.
        //pubOneTimeSubs (inside runJobCompletionTasks) sends a NOTIFY
        //that must not be processed until registerJobCallback runs
        //AFTER pub() returns. An `await` here yields to the event loop,
        //which could deliver the NOTIFY before the callback is registered.
        const txn3 = this.store.transact();
        CollatorService.notarizeStep3(this, msgGuid, txn3)
          .then(() => txn3.exec())
          .catch((err) =>
            this.logger.error('trigger-notarize-step3-error', { error: err }),
          );
      }

      //telemetry
      telemetry.mapActivityAttributes();
      telemetry.setJobAttributes({ 'app.job.jss': jobStatus });
      const attrs: StringScalarType = { 'app.job.jss': jobStatus };
      telemetry.setActivityAttributes(attrs);

      return this.context.metadata.jid;
    } catch (error) {
      telemetry?.setActivityError(error.message);
      if (error instanceof DuplicateJobError) {
        this.logger.error('duplicate-job-error', {
          job_id: error.jobId,
          guid: this.context.metadata.guid,
        });
      } else {
        this.logger.error('trigger-process-error', { error });
      }
      throw error;
    } finally {
      telemetry?.endJobSpan();
      telemetry?.endActivitySpan();
      this.logger.debug('trigger-process-end', {
        subscribes: this.config.subscribes,
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
      });
    }
  }

  /**
   * `pending` flows will not transition from the trigger to adjacent children until resumed
   */
  initStatus(options: ExtensionType = {}, count: number): number {
    if (options.pending) {
      return -1;
    }
    return count;
  }

  async setExpired(seconds: number, transaction: ProviderTransaction) {
    await this.store.expireJob(this.context.metadata.jid, seconds, transaction);
  }

  safeKey(key: string): string {
    return `_${key}`;
  }

  bindSearchData(options?: ExtensionType): void {
    if (options?.search) {
      Object.keys(options.search).forEach((key) => {
        this.context.data[this.safeKey(key)] = options.search[key].toString();
      });
    }
  }

  bindMarkerData(options?: ExtensionType): void {
    if (options?.marker) {
      Object.keys(options.marker).forEach((key) => {
        if (key.startsWith('-')) {
          this.context.data[key] = options.marker[key].toString();
        }
      });
    }
  }

  async setStatus(amount: number): Promise<void> {
    this.context.metadata.js = amount;
  }

  /**
   * if the parent (spawner) chose not to await, emit the job_id
   * as the data payload { job_id }
   */
  async execAdjacentParent() {
    if (this.context.metadata.px) {
      const timestamp = formatISODate(new Date());
      const jobStartedConfirmationMessage = {
        metadata: this.context.metadata,
        data: {
          job_id: this.context.metadata.jid,
          jc: timestamp,
          ju: timestamp,
        },
      };
      await this.engine.execAdjacentParent(
        this.context,
        jobStartedConfirmationMessage,
      );
    }
  }

  createInputContext(): Partial<JobState> {
    const input = {
      [this.metadata.aid]: {
        input: { data: this.data },
      },
      $self: {
        input: { data: this.data },
        output: { data: this.data },
      },
    } as Partial<JobState>;
    return input;
  }

  async getState(): Promise<void> {
    const inputContext = this.createInputContext();
    const jobId = this.resolveJobId(inputContext);
    const jobKey = this.resolveJobKey(inputContext);

    const utc = formatISODate(new Date());
    const { id, version } = await this.engine.getVID();
    this.initDimensionalAddress(CollatorService.getDimensionalSeed());
    const activityMetadata = {
      ...this.metadata,
      jid: jobId,
      key: jobKey,
      as: CollatorService.getTriggerSeed(),
    };
    this.context = {
      metadata: {
        ...this.metadata,
        gid: guid(),
        ngn: this.context.metadata.ngn,
        pj: this.context.metadata.pj,
        pg: this.context.metadata.pg,
        pd: this.context.metadata.pd,
        pa: this.context.metadata.pa,
        px: this.context.metadata.px,
        app: id,
        vrs: version,
        tpc: this.config.subscribes,
        trc: this.context.metadata.trc,
        spn: this.context.metadata.spn,
        guid: this.context.metadata.guid,
        jid: jobId,
        dad: CollatorService.getDimensionalSeed(), //top-level job implicitly uses `,0`
        key: jobKey,
        jc: utc,
        ju: utc,
        ts: getTimeSeries(this.resolveGranularity()),
        js: 0,
      },
      data: {},
      [this.metadata.aid]: {
        input: {
          data: this.data,
          metadata: activityMetadata,
        },
        output: {
          data: this.data,
          metadata: activityMetadata,
        },
        settings: { data: {} },
        errors: { data: {} },
      },
    };
    this.context['$self'] = this.context[this.metadata.aid];
    this.context['$job'] = this.context; //NEVER call STRINGIFY! (circular)
  }

  bindJobMetadataPaths(): string[] {
    return MDATA_SYMBOLS.JOB.KEYS.map((key) => `metadata/${key}`);
  }

  bindActivityMetadataPaths(): string[] {
    return MDATA_SYMBOLS.ACTIVITY.KEYS.map((key) => `output/metadata/${key}`);
  }

  resolveGranularity(): string {
    return (
      this.config.stats?.granularity || ReporterService.DEFAULT_GRANULARITY
    );
  }

  getJobStatus(): number {
    return this.context.metadata.js;
  }

  resolveJobId(context: Partial<JobState>): string {
    const jobId = this.config.stats?.id;
    return jobId ? Pipe.resolve(jobId, context) : guid();
  }

  resolveJobKey(context: Partial<JobState>): string {
    const jobKey = this.config.stats?.key;
    return jobKey ? Pipe.resolve(jobKey, context) : '';
  }

  async setStats(transaction?: ProviderTransaction): Promise<void> {
    const md = this.context.metadata;
    if (md.key && this.config.stats?.measures) {
      const config = await this.engine.getVID();
      const reporter = new ReporterService(config, this.store, this.logger);
      await this.store.setStats(
        md.key,
        md.jid,
        md.ts,
        reporter.resolveTriggerStatistics(this.config, this.context),
        config,
        transaction,
      );
    }
  }
}

export { Trigger };
