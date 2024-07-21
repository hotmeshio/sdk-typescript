import { DuplicateJobError } from '../../modules/errors';
import { formatISODate, getTimeSeries, guid } from '../../modules/utils';
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
import { JobState, ExtensionType, JobStatus } from '../../types/job';
import { RedisMulti } from '../../types/redis';
import { StringScalarType } from '../../types/serializer';
import { WorkListTaskType } from '../../types/task';

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
      await this.setStateNX(initialStatus);
      await this.setStatus(initialStatus);

      this.bindSearchData(options);
      this.bindMarkerData(options);

      const multi = this.store.getMulti();
      await this.setState(multi);
      await this.setStats(multi);
      if (options?.pending) {
        await this.setExpired(options?.pending, multi);
      } else {
        await this.registerJobDependency(multi);
      }
      await multi.exec();

      this.execAdjacentParent();
      telemetry.mapActivityAttributes();
      const jobStatus = Number(this.context.metadata.js);
      telemetry.setJobAttributes({ 'app.job.jss': jobStatus });
      const attrs: StringScalarType = { 'app.job.jss': jobStatus };
      await this.transitionAndLogAdjacent(options, jobStatus, attrs);
      telemetry.setActivityAttributes(attrs);

      return this.context.metadata.jid;
    } catch (error) {
      if (error instanceof DuplicateJobError) {
        this.logger.error('duplicate-job-error', { job_id: error.jobId });
      } else {
        this.logger.error('trigger-process-error', { ...error });
      }
      telemetry?.setActivityError(error.message);
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

  async transitionAndLogAdjacent(options: ExtensionType = {}, jobStatus: JobStatus, attrs: StringScalarType): Promise<void> {
    //todo: enable resume from pending state
    if (isNaN(options.pending)) {
      const messageIds = await this.transition(this.adjacencyList, jobStatus);
      if (messageIds.length) {
        attrs['app.activity.mids'] = messageIds.join(',');
      }
    }
  }

  /**
   * `pending` flows will not transition from the trigger to adjacent children until resumed
   * 
   * `expiring` flows initialize with a job status of `1m + adjacentChildCount` (not 0, the default);
   * they emit the 'job done' event when 1_000_000 is reached (as if 0 had been reached);
   * the record is still 'active' once 1m is reached, but the positive integer value is sufficient
   * to allow the system to accept outside events and keep the record 'alive'.
   */
  initStatus(options: ExtensionType = {}, count: number): number {
    if (options.pending) {
      return -1;
    } else if (options.statusThreshold) {
      return 1_000_000 - options.statusThreshold + count;
    }
    return count;
  }

  async setExpired(seconds: number, multi: RedisMulti) {
    await this.store.expireJob(this.context.metadata.jid, seconds, multi);
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

  async setStateNX(status?: number): Promise<void> {
    // const expire = Pipe.resolve(
    //   this.config.expire ?? HMSH_EXPIRE_DURATION,
    //   this.context,
    // );
    const jobId = this.context.metadata.jid;
    if (!await this.store.setStateNX(jobId, this.engine.appId, status)) {
      throw new DuplicateJobError(jobId);
    }
  }

  /**
   * Registers this job as a dependent of the parent job; when the
   * parent job is interrupted, this job will be interrupted
   */
  async registerJobDependency(multi?: RedisMulti): Promise<void> {
    const depKey = this.config.stats?.parent ?? this.context.metadata.pj;
    let resolvedDepKey = depKey ? Pipe.resolve(depKey, this.context) : '';
    const adjKey = this.config.stats?.adjacent;
    const resolvedAdjKey = depKey ? Pipe.resolve(adjKey, this.context) : '';
    if (!resolvedDepKey) {
      resolvedDepKey = this.context.metadata.pj;
    }
    if (resolvedDepKey) {
      const isParentOrigin =
        resolvedDepKey === this.context.metadata.pj ||
        resolvedDepKey === resolvedAdjKey;
      let type: WorkListTaskType;
      if (isParentOrigin) {
        if (this.context.metadata.px) {
          type = 'child';
        } else {
          type = 'expire-child';
        }
      } else {
        type = 'expire';
      }
      await this.store.registerJobDependency(
        type,
        resolvedDepKey,
        this.context.metadata.tpc,
        this.context.metadata.jid,
        this.context.metadata.gid,
        this.context.metadata.pd,
        multi,
      );
    }
    if (resolvedAdjKey && resolvedAdjKey !== resolvedDepKey) {
      await this.store.registerJobDependency(
        'child',
        resolvedAdjKey,
        this.context.metadata.tpc,
        this.context.metadata.jid,
        this.context.metadata.gid,
        this.context.metadata.pd,
        multi,
      );
    }
  }

  async setStats(multi?: RedisMulti): Promise<void> {
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
        multi,
      );
    }
  }
}

export { Trigger };
