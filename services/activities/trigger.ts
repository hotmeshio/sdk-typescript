import { v4 as uuidv4 } from 'uuid';
import { DuplicateJobError } from '../../modules/errors';
import { formatISODate, getTimeSeries } from '../../modules/utils';
import { Activity } from './activity';
import { CollatorService } from '../collator';
import { DimensionService } from '../dimension';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { ReporterService } from '../reporter';
import { MDATA_SYMBOLS } from '../serializer';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  TriggerActivity } from '../../types/activity';
import { JobState } from '../../types/job';
import { RedisMulti } from '../../types/redis';
import { StringScalarType } from '../../types/serializer';

class Trigger extends Activity {
  config: TriggerActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState) {
      super(config, data, metadata, hook, engine, context);
  }

  async process(): Promise<string> {
    this.logger.debug('trigger-process', { subscribes: this.config.subscribes });
    let telemetry: TelemetryService;
    try {
      this.setLeg(2);
      await this.getState();
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startJobSpan();
      telemetry.startActivitySpan(this.leg);
      this.mapJobData();
      await this.setStateNX();
      this.adjacencyList = await this.filterAdjacent();
      await this.setStatus(this.adjacencyList.length);

      const multi = this.store.getMulti();
      await this.setState(multi);
      await this.setStats(multi);
      await multi.exec();

      telemetry.mapActivityAttributes();
      const jobStatus = Number(this.context.metadata.js);
      telemetry.setJobAttributes({ 'app.job.jss': jobStatus });
      const attrs: StringScalarType = { 'app.job.jss': jobStatus };
      const messageIds = await this.transition(this.adjacencyList, jobStatus);
      if (messageIds.length) {
        attrs['app.activity.mids'] = messageIds.join(',')
      }
      telemetry.setActivityAttributes(attrs);
      return this.context.metadata.jid;
    } catch (error) {
      if (error instanceof DuplicateJobError) {
        this.logger.error('duplicate-job-error', error);
      } else {
        this.logger.error('trigger-process-error', error);
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endJobSpan();
      telemetry.endActivitySpan();
      this.logger.debug('trigger-process-end', { subscribes: this.config.subscribes, jid: this.context.metadata.jid });
    }
  }

  async setStatus(amount: number): Promise<void> {
    this.context.metadata.js = amount;
  }

  createInputContext(): Partial<JobState> {
    const input = { 
      [this.metadata.aid]: {
        input: { data: this.data }
      },
      '$self': {
        input: { data: this.data },
        output: { data: this.data }
      },
    } as Partial<JobState>;
    return input
  }

  async getState(): Promise<void> {
    const inputContext = this.createInputContext();
    const jobId = this.resolveJobId(inputContext);
    const jobKey = this.resolveJobKey(inputContext);

    const utc = formatISODate(new Date());
    const { id, version } = await this.engine.getVID();
    this.initDimensionalAddress(DimensionService.getSeed());
    const activityMetadata = {
      ...this.metadata,
      jid: jobId,
      key: jobKey,
      as: CollatorService.getTriggerSeed(),
    };
    this.context = {
      metadata: {
        ...this.metadata,
        ngn: this.context.metadata.ngn,
        pj: this.context.metadata.pj,
        pd: this.context.metadata.pd,
        pa: this.context.metadata.pa,
        app: id,
        vrs: version,
        tpc: this.config.subscribes,
        trc: this.context.metadata.trc,
        spn: this.context.metadata.spn,
        jid: jobId,
        dad: DimensionService.getSeed(), //top-level job implicitly uses `,0`
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
          metadata: activityMetadata
        },
        output: { 
          data: this.data,
          metadata: activityMetadata
        },
        settings: { data: {} },
        errors: { data: {} },
       },
    };
    this.context['$self'] = this.context[this.metadata.aid];
  }

  bindJobMetadataPaths(): string[] {
    return MDATA_SYMBOLS.JOB.KEYS.map((key) => `metadata/${key}`);
  }

  bindActivityMetadataPaths(): string[] {
    return MDATA_SYMBOLS.ACTIVITY.KEYS.map((key) => `output/metadata/${key}`);
  }

  resolveGranularity(): string {
    return ReporterService.DEFAULT_GRANULARITY;
  }

  getJobStatus(): number {
    return this.context.metadata.js;
  }

  resolveJobId(context: Partial<JobState>): string {
    const jobId = this.config.stats?.id;
    return jobId ? Pipe.resolve(jobId, context) : uuidv4();
  }

  resolveJobKey(context: Partial<JobState>): string {
    const jobKey = this.config.stats?.key;
    return jobKey ? Pipe.resolve(jobKey, context) : '';
  }

  async setStateNX(): Promise<void> {
    const jobId = this.context.metadata.jid;
    if (!await this.store.setStateNX(jobId, this.engine.appId)) {
      throw new DuplicateJobError(jobId);
    }
  }

  async setStats(multi?: RedisMulti): Promise<void> {
    const md = this.context.metadata;
    if (this.config.stats?.measures) {
      const config = await this.engine.getVID();
      const reporter = new ReporterService(config, this.store, this.logger);
      await this.store.setStats(
        md.key,
        md.jid,
        md.ts,
        reporter.resolveTriggerStatistics(this.config, this.context),
        config,
        multi
      );
    }
  }
}

export { Trigger };
