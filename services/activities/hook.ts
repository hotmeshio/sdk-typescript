import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { TaskService } from '../task';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  HookActivity,
} from '../../types/activity';
import { HookRule } from '../../types/hook';
import { JobState, JobStatus } from '../../types/job';
import {
  ProviderTransaction,
  TransactionResultList,
} from '../../types/provider';
import { StringScalarType } from '../../types/serializer';
import { StreamCode, StreamStatus } from '../../types/stream';

import { Activity } from './activity';

/**
 * Supports `signal hook`, `time hook`, and `cycle hook` patterns
 */
class Hook extends Activity {
  config: HookActivity;

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

  //********  INITIAL ENTRY POINT (A)  ********//
  async process(): Promise<string> {
    this.logger.debug('hook-process', {
      jid: this.context.metadata.jid,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    let telemetry: TelemetryService;

    try {
      await this.verifyEntry();

      telemetry = new TelemetryService(
        this.engine.appId,
        this.config,
        this.metadata,
        this.context,
      );
      telemetry.startActivitySpan(this.leg);

      if (this.doesHook()) {
        //sleep and wait to awaken upon a signal
        await this.doHook(telemetry);
      } else {
        //end the activity and transition to its children
        await this.doPassThrough(telemetry);
      }

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('hook-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('hook-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('hook-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('hook-collation-error', { error });
      } else {
        this.logger.error('hook-process-error', { error });
      }
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('hook-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
    }
  }

  /**
   * does this activity use a time-hook or web-hook
   */
  doesHook(): boolean {
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      return !isNaN(duration) && Number(duration) > 0;
    }
    return !!this.config.hook?.topic;
  }

  async doHook(telemetry: TelemetryService) {
    const transaction = this.store.transact();
    await this.registerHook(transaction);
    this.mapOutputData();
    this.mapJobData();
    await this.setState(transaction);
    await CollatorService.authorizeReentry(this, transaction);

    await this.setStatus(0, transaction);
    await transaction.exec();
    telemetry.mapActivityAttributes();
  }

  async doPassThrough(telemetry: TelemetryService) {
    const transaction = this.store.transact();
    let multiResponse: TransactionResultList;

    this.adjacencyList = await this.filterAdjacent();
    this.mapOutputData();
    this.mapJobData();
    await this.setState(transaction);
    await CollatorService.notarizeEarlyCompletion(this, transaction);

    await this.setStatus(this.adjacencyList.length - 1, transaction);
    multiResponse = (await transaction.exec()) as TransactionResultList;
    telemetry.mapActivityAttributes();
    const jobStatus = this.resolveStatus(multiResponse);
    const attrs: StringScalarType = { 'app.job.jss': jobStatus };
    const messageIds = await this.transition(this.adjacencyList, jobStatus);
    if (messageIds.length) {
      attrs['app.activity.mids'] = messageIds.join(',');
    }
    telemetry.setActivityAttributes(attrs);
  }

  async getHookRule(topic: string): Promise<HookRule | undefined> {
    const rules = await this.store.getHookRules();
    return rules?.[topic]?.[0] as HookRule;
  }

  async registerHook(
    transaction?: ProviderTransaction,
  ): Promise<string | void> {
    if (this.config.hook?.topic) {
      return await this.engine.taskService.registerWebHook(
        this.config.hook.topic,
        this.context,
        this.resolveDad(),
        this.context.metadata.expire,
        transaction,
      );
    } else if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      await this.engine.taskService.registerTimeHook(
        this.context.metadata.jid,
        this.context.metadata.gid,
        `${this.metadata.aid}${this.metadata.dad || ''}`,
        'sleep',
        duration,
        this.metadata.dad || '',
      );
      return this.context.metadata.jid;
    }
  }

  //********  SIGNAL RE-ENTRY POINT  ********//
  async processWebHookEvent(
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
  ): Promise<JobStatus | void> {
    this.logger.debug('hook-process-web-hook-event', {
      topic: this.config.hook.topic,
      aid: this.metadata.aid,
      status,
      code,
    });
    const taskService = new TaskService(this.store, this.logger);
    const data = { ...this.data };
    const signal = await taskService.processWebHookSignal(
      this.config.hook.topic,
      data,
    );
    if (signal) {
      const [jobId, _aid, dad, gId] = signal;
      this.context.metadata.jid = jobId;
      this.context.metadata.gid = gId;
      this.context.metadata.dad = dad;
      await this.processEvent(status, code, 'hook');
      if (code === 200) {
        await taskService.deleteWebHookSignal(this.config.hook.topic, data);
      } //else => 202/keep alive
    } //else => already resolved
  }

  async processTimeHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('hook-process-time-hook-event', {
      jid: jobId,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    await this.processEvent(StreamStatus.SUCCESS, 200, 'hook');
  }
}

export { Hook };
