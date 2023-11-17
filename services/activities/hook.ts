import { GetStateError } from '../../modules/errors';
import { Activity } from './activity';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { StoreSignaler } from '../signaler/store';
import { TelemetryService } from '../telemetry';
import { 
  ActivityData,
  ActivityMetadata,
  ActivityType,
  HookActivity } from '../../types/activity';
import { HookRule } from '../../types/hook';
import { JobState, JobStatus } from '../../types/job';
import {
  MultiResponseFlags,
  RedisMulti } from '../../types/redis';
import { StringScalarType } from '../../types/serializer';
import { StreamStatus } from '../../types/stream';

/**
 * Listens for `webhook`, `timehook`, and `cycle` (repeat) signals
 */
class Hook extends Activity {
  config: HookActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState) {
      super(config, data, metadata, hook, engine, context);
  }

  //********  INITIAL ENTRY POINT (A)  ********//
  async process(): Promise<string> {
    this.logger.debug('hook-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
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
        this.logger.error('hook-get-state-error', { error });
      } else {
        this.logger.error('hook-process-error', { error });
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('hook-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }

  //********  SIGNAL RE-ENTRY POINT  ********//
  doesHook(): boolean {
    //does this activity use a time-hook or web-hook
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
    this.logger.debug('hook-process-web-hook-event', {
      topic: this.config.hook.topic,
      aid: this.metadata.aid
    });
    const signaler = new StoreSignaler(this.store, this.logger);
    const data = { ...this.data };
    const jobId = await signaler.processWebHookSignal(this.config.hook.topic, data);
    if (jobId) {
      //if a webhook signal is sent that includes 'keep_alive' the hook will remain open
      const code = data.keep_alive ? 202 : 200;
      await this.processEvent(StreamStatus.SUCCESS, code, 'hook', jobId);
      if (code === 200) {
        await signaler.deleteWebHookSignal(this.config.hook.topic, data);
      }
    } //else => already resolved
  }

  async processTimeHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('hook-process-time-hook-event', {
      jid: jobId,
      aid: this.metadata.aid
    });
    await this.processEvent(StreamStatus.SUCCESS, 200, 'hook');
  }
}

export { Hook };
