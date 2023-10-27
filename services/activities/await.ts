import { GetStateError } from '../../modules/errors';
import { Activity } from './activity';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import {
  ActivityData,
  ActivityMetadata,
  AwaitActivity,
  ActivityType } from '../../types/activity';
import { JobState } from '../../types/job';
import { MultiResponseFlags, RedisMulti } from '../../types/redis';
import { StreamData, StreamDataType } from '../../types/stream';
import { TelemetryService } from '../telemetry';
import { Pipe } from '../pipe';

class Await extends Activity {
  config: AwaitActivity;

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
    this.logger.debug('await-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    let telemetry: TelemetryService;
    try {
      //confirm entry is allowed and restore state
      this.setLeg(1);
      await CollatorService.notarizeEntry(this);
      await this.getState();
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
      this.mapInputData();

      //save state and authorize reentry
      const multi = this.store.getMulti();
      //todo: await this.registerTimeout();
      const messageId = await this.execActivity(multi);
      await CollatorService.authorizeReentry(this, multi);
      await this.setState(multi);
      await this.setStatus(0, multi);
      const multiResponse = await multi.exec() as MultiResponseFlags;

      //telemetry
      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);
      telemetry.setActivityAttributes({
        'app.activity.mid': messageId,
        'app.job.jss': jobStatus
      });
      return this.context.metadata.aid;
    } catch (error) {
      telemetry.setActivityError(error.message);
      if (error instanceof GetStateError) {
        this.logger.error('await-get-state-error', { error });
      } else {
        this.logger.error('await-process-error', { error });
      }
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('await-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }

  async execActivity(multi: RedisMulti): Promise<string> {
    const topic = Pipe.resolve(this.config.subtype, this.context);
    const streamData: StreamData = {
      metadata: {
        jid: this.context.metadata.jid,
        dad: this.metadata.dad,
        aid: this.metadata.aid,
        topic,
        spn: this.context['$self'].output.metadata?.l1s,
        trc: this.context.metadata.trc,
      },
      type: StreamDataType.AWAIT,
      data: this.context.data
    };
    if (this.config.retry) {
      streamData.policies = {
        retry: this.config.retry
      };
    }
    return (await this.engine.streamSignaler?.publishMessage(null, streamData, multi)) as string;
  }
}

export { Await };
