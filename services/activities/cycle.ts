import { GetStateError } from '../../modules/errors';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Activity, ActivityType } from './activity';
import {
  ActivityData,
  ActivityMetadata,
  CycleActivity } from '../../types/activity';
import { JobState } from '../../types/job';
import { MultiResponseFlags, RedisMulti } from '../../types/redis';
import { StreamData } from '../../types/stream';
import { TelemetryService } from '../telemetry';

class Cycle extends Activity {
  config: CycleActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState) {
      super(config, data, metadata, hook, engine, context);
  }


  //********  LEG 1 ENTRY  ********//
  async process(): Promise<string> {
    this.logger.debug('cycle-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    let telemetry: TelemetryService;
    try {
      //verify entry is allowed
      this.setLeg(1);
      await CollatorService.notarizeEntry(this);
      await this.getState();
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
      this.mapInputData();

      //set state/status
      let multi = this.store.getMulti();
      await this.setState(multi);
      await this.setStatus(0, multi); //leg 1 never changes job status
      const multiResponse = await multi.exec() as MultiResponseFlags;
      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);

      //cycle the target ancestor
      multi = this.store.getMulti();
      const messageId = await this.cycleAncestorActivity(multi);
      telemetry.setActivityAttributes({
        'app.activity.mid': messageId,
        'app.job.jss': jobStatus
      });

      //exit early (`Cycle` activities only execute Leg 1)
      await CollatorService.notarizeEarlyExit(this, multi);
      await multi.exec() as MultiResponseFlags;

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof GetStateError) {
        this.logger.error('cycle-get-state-error', error);
      } else {
        this.logger.error('cycle-process-error', error);
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('cycle-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }

  /**
   * Trigger the target ancestor to execute in a cycle,
   * without violating the constraints of the DAG. Immutable
   * `individual activity state` will execute in a new dimensional
   * thread while `shared job state` can change. This
   * pattern allows for retries without violating the DAG.
   */
  async cycleAncestorActivity(multi: RedisMulti): Promise<string> {
    //Cycle activity L1 is a standin for the target ancestor L1.
    //Input data mapping (mapInputData) allows for the 
    //next dimensonal thread to execute with different
    //input data than the current dimensional thread
    this.mapInputData();
    const streamData: StreamData = {
      metadata: {
        dad: CollatorService.resolveReentryDimension(this),
        jid: this.context.metadata.jid,
        aid: this.config.ancestor,
      },
      data: this.context.data
    };
    return (await this.engine.streamSignaler?.publishMessage(null, streamData, multi)) as string;
  }
}

export { Cycle };
