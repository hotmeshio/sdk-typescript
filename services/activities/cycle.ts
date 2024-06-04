import {
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { guid } from '../../modules/utils';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  CycleActivity,
} from '../../types/activity';
import { JobState } from '../../types/job';
import { MultiResponseFlags, RedisMulti } from '../../types/redis';
import { StreamData } from '../../types/stream';
import { TelemetryService } from '../telemetry';

import { Activity } from './activity';

class Cycle extends Activity {
  config: CycleActivity;

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

  //********  LEG 1 ENTRY  ********//
  async process(): Promise<string> {
    this.logger.debug('cycle-process', {
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
      this.mapInputData();

      //set state/status
      let multi = this.store.getMulti();
      await this.setState(multi);
      await this.setStatus(0, multi); //leg 1 never changes job status
      const multiResponse = (await multi.exec()) as MultiResponseFlags;
      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);

      //cycle the target ancestor
      multi = this.store.getMulti();
      const messageId = await this.cycleAncestorActivity(multi);
      telemetry.setActivityAttributes({
        'app.activity.mid': messageId,
        'app.job.jss': jobStatus,
      });

      //exit early (`Cycle` activities only execute Leg 1)
      await CollatorService.notarizeEarlyExit(this, multi);
      (await multi.exec()) as MultiResponseFlags;

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('cycle-inactive-job-error', { ...error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { ...error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('cycle-get-state-error', { ...error });
        return;
      } else {
        this.logger.error('cycle-process-error', { ...error });
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('cycle-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
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
        guid: guid(),
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        dad: CollatorService.resolveReentryDimension(this),
        aid: this.config.ancestor,
        spn: this.context['$self'].output.metadata?.l1s,
        trc: this.context.metadata.trc,
      },
      data: this.context.data,
    };
    return (await this.engine.router?.publishMessage(
      null,
      streamData,
      multi,
    )) as string;
  }
}

export { Cycle };
