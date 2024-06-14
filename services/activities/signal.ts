import {
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { MapperService } from '../mapper';
import { Pipe } from '../pipe';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  SignalActivity,
} from '../../types/activity';
import { JobState } from '../../types/job';
import { MultiResponseFlags } from '../../types/redis';
import { StringScalarType } from '../../types/serializer';
import { JobStatsInput } from '../../types/stats';

import { Activity } from './activity';

class Signal extends Activity {
  config: SignalActivity;

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
    this.logger.debug('signal-process', {
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

      //save state and notarize early completion (signals only run leg1)
      const multi = this.store.getMulti();
      this.adjacencyList = await this.filterAdjacent();
      this.mapOutputData();
      this.mapJobData();
      await this.setState(multi);
      await CollatorService.notarizeEarlyCompletion(this, multi);
      await this.setStatus(this.adjacencyList.length - 1, multi);
      const multiResponse = (await multi.exec()) as MultiResponseFlags;

      //todo: this should execute BEFORE the status is decremented
      if (this.config.subtype === 'all') {
        await this.hookAll();
      } else {
        await this.hookOne();
      }

      //transition to adjacent activities
      const jobStatus = this.resolveStatus(multiResponse);
      const attrs: StringScalarType = { 'app.job.jss': jobStatus };
      const messageIds = await this.transition(this.adjacencyList, jobStatus);
      if (messageIds.length) {
        attrs['app.activity.mids'] = messageIds.join(',');
      }
      telemetry.mapActivityAttributes();
      telemetry.setActivityAttributes(attrs);

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('signal-inactive-job-error', { ...error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { ...error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('signal-get-state-error', { ...error });
        return;
      } else {
        this.logger.error('signal-process-error', { ...error });
      }
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('signal-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
    }
  }

  mapSignalData(): Record<string, any> {
    if (this.config.signal?.maps) {
      const mapper = new MapperService(this.config.signal.maps, this.context);
      return mapper.mapRules();
    }
  }

  mapResolverData(): Record<string, any> {
    if (this.config.resolver?.maps) {
      const mapper = new MapperService(this.config.resolver.maps, this.context);
      return mapper.mapRules();
    }
  }

  /**
   * The signal activity will hook one
   */
  async hookOne(): Promise<string> {
    const topic = Pipe.resolve(this.config.topic, this.context);
    const signalInputData = this.mapSignalData();
    const status = Pipe.resolve(this.config.status, this.context);
    const code = Pipe.resolve(this.config.code, this.context);
    return await this.engine.hook(topic, signalInputData, status, code);
  }

  /**
   * The signal activity will hook all paused jobs that share the same job key.
   */
  async hookAll(): Promise<string[]> {
    //prep 1) generate `input signal data` (essentially the webhook payload)
    const signalInputData = this.mapSignalData();

    //prep 2) generate data that resolves the job key (per the YAML config)
    const keyResolverData = this.mapResolverData() as JobStatsInput;
    if (this.config.scrub) {
      //self-clean the indexes upon use if configured
      keyResolverData.scrub = true;
    }

    //prep 3) jobKeys can contain multiple indexes (per the YAML config)
    const key_name = Pipe.resolve(this.config.key_name, this.context);
    const key_value = Pipe.resolve(this.config.key_value, this.context);
    const indexQueryFacets = [`${key_name}:${key_value}`];

    //execute: `hookAll` will now resume all paused jobs that share the same job key
    return await this.engine.hookAll(
      this.config.topic,
      signalInputData,
      keyResolverData,
      indexQueryFacets,
    );
  }
}

export { Signal };
