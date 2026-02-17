import {
  CollationError,
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
import { ProviderTransaction } from '../../types/provider';
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
      //Category B: entry with step resume
      await this.verifyLeg1Entry();

      telemetry = new TelemetryService(
        this.engine.appId,
        this.config,
        this.metadata,
        this.context,
      );
      telemetry.startActivitySpan(this.leg);

      this.adjacencyList = await this.filterAdjacent();
      this.mapOutputData();
      this.mapJobData();

      //Step A: Bundle signal hook with Leg1 completion marker.
      //hookOne is transactional; hookAll is best-effort (complex multi-step).
      if (!CollatorService.isGuidStep1Done(this.guidLedger)) {
        const txn1 = this.store.transact();
        if (this.config.subtype === 'all') {
          await this.hookAll();
        } else {
          await this.hookOne(txn1);
        }
        await this.setState(txn1);
        if (this.adjacentIndex === 0) {
          await CollatorService.notarizeLeg1Completion(this, txn1);
        }
        await CollatorService.notarizeStep1(this, this.context.metadata.guid, txn1);
        await txn1.exec();
        //update in-memory guidLedger so executeLeg1StepProtocol skips Step A
        this.guidLedger += CollatorService.WEIGHTS.STEP1_WORK;
      }

      //Steps B and C: spawn children + semaphore + edge capture
      await this.executeLeg1StepProtocol(this.adjacencyList.length - 1);

      telemetry.mapActivityAttributes();
      telemetry.setActivityAttributes({});

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('signal-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('signal-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('signal-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('signal-collation-error', { error });
      } else {
        this.logger.error('signal-process-error', { error });
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
   * The signal activity will hook one. Accepts an optional transaction
   * so the hook publish can be bundled with the Leg1 completion marker.
   */
  async hookOne(transaction?: ProviderTransaction): Promise<string> {
    const topic = Pipe.resolve(this.config.topic, this.context);
    const signalInputData = this.mapSignalData();
    const status = Pipe.resolve(this.config.status, this.context);
    const code = Pipe.resolve(this.config.code, this.context);
    return await this.engine.hook(topic, signalInputData, status, code, transaction);
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
