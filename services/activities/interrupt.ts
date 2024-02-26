import { EngineService } from '../engine';
import { Activity, ActivityType } from './activity';
import {
  ActivityData,
  ActivityMetadata,
  InterruptActivity } from '../../types/activity';
import { GetStateError, InactiveJobError } from '../../modules/errors';
import { MultiResponseFlags } from '../../types';
import { CollatorService } from '../collator';
import { JobInterruptOptions, JobState } from '../../types/job';
import { TelemetryService } from '../telemetry';
import { Pipe } from '../pipe';

class Interrupt extends Activity {
  config: InterruptActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState
    ) {
      super(config, data, metadata, hook, engine, context);
  }


  //********  LEG 1 ENTRY  ********//
  async process(): Promise<string> {
    this.logger.debug('interrupt-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    let telemetry: TelemetryService;
    try {
      this.setLeg(1);
      await CollatorService.notarizeEntry(this);
      await this.getState();
      CollatorService.assertJobActive(this.context.metadata.js, this.context.metadata.jid, this.metadata.aid); // Ensure job active
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
  
      if (this.isInterruptingSelf()) {
        return await this.interruptSelf(telemetry);
      } else {
        return await this.interruptAnother(telemetry);
      }
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('interrupt-inactive-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('interrupt-get-state-error', { error });
        return;
      } else {
        this.logger.error('interrupt-process-error', { error });
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('interrupt-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }

  async interruptSelf(telemetry: TelemetryService): Promise<string> {
    // Apply final updates to THIS job's state
    if (this.config.job?.maps) {
      this.mapJobData();
      await this.setState();
    }
  
    // Interrupt THIS job
    const messageId = await this.interrupt();
  
    // Notarize completion and log
    telemetry.mapActivityAttributes();
    const multi = this.store.getMulti();
    await CollatorService.notarizeEarlyCompletion(this, multi);
    await this.setStatus(-1, multi);
    const multiResponse = await multi.exec() as MultiResponseFlags;
    const jobStatus = this.resolveStatus(multiResponse);
    telemetry.setActivityAttributes({
      'app.activity.mid': messageId,
      'app.job.jss': jobStatus
    });
  
    return this.context.metadata.aid;
  }

  async interruptAnother(telemetry: TelemetryService): Promise<string> {
    // Interrupt ANOTHER job
    const messageId = await this.interrupt();
    const attrs = { 'app.activity.mid': messageId };
  
    // Apply updates to THIS job's state
    telemetry.mapActivityAttributes();
    this.adjacencyList = await this.filterAdjacent();
    if (this.config.job?.maps || this.config.output?.maps) {
      this.mapOutputData();
      this.mapJobData();
      const multi = this.store.getMulti();
      await this.setState(multi);
    }
  
    // Notarize completion
    const multi = this.store.getMulti();
    await CollatorService.notarizeEarlyCompletion(this, multi);
    await this.setStatus(this.adjacencyList.length - 1, multi);
    const multiResponse = await multi.exec() as MultiResponseFlags;
    const jobStatus = this.resolveStatus(multiResponse);
    attrs['app.job.jss'] = jobStatus;
  
    // Transition next generation and log
    const messageIds = await this.transition(this.adjacencyList, jobStatus);
    if (messageIds.length) {
      attrs['app.activity.mids'] = messageIds.join(',');
    }
    telemetry.setActivityAttributes(attrs);
  
    return this.context.metadata.aid;
  }
  

  isInterruptingSelf(): boolean {
    if (!this.config.target) {
      return true;
    }
    const resolvedJob = Pipe.resolve(this.config.target, this.context);
    return resolvedJob == this.context.metadata.jid;
  }

  resolveInterruptOptions(): JobInterruptOptions {
    return {
      reason: this.config.reason !== undefined 
        ? Pipe.resolve(this.config.reason, this.context) 
        : undefined,
      throw: this.config.throw !== undefined 
        ? Pipe.resolve(this.config.throw, this.context) 
        : undefined,
      descend: this.config.descend !== undefined 
        ? Pipe.resolve(this.config.descend, this.context) 
        : undefined,
    };
  }

  async interrupt(): Promise<string> {
    const options = this.resolveInterruptOptions();
    return await this.engine.interrupt(
      this.config.topic !== undefined 
        ? Pipe.resolve(this.config.topic, this.context) 
        : this.context.metadata.tpc,
      this.config.target !== undefined 
        ? Pipe.resolve(this.config.target, this.context) 
        : this.context.metadata.jid,
      options as JobInterruptOptions,
    );
  }
}

export { Interrupt };
