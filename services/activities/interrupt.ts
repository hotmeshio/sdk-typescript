import {
  GenerationalError,
  GetStateError,
  InactiveJobError } from '../../modules/errors';
import { CollatorService } from '../collator';
import { Activity } from './activity';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  InterruptActivity } from '../../types/activity';
import { JobInterruptOptions, JobState } from '../../types/job';
import { MultiResponseFlags } from '../../types/redis';

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
    this.logger.debug('interrupt-process', { jid: this.context.metadata.jid, gid: this.context.metadata.gid, aid: this.metadata.aid });
    let telemetry: TelemetryService;
    try {
      await this.verifyEntry();

      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
  
      if (this.isInterruptingSelf()) {
        await this.interruptSelf(telemetry);
      } else {
        await this.interruptAnother(telemetry);
      }
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('interrupt-inactive-job-error', { ...error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { ...error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('interrupt-get-state-error', { ...error });
        return;
      } else {
        this.logger.error('interrupt-process-error', { ...error });
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('interrupt-process-end', { jid: this.context.metadata.jid, gid: this.context.metadata.gid, aid: this.metadata.aid });
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
      code: this.config.code !== undefined 
        ? Pipe.resolve(this.config.code, this.context) 
        : undefined,
      expire: this.config.expire !== undefined
        ? Pipe.resolve(this.config.expire, this.context)
        : undefined,
      stack: this.config.stack !== undefined
        ? Pipe.resolve(this.config.stack, this.context)
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
