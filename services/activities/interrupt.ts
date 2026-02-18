import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  InterruptActivity,
} from '../../types/activity';
import { TransactionResultList } from '../../types/provider';
import { JobInterruptOptions, JobState } from '../../types/job';

import { Activity } from './activity';

class Interrupt extends Activity {
  config: InterruptActivity;

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
    this.logger.debug('interrupt-process', {
      jid: this.context.metadata.jid,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    let telemetry: TelemetryService;
    try {
      if (!this.config.target) {
        //Category C: self-interrupt (no children, no semaphore edge risk)
        await this.verifyEntry();
        telemetry = new TelemetryService(
          this.engine.appId,
          this.config,
          this.metadata,
          this.context,
        );
        telemetry.startActivitySpan(this.leg);
        await this.interruptSelf(telemetry);
      } else {
        //Category B: interrupt another (spawns children, needs step protocol)
        await this.verifyLeg1Entry();
        telemetry = new TelemetryService(
          this.engine.appId,
          this.config,
          this.metadata,
          this.context,
        );
        telemetry.startActivitySpan(this.leg);
        await this.interruptAnother(telemetry);
      }
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('interrupt-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('interrupt-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('interrupt-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('interrupt-collation-error', { error });
      } else {
        this.logger.error('interrupt-process-error', { error });
      }
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('interrupt-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
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

    // Notarize Leg1 completion and set status
    telemetry.mapActivityAttributes();
    const transaction = this.store.transact();
    await CollatorService.notarizeLeg1Completion(this, transaction);
    await this.setStatus(-1, transaction);
    const txResponse = (await transaction.exec()) as TransactionResultList;
    const jobStatus = this.resolveStatus(txResponse);
    telemetry.setActivityAttributes({
      'app.activity.mid': messageId,
      'app.job.jss': jobStatus,
    });

    return this.context.metadata.aid;
  }

  async interruptAnother(telemetry: TelemetryService): Promise<string> {
    // Interrupt ANOTHER job (best-effort, fires before step protocol)
    await this.interrupt();

    // Apply updates to THIS job's state
    this.adjacencyList = await this.filterAdjacent();
    if (this.config.job?.maps || this.config.output?.maps) {
      this.mapOutputData();
      this.mapJobData();
    }

    //Category B: use Leg1 step protocol for crash-safe edge capture
    await this.executeLeg1StepProtocol(this.adjacencyList.length - 1);

    telemetry.mapActivityAttributes();
    telemetry.setActivityAttributes({});

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
      reason:
        this.config.reason !== undefined
          ? Pipe.resolve(this.config.reason, this.context)
          : undefined,
      throw:
        this.config.throw !== undefined
          ? Pipe.resolve(this.config.throw, this.context)
          : undefined,
      descend:
        this.config.descend !== undefined
          ? Pipe.resolve(this.config.descend, this.context)
          : undefined,
      code:
        this.config.code !== undefined
          ? Pipe.resolve(this.config.code, this.context)
          : undefined,
      expire:
        this.config.expire !== undefined
          ? Pipe.resolve(this.config.expire, this.context)
          : undefined,
      stack:
        this.config.stack !== undefined
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
