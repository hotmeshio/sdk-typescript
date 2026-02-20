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

/**
 * Terminates a flow by sending an interrupt signal. The `interrupt` activity
 * can target the current flow (self-interrupt) or any other flow by its
 * job ID (remote interrupt). Interrupted jobs have their status set to a
 * value less than -100,000,000, indicating abnormal termination.
 *
 * ## YAML Configuration — Self-Interrupt
 *
 * When no `target` is specified, the activity interrupts the current flow.
 * The flow terminates immediately after this activity executes. Use
 * conditional transitions to route to an interrupt only when needed.
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: validation.check
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         validate:
 *           type: worker
 *           topic: validate.input
 *
 *         cancel:
 *           type: interrupt
 *           reason: 'Validation failed'
 *           throw: true
 *           code: 410
 *           job:
 *             maps:
 *               cancelled_at: '{$self.output.metadata.ac}'
 *
 *         proceed:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: validate
 *         validate:
 *           - to: cancel
 *             conditions:
 *               code: 422               # interrupt only on validation failure
 *           - to: proceed
 * ```
 *
 * ## YAML Configuration — Remote Interrupt
 *
 * When `target` is specified, the activity interrupts another flow while
 * the current flow continues to transition to adjacent activities.
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: parent.flow
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *           job:
 *             maps:
 *               childJobId: '{$self.output.data.childJobId}'
 *
 *         stop_child:
 *           type: interrupt
 *           topic: child.flow.topic     # topic of the target flow
 *           target: '{t1.output.data.childJobId}'
 *           throw: false                # do not throw (silent cancellation)
 *           descend: true               # also interrupt descendant sub-flows
 *           job:
 *             maps:
 *               interrupted: true
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: stop_child
 *         stop_child:
 *           - to: done
 * ```
 *
 * ## Configuration Properties
 *
 * | Property   | Type    | Default            | Description |
 * |------------|---------|--------------------|-------------|
 * | `target`   | string  | (current job)      | Job ID to interrupt. Supports `@pipe` expressions. |
 * | `topic`    | string  | (current topic)    | Topic of the target flow |
 * | `reason`   | string  | `'Job Interrupted'`| Error message attached to the interruption |
 * | `throw`    | boolean | `true`             | Whether to throw a `JobInterrupted` error |
 * | `descend`  | boolean | `false`            | Whether to cascade to child/descendant flows |
 * | `code`     | number  | `410`              | Error code attached to the interruption |
 * | `stack`    | string  | —                  | Optional stack trace |
 *
 * ## Execution Model
 *
 * - **Self-interrupt (no `target`)**: Category C. Verifies entry, maps job
 *   data, sets status to -1, and fires the interrupt. No children.
 * - **Remote interrupt (with `target`)**: Category B. Fires the interrupt
 *   best-effort, then uses `executeLeg1StepProtocol` to transition to
 *   adjacent activities.
 *
 * @see {@link InterruptActivity} for the TypeScript interface
 */
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
    if (this.config.job?.maps) {
      this.mapJobData();
    }

    // Bundle state + Leg1 completion + semaphore in one transaction
    telemetry.mapActivityAttributes();
    const transaction = this.store.transact();
    if (this.config.job?.maps) {
      await this.setState(transaction);
    }
    await CollatorService.notarizeLeg1Completion(this, transaction);
    await this.setStatus(-1, transaction);
    const txResponse = (await transaction.exec()) as TransactionResultList;
    const jobStatus = this.resolveStatus(txResponse);

    // Interrupt fires AFTER proof commits (best-effort)
    const messageId = await this.interrupt();
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
