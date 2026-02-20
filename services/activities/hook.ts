import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { TaskService } from '../task';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  HookActivity,
} from '../../types/activity';
import { HookRule } from '../../types/hook';
import { JobState, JobStatus } from '../../types/job';
import { ProviderTransaction } from '../../types/provider';
import { StreamCode, StreamStatus } from '../../types/stream';

import { Activity } from './activity';

/**
 * A versatile pause/resume activity that supports three distinct patterns:
 * **time hook** (sleep), **web hook** (external signal), and **passthrough**
 * (immediate transition with optional data mapping).
 *
 * The hook activity is the most flexible activity type. Depending on its
 * YAML configuration, it operates in one of the following modes:
 *
 * ## Time Hook (Sleep)
 *
 * Pauses the flow for a specified duration in seconds. The `sleep` value
 * can be a literal number or a `@pipe` expression for dynamic delays
 * (e.g., exponential backoff).
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: job.start
 *       expire: 300
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         delay:
 *           type: hook
 *           sleep: 60                        # pause for 60 seconds
 *           job:
 *             maps:
 *               paused_at: '{$self.output.metadata.ac}'
 *
 *         resume:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: delay
 *         delay:
 *           - to: resume
 * ```
 *
 * ## Web Hook (External Signal)
 *
 * Registers a webhook listener on a named topic. The flow pauses until
 * an external signal is sent to the hook's topic. The signal data becomes
 * available as `$self.hook.data`. The `hooks` section at the graph level
 * routes incoming signals to the waiting activity.
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: order.placed
 *       expire: 3600
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         wait_for_approval:
 *           type: hook
 *           hook:
 *             type: object
 *             properties:
 *               approved: { type: boolean }
 *           job:
 *             maps:
 *               approved: '{$self.hook.data.approved}'
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: wait_for_approval
 *         wait_for_approval:
 *           - to: done
 *
 *       hooks:
 *         order.approval:                    # external topic that delivers the signal
 *           - to: wait_for_approval
 *             conditions:
 *               match:
 *                 - expected: '{t1.output.data.id}'
 *                   actual: '{$self.hook.data.id}'
 * ```
 *
 * ## Passthrough (No Hook)
 *
 * When neither `sleep` nor `hook` is configured, the hook activity acts
 * as a passthrough: it maps data and immediately transitions to children.
 * This is useful for data transformation, convergence points, or as a
 * cycle pivot (with `cycle: true`).
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: job.start
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         pivot:
 *           type: hook
 *           cycle: true                      # enables re-entry from a cycle activity
 *           output:
 *             maps:
 *               retryCount: 0
 *           job:
 *             maps:
 *               counter: '{$self.output.data.retryCount}'
 *
 *         do_work:
 *           type: worker
 *           topic: work.do
 *
 *       transitions:
 *         t1:
 *           - to: pivot
 *         pivot:
 *           - to: do_work
 * ```
 *
 * ## Execution Model
 *
 * - **With `sleep` or `hook`**: Category A (duplex). Leg 1 registers the
 *   hook and saves state. Leg 2 fires when the timer expires or the
 *   external signal arrives (via `processTimeHookEvent` or
 *   `processWebHookEvent`).
 * - **Without `sleep` or `hook`**: Category B (passthrough). Uses the
 *   crash-safe `executeLeg1StepProtocol` to map data and transition
 *   to adjacent activities.
 *
 * @see {@link HookActivity} for the TypeScript interface
 */
class Hook extends Activity {
  config: HookActivity;

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

  //********  INITIAL ENTRY POINT (A)  ********//
  async process(): Promise<string> {
    this.logger.debug('hook-process', {
      jid: this.context.metadata.jid,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    let telemetry: TelemetryService;

    try {
      //Phase 1: Load state and verify entry (all paths use verifyLeg1Entry
      //for GUID-ledger-backed crash recovery)
      const isResume = await this.verifyLeg1Entry();

      telemetry = new TelemetryService(
        this.engine.appId,
        this.config,
        this.metadata,
        this.context,
      );
      telemetry.startActivitySpan(this.leg);

      //Phase 2: Route based on RUNTIME evaluation (not static config)
      if (this.isConfiguredAsHook() && this.doesHook()) {
        //Category A: duplexed hook registration (Leg2 handles completion)
        if (!isResume) {
          await this.doHook(telemetry);
        }
        //If resume, Leg1 already ran — Leg2 will handle completion
      } else {
        //Category B: passthrough with crash-safe step protocol + GUID ledger
        await this.doPassThrough(telemetry);
      }

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('hook-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('hook-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('hook-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('hook-collation-error', { error });
      } else {
        this.logger.error('hook-process-error', { error });
      }
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('hook-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
    }
  }

  /**
   * Static config check: does this activity have a hook or sleep config?
   * Used for routing before context is loaded.
   */
  isConfiguredAsHook(): boolean {
    return !!this.config.sleep || !!this.config.hook?.topic;
  }

  /**
   * does this activity use a time-hook or web-hook
   */
  doesHook(): boolean {
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      return !isNaN(duration) && Number(duration) > 0;
    }
    return !!this.config.hook?.topic;
  }

  async doHook(telemetry: TelemetryService) {
    const transaction = this.store.transact();
    await this.registerHook(transaction);
    this.mapOutputData();
    this.mapJobData();
    await this.setState(transaction);
    await CollatorService.notarizeLeg1Completion(this, transaction);

    await this.setStatus(0, transaction);
    await transaction.exec();
    telemetry.mapActivityAttributes();
  }

  async doPassThrough(telemetry: TelemetryService) {
    this.adjacencyList = await this.filterAdjacent();
    this.mapOutputData();
    this.mapJobData();

    //Category B: use Leg1 step protocol for crash-safe edge capture
    await this.executeLeg1StepProtocol(this.adjacencyList.length - 1);
    telemetry.mapActivityAttributes();
    telemetry.setActivityAttributes({});
  }

  async getHookRule(topic: string): Promise<HookRule | undefined> {
    const rules = await this.store.getHookRules();
    return rules?.[topic]?.[0] as HookRule;
  }

  async registerHook(
    transaction?: ProviderTransaction,
  ): Promise<string | void> {
    if (this.config.hook?.topic) {
      return await this.engine.taskService.registerWebHook(
        this.config.hook.topic,
        this.context,
        this.resolveDad(),
        this.context.metadata.expire,
        transaction,
      );
    } else if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      await this.engine.taskService.registerTimeHook(
        this.context.metadata.jid,
        this.context.metadata.gid,
        `${this.metadata.aid}${this.metadata.dad || ''}`,
        'sleep',
        duration,
        this.metadata.dad || '',
        transaction,
      );
      return this.context.metadata.jid;
    }
  }

  //********  SIGNAL RE-ENTRY POINT  ********//
  async processWebHookEvent(
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
  ): Promise<JobStatus | void> {
    this.logger.debug('hook-process-web-hook-event', {
      topic: this.config.hook.topic,
      aid: this.metadata.aid,
      status,
      code,
    });
    const taskService = new TaskService(this.store, this.logger);
    const data = { ...this.data };
    const signal = await taskService.processWebHookSignal(
      this.config.hook.topic,
      data,
    );
    if (signal) {
      const [jobId, _aid, dad, gId] = signal;
      this.context.metadata.jid = jobId;
      this.context.metadata.gid = gId;
      this.context.metadata.dad = dad;
      await this.processEvent(status, code, 'hook');
      if (code === 200) {
        await taskService.deleteWebHookSignal(this.config.hook.topic, data);
      } //else => 202/keep alive
    } //else => already resolved
  }

  async processTimeHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('hook-process-time-hook-event', {
      jid: jobId,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    await this.processEvent(StreamStatus.SUCCESS, 200, 'hook');
  }
}

export { Hook };
