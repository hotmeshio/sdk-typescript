import { CollatorService } from '../collator';
import { Pipe } from '../pipe';
import { TaskService } from '../task';
import { TelemetryService } from '../telemetry';
import { HookActivity } from '../../types/activity';
import { HookRule } from '../../types/hook';
import { JobStatus } from '../../types/job';
import { ProviderTransaction } from '../../types/provider';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamStatus,
} from '../../types/stream';
import { CollationError } from '../../modules/errors';
import { CollationFaultType } from '../../types/collator';
import { guid, sleepFor } from '../../modules/utils';

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
      } else {
        //Category B: passthrough with crash-safe step protocol + GUID ledger
        await this.doPassThrough(telemetry);
      }

      return this.context.metadata.aid;
    } catch (error) {
      this.handleProcessError(error, telemetry, 'hook');
      return;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('hook-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
    }
  }

  isConfiguredAsHook(): boolean {
    return !!this.config.sleep || !!this.config.hook?.topic;
  }

  doesHook(): boolean {
    if (this.config.hook?.topic) {
      return true;
    }
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      return !isNaN(duration) && Number(duration) > 0;
    }
    return false;
  }

  async doHook(telemetry: TelemetryService): Promise<void> {
    const transaction = this.store.transact();
    const hookResult = await this.registerHook(transaction);
    this.mapOutputData();
    this.mapJobData();
    await this.setState(transaction);
    await CollatorService.notarizeLeg1Completion(this, transaction);
    await this.setStatus(0, transaction);
    await transaction.exec();
    telemetry.mapActivityAttributes();

    //if a pending signal was detected (signal arrived before hook
    //registered), re-publish the WEBHOOK so leg2 processes it
    //now that the hook signal is committed and state is saved
    if (hookResult && hookResult.pending) {
      await this.redeliverPendingSignal(hookResult.pending);
    }
  }

  /**
   * Re-publishes a pending signal as a WEBHOOK stream message so the
   * normal leg2 dispatch path processes it. Called when leg1's
   * setHookSignal atomically detected and consumed a pending signal.
   */
  private async redeliverPendingSignal(pendingJson: string): Promise<void> {
    const data = JSON.parse(pendingJson);
    const hookRule = await this.getHookRule(this.config.hook.topic);
    this.logger.warn('hook-pending-signal-redelivery', {
      topic: this.config.hook.topic,
      aid: hookRule?.to || this.metadata.aid,
      jid: this.context.metadata.jid,
    });
    const streamData: StreamData = {
      type: StreamDataType.WEBHOOK,
      status: StreamStatus.SUCCESS,
      code: 200,
      metadata: {
        guid: guid(),
        aid: hookRule?.to || this.metadata.aid,
        topic: this.config.hook.topic,
      },
      data,
    };
    await this.engine.router?.publishMessage(null, streamData);
  }

  async doPassThrough(telemetry: TelemetryService): Promise<void> {
    this.adjacencyList = await this.filterAdjacent();
    this.mapOutputData();
    this.mapJobData();
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
  ): Promise<{ jobId?: string; pending?: string } | void> {
    let jobId: string | undefined;
    let pending: string | undefined;
    if (this.config.hook?.topic) {
      //hook signal is set standalone (not in the transaction) so the
      //single CTE query can atomically detect a pending signal collision
      const hookResult = await this.engine.taskService.registerWebHook(
        this.config.hook.topic,
        this.context,
        this.resolveDad(),
        this.context.metadata.expire,
      );
      jobId = hookResult.jobId;
      pending = hookResult.pending;
    }
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      if (!isNaN(duration) && Number(duration) > 0) {
        await this.engine.taskService.registerTimeHook(
          this.context.metadata.jid,
          this.context.metadata.gid,
          `${this.metadata.aid}${this.metadata.dad || ''}`,
          'sleep',
          duration,
          this.metadata.dad || '',
          transaction,
        );
        if (!jobId) jobId = this.context.metadata.jid;
      }
    }
    if (jobId) return { jobId, pending };
  }

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

      // Inline retry for FORBIDDEN: Leg2 arrived in the window between
      // setHookSignal (standalone) and Leg1 transaction.exec(). The 100B
      // ledger digit is not yet visible. Leg1 needs only milliseconds to
      // commit — retry here, inside the message processing loop, before
      // consumeOne's finally block acks the message. Stream-level retry
      // won't help: ENGINE consumers have no retry policy, so shouldRetry
      // returns [false, 0] and the message is ack'd with no retry.
      const MAX_FORBIDDEN_RETRIES = 5;
      const FORBIDDEN_RETRY_DELAY_MS = 50;
      for (let attempt = 0; attempt <= MAX_FORBIDDEN_RETRIES; attempt++) {
        try {
          await this.processEvent(status, code, 'hook');
          if (code === 200) {
            await taskService.deleteWebHookSignal(this.config.hook.topic, data);
          }
          return;
        } catch (error) {
          if (error instanceof CollationError &&
              error.fault === CollationFaultType.FORBIDDEN &&
              attempt < MAX_FORBIDDEN_RETRIES) {
            this.logger.warn('hook-webhook-forbidden-inline-retry', {
              attempt: attempt + 1,
              maxAttempts: MAX_FORBIDDEN_RETRIES,
              jid: this.context.metadata.jid,
              aid: this.metadata.aid,
            });
            await sleepFor(FORBIDDEN_RETRY_DELAY_MS * (attempt + 1));
            continue;
          }
          throw error;
        }
      }
    }
  }

  async processTimeHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('hook-process-time-hook-event', {
      jid: jobId,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    //if combined hook+sleep, clean up the webhook signal (timeout won the race)
    if (this.config.hook?.topic) {
      try {
        const taskService = new TaskService(this.store, this.logger);
        await taskService.deleteWebHookSignal(
          this.config.hook.topic,
          this.data,
        );
      } catch (e) {
        this.logger.debug('hook-timeout-signal-cleanup', {
          topic: this.config.hook.topic,
          error: e.message,
        });
      }
    }
    await this.processEvent(StreamStatus.SUCCESS, 200, 'hook');
  }
}

export { Hook };
