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

/**
 * Sends a signal to one or more paused flows, resuming their execution.
 * The `signal` activity is the counterpart to a `Hook` activity
 * configured with a webhook listener. It allows any flow to reach into
 * another flow and deliver data to a waiting hook, regardless of the
 * relationship between the flows.
 *
 * ## YAML Configuration — Signal One
 *
 * Resumes a single paused flow by publishing to the hook's topic. Use
 * `subtype: one` when you know the specific hook topic to signal.
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: signal.start
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         resume_hook:
 *           type: signal
 *           subtype: one
 *           topic: my.hook.topic          # the hook's registered topic
 *           status: success               # optional: success (default) or pending
 *           code: 200                     # optional: 200 (default) or 202 (keep-alive)
 *           signal:
 *             schema:
 *               type: object
 *               properties:
 *                 approved: { type: boolean }
 *             maps:
 *               approved: true            # data delivered to the hook
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: resume_hook
 *         resume_hook:
 *           - to: done
 * ```
 *
 * A `code: 202` signal delivers data but keeps the hook alive for
 * additional signals. A `code: 200` (default) closes the hook.
 *
 * ## YAML Configuration — Signal All
 *
 * Resumes all paused flows that share a common job key facet. Use
 * `subtype: all` for fan-out patterns where multiple waiting flows
 * should be resumed simultaneously.
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: signal.fan.out
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         resume_all:
 *           type: signal
 *           subtype: all
 *           topic: hook.resume
 *           key_name: parent_job_id       # index facet name
 *           key_value: '{$job.metadata.jid}'
 *           scrub: true                   # clean up indexes after use
 *           resolver:
 *             maps:
 *               data:
 *                 parent_job_id: '{$job.metadata.jid}'
 *               scrub: true
 *           signal:
 *             maps:
 *               done: true                # data delivered to all matching hooks
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: resume_all
 *         resume_all:
 *           - to: done
 * ```
 *
 * ## Execution Model
 *
 * Signal is a **Category B (Leg1-only with children)** activity:
 * - Bundles the hook signal with the Leg 1 completion marker in a
 *   single transaction (`hookOne`) or fires best-effort (`hookAll`).
 * - Executes the crash-safe `executeLeg1StepProtocol` to transition
 *   to adjacent activities.
 *
 * @see {@link SignalActivity} for the TypeScript interface
 */
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
          await this.signalAll();
        } else {
          await this.signalOne(txn1);
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
  async signalOne(transaction?: ProviderTransaction): Promise<string> {
    const topic = Pipe.resolve(this.config.topic, this.context);
    const signalInputData = this.mapSignalData();
    const status = Pipe.resolve(this.config.status, this.context);
    const code = Pipe.resolve(this.config.code, this.context);
    return await this.engine.signal(topic, signalInputData, status, code, transaction);
  }

  /**
   * Signals all paused jobs that share the same job key, resuming their execution.
   */
  async signalAll(): Promise<string[]> {
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

    //execute: `signalAll` will now resume all paused jobs that share the same job key
    return await this.engine.signalAll(
      this.config.topic,
      signalInputData,
      keyResolverData,
      indexQueryFacets,
    );
  }
}

export { Signal };
