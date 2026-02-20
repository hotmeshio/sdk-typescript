import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { guid } from '../../modules/utils';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  CycleActivity,
} from '../../types/activity';
import {
  ProviderTransaction,
  TransactionResultList,
} from '../../types/provider';
import { JobState } from '../../types/job';
import { StreamData } from '../../types/stream';

import { Activity } from './activity';

/**
 * Re-executes an ancestor activity in a new dimensional thread, enabling
 * retry loops and iterative patterns without violating the DAG constraint.
 * The `cycle` activity targets a specific ancestor (typically a
 * `Hook` with `cycle: true`) and sends execution back to that point.
 *
 * Each cycle iteration runs in a fresh **dimensional thread** — individual
 * activity state is isolated per iteration, while **shared job state**
 * (`job.maps`) accumulates across iterations. This pattern enables retries,
 * polling loops, and iterative processing.
 *
 * ## YAML Configuration
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: retry.start
 *       expire: 300
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         pivot:
 *           type: hook
 *           cycle: true                    # marks this activity as a cycle target
 *           output:
 *             maps:
 *               retryCount: 0
 *
 *         do_work:
 *           type: worker
 *           topic: work.do
 *           output:
 *             schema:
 *               type: object
 *               properties:
 *                 result: { type: string }
 *
 *         retry:
 *           type: cycle
 *           ancestor: pivot                # re-execute from this activity
 *           input:
 *             maps:
 *               retryCount:                # increment retry counter each cycle
 *                 '@pipe':
 *                   - ['{pivot.output.data.retryCount}', 1]
 *                   - ['{@math.add}']
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: pivot
 *         pivot:
 *           - to: do_work
 *         do_work:
 *           - to: retry
 *             conditions:
 *               code: 500                  # cycle on error
 *           - to: done
 * ```
 *
 * ## Key Behaviors
 *
 * - The `ancestor` field must reference an activity with `cycle: true`.
 * - The cycle activity's `input.maps` override the ancestor's output data
 *   for the next iteration, allowing each cycle to pass different values.
 * - Dimensional isolation ensures parallel cycle iterations don't collide.
 *
 * ## Execution Model
 *
 * Cycle is a **Category A (Leg 1 only)** activity:
 * - Maps input data, resolves the re-entry dimensional address, and
 *   publishes a stream message addressed to the ancestor activity.
 * - The ancestor re-enters via its Leg 2 path in the new dimension.
 *
 * @see {@link CycleActivity} for the TypeScript interface
 */
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

      //set state/status, cycle ancestor, and mark Leg1 complete — single transaction
      const transaction = this.store.transact();
      await this.setState(transaction);
      await this.setStatus(0, transaction); //leg 1 never changes job status
      const messageId = await this.cycleAncestorActivity(transaction);
      await CollatorService.notarizeLeg1Completion(this, transaction);
      const txResponse = (await transaction.exec()) as TransactionResultList;
      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(txResponse);
      telemetry.setActivityAttributes({
        'app.activity.mid': messageId,
        'app.job.jss': jobStatus,
      });

      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('cycle-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('cycle-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('cycle-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('cycle-collation-error', { error });
      } else {
        this.logger.error('cycle-process-error', { error });
      }
      telemetry?.setActivityError(error.message);
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
  async cycleAncestorActivity(
    transaction: ProviderTransaction,
  ): Promise<string> {
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
      transaction,
    )) as string;
  }
}

export { Cycle };
