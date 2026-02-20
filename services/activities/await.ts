import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../modules/errors';
import { guid } from '../../modules/utils';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import { Pipe } from '../pipe';
import { TelemetryService } from '../telemetry';
import {
  ActivityData,
  ActivityMetadata,
  AwaitActivity,
  ActivityType,
} from '../../types/activity';
import {
  ProviderTransaction,
  TransactionResultList,
} from '../../types/provider';
import { JobState } from '../../types/job';
import { StreamData, StreamDataType } from '../../types/stream';

import { Activity } from './activity';

/**
 * Invokes another graph (sub-flow) and optionally waits for its completion.
 * The `await` activity enables compositional workflows where one graph
 * triggers another by publishing to its `subscribes` topic, creating a
 * parent-child relationship between flows.
 *
 * ## YAML Configuration
 *
 * The `topic` in the await activity must match the `subscribes` topic of
 * the child graph. Both graphs are defined in the same app YAML:
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *
 *     # ── Parent graph ──────────────────────────────
 *     - subscribes: order.placed
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *           job:
 *             maps:
 *               orderId: '{$self.output.data.id}'
 *
 *         a1:
 *           type: await
 *           topic: approval.requested  # ◄── targets the child graph's subscribes
 *           await: true
 *           input:
 *             schema:
 *               type: object
 *               properties:
 *                 orderId: { type: string }
 *             maps:
 *               orderId: '{t1.output.data.id}'
 *           output:
 *             schema:
 *               type: object
 *               properties:
 *                 approved: { type: boolean }
 *           job:
 *             maps:
 *               approval: '{$self.output.data.approved}'
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: a1
 *         a1:
 *           - to: done
 *
 *     # ── Child graph (invoked by the await) ────────
 *     - subscribes: approval.requested  # ◄── matched by the await activity's topic
 *       publishes: approval.completed
 *       expire: 60
 *
 *       activities:
 *         t1:
 *           type: trigger
 *         review:
 *           type: worker
 *           topic: approval.review
 *
 *       transitions:
 *         t1:
 *           - to: review
 * ```
 *
 * ## Fire-and-Forget Mode
 *
 * When `await` is explicitly set to `false`, the activity starts the child
 * flow but does not wait for its completion. The parent flow immediately
 * continues. The child's `job_id` is returned as the output.
 *
 * ```yaml
 * a1:
 *   type: await
 *   topic: background.process
 *   await: false
 *   job:
 *     maps:
 *       childJobId: '{$self.output.data.job_id}'
 * ```
 *
 * ## Execution Model
 *
 * Await is a **Category A (duplex)** activity:
 * - **Leg 1** (`process`): Maps input data and publishes a
 *   `StreamDataType.AWAIT` message to the engine stream. The engine
 *   starts the child flow.
 * - **Leg 2** (`processEvent`, inherited): Receives the child flow's
 *   final output, maps output data, and transitions to adjacent activities.
 *
 * @see {@link AwaitActivity} for the TypeScript interface
 */
class Await extends Activity {
  config: AwaitActivity;

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
    this.logger.debug('await-process', {
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

      //save state and mark Leg1 complete
      const transaction = this.store.transact();
      //todo: await this.registerTimeout();
      const messageId = await this.execActivity(transaction);
      await CollatorService.notarizeLeg1Completion(this, transaction);
      await this.setState(transaction);
      await this.setStatus(0, transaction);
      const multiResponse = (await transaction.exec()) as TransactionResultList;

      //telemetry
      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);
      telemetry.setActivityAttributes({
        'app.activity.mid': messageId,
        'app.job.jss': jobStatus,
      });
      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof InactiveJobError) {
        this.logger.error('await-inactive-job-error', { error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('await-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('await-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('await-collation-error', { error });
      } else {
        this.logger.error('await-process-error', { error });
      }
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('await-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
    }
  }

  async execActivity(transaction: ProviderTransaction): Promise<string> {
    const topic = Pipe.resolve(this.config.subtype, this.context);
    const streamData: StreamData = {
      metadata: {
        guid: guid(),
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        dad: this.metadata.dad,
        aid: this.metadata.aid,
        topic,
        spn: this.context['$self'].output.metadata?.l1s,
        trc: this.context.metadata.trc,
      },
      type: StreamDataType.AWAIT,
      data: this.context.data,
    };
    if (this.config.await !== true) {
      const doAwait = Pipe.resolve(this.config.await, this.context);
      if (doAwait === false) {
        streamData.metadata.await = false;
      }
    }
    if (this.config.retry) {
      streamData.policies = {
        retry: this.config.retry,
      };
    }
    return (await this.engine.router?.publishMessage(
      null,
      streamData,
      transaction,
    )) as string;
  }
}

export { Await };
