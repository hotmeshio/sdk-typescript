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
  ActivityType,
  WorkerActivity,
} from '../../types/activity';
import { JobState } from '../../types/job';
import {
  ProviderTransaction,
  TransactionResultList,
} from '../../types/provider';
import { StreamData } from '../../types/stream';

import { Activity } from './activity';

/**
 * Dispatches work to a registered callback function. The `worker` activity
 * publishes a message to its configured `topic` stream, where a worker
 * process picks it up, executes the callback, and returns a response
 * that the engine captures as the activity's output.
 *
 * ## YAML Configuration
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: order.placed
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         a1:
 *           type: worker
 *           topic: work.do              # matches the registered worker topic
 *           input:
 *             schema:
 *               type: object
 *               properties:
 *                 x: { type: string }
 *             maps:
 *               x: '{t1.output.data.inputField}'
 *           output:
 *             schema:
 *               type: object
 *               properties:
 *                 y: { type: string }
 *           job:
 *             maps:
 *               result: '{$self.output.data.y}'
 *
 *       transitions:
 *         t1:
 *           - to: a1
 * ```
 *
 * ## Worker Registration (JavaScript)
 *
 * Workers are registered at initialization time via the `workers` array
 * in `HotMesh.init`. Each worker binds a `topic` to a `callback` function.
 *
 * ```typescript
 * const hotMesh = await HotMesh.init({
 *   appId: 'myapp',
 *   engine: { connection },
 *   workers: [{
 *     topic: 'work.do',
 *     connection,
 *     callback: async (data: StreamData) => ({
 *       metadata: { ...data.metadata },
 *       data: { y: `${data.data.x} transformed` }
 *     })
 *   }]
 * });
 * ```
 *
 * ## Retry Policy
 *
 * Retry behavior is configured at the **worker level** (not in YAML) via
 * the `retryPolicy` option. Failed callbacks are retried with exponential
 * backoff until `maximumAttempts` is exhausted. The `maximumInterval` caps
 * the delay between retries.
 *
 * ```typescript
 * const hotMesh = await HotMesh.init({
 *   appId: 'myapp',
 *   engine: { connection },
 *   workers: [{
 *     topic: 'work.backoff',
 *     connection,
 *     retryPolicy: {
 *       maximumAttempts: 5,          // retry up to 5 times
 *       backoffCoefficient: 2,       // exponential: 2^0, 2^1, 2^2, ... seconds
 *       maximumInterval: '30s',      // cap delay at 30 seconds
 *     },
 *     callback: async (data: StreamData) => {
 *       const result = await doWork(data.data);
 *       return {
 *         code: 200,
 *         status: StreamStatus.SUCCESS,
 *         metadata: { ...data.metadata },
 *         data: { result },
 *       } as StreamDataResponse;
 *     },
 *   }]
 * });
 * ```
 *
 * ## Execution Model
 *
 * Worker is a **Category A (duplex)** activity:
 * - **Leg 1** (`process`): Maps input data and publishes a message to the
 *   worker's topic stream.
 * - **Leg 2** (`processEvent`, inherited): Receives the worker's response,
 *   maps output data, and executes the step protocol to transition to
 *   adjacent activities.
 *
 * @see {@link WorkerActivity} for the TypeScript interface
 */
class Worker extends Activity {
  config: WorkerActivity;

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
    this.logger.debug('worker-process', {
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
      const txResponse = (await transaction.exec()) as TransactionResultList;

      //telemetry
      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(txResponse);
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
        this.logger.error('worker-get-state-error', { error });
        return;
      } else if (error instanceof CollationError) {
        if (error.fault === 'duplicate') {
          this.logger.info('worker-collation-overage', {
            job_id: this.context.metadata.jid,
            guid: this.context.metadata.guid,
          });
          return;
        }
        //unknown collation error
        this.logger.error('worker-collation-error', { error });
      } else {
        this.logger.error('worker-process-error', { error });
      }
      telemetry?.setActivityError(error.message);
      throw error;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('worker-process-end', {
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
        spn: this.context['$self'].output.metadata.l1s,
        trc: this.context.metadata.trc,
      },
      data: this.context.data,
    };
    if (this.config.retry) {
      streamData.policies = {
        retry: this.config.retry,
      };
    }
    return (await this.engine.router?.publishMessage(
      topic,
      streamData,
      transaction,
    )) as string;
  }
}

export { Worker };
