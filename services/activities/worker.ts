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
import {
  ProviderTransaction,
  TransactionResultList,
} from '../../types/hotmesh';
import { JobState } from '../../types/job';
import { StreamData } from '../../types/stream';

import { Activity } from './activity';

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

      //save state and authorize reentry
      const transaction = this.store.transact();
      //todo: await this.registerTimeout();
      const messageId = await this.execActivity(transaction);
      await CollatorService.authorizeReentry(this, transaction);
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
        this.logger.error('await-inactive-job-error', { ...error });
        return;
      } else if (error instanceof GenerationalError) {
        this.logger.info('process-event-generational-job-error', { ...error });
        return;
      } else if (error instanceof GetStateError) {
        this.logger.error('worker-get-state-error', { ...error });
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
        this.logger.error('worker-collation-error', { ...error });
      } else {
        this.logger.error('worker-process-error', { ...error });
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
