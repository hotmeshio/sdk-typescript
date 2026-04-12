import { HMSH_CODE_DURABLE_MAXED } from '../../../modules/enums';
import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../../modules/errors';
import { CollatorService } from '../../collator';
import { EngineService } from '../../engine';
import { ILogger } from '../../logger';
import { TelemetryService } from '../../telemetry';
import {
  ActivityData,
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
} from '../../../types/activity';
import { TransactionResultList } from '../../../types/provider';
import { JobState } from '../../../types/job';
import {
  StreamCode,
  StreamData,
  StreamStatus,
} from '../../../types/stream';

interface ProcessContext {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
  engine: EngineService;
  logger: ILogger;
  status: StreamStatus;
  code: StreamCode;
  data: ActivityData;
  leg: ActivityLeg;
  adjacencyList: StreamData[];
  adjacentIndex: number;
  setLeg(leg: ActivityLeg): void;
  verifyReentry(): Promise<number>;
  bindActivityError(data: Record<string, unknown>): void;
  bindActivityData(type: 'output' | 'hook'): void;
  bindJobError(data: Record<string, unknown>): void;
  filterAdjacent(): Promise<StreamData[]>;
  mapJobData(): void;
  executeStepProtocol(
    delta: number,
    shouldFinalize: boolean,
  ): Promise<boolean>;
}

export async function processEvent(
  instance: ProcessContext,
  status: StreamStatus = StreamStatus.SUCCESS,
  code: StreamCode = 200,
  type: 'hook' | 'output' = 'output',
): Promise<void> {
  instance.setLeg(2);
  const jid = instance.context.metadata.jid;
  if (!jid) {
    instance.logger.error('activity-process-event-error', {
      message: 'job id is undefined',
    });
    return;
  }
  const aid = instance.metadata.aid;
  instance.status = status;
  instance.code = code;
  instance.logger.debug('activity-process-event', {
    topic: instance.config.subtype,
    jid,
    aid,
    status,
    code,
  });
  let telemetry: TelemetryService;

  try {
    const collationKey = await instance.verifyReentry();

    instance.adjacentIndex =
      CollatorService.getDimensionalIndex(collationKey);
    telemetry = new TelemetryService(
      instance.engine.appId,
      instance.config,
      instance.metadata,
      instance.context,
    );
    telemetry.startActivitySpan(instance.leg);

    //bind data per status type
    if (status === StreamStatus.ERROR) {
      instance.bindActivityError(instance.data);
      instance.adjacencyList = await instance.filterAdjacent();
      if (!instance.adjacencyList.length) {
        instance.bindJobError(instance.data);
      }
    } else {
      instance.bindActivityData(type);
      instance.adjacencyList = await instance.filterAdjacent();
    }
    instance.mapJobData();

    //mark unrecoverable errors as terminal
    if (status === StreamStatus.ERROR && !instance.adjacencyList?.length) {
      if (!instance.context.data) instance.context.data = {};
      instance.context.data.done = true;
      instance.context.data.$error = {
        message: instance.data?.message || 'unknown error',
        code: HMSH_CODE_DURABLE_MAXED,
        stack: instance.data?.stack,
      };
    }

    const delta =
      status === StreamStatus.PENDING
        ? instance.adjacencyList.length
        : instance.adjacencyList.length - 1;
    const shouldFinalize = status !== StreamStatus.PENDING;

    await instance.executeStepProtocol(delta, shouldFinalize);

    telemetry.mapActivityAttributes();
    telemetry.setActivityAttributes({});
  } catch (error) {
    if (error instanceof CollationError) {
      instance.logger.info(`process-event-${error.fault}-error`, { error });
      return;
    } else if (error instanceof InactiveJobError) {
      instance.logger.info('process-event-inactive-job-error', { error });
      return;
    } else if (error instanceof GenerationalError) {
      instance.logger.info('process-event-generational-job-error', {
        error,
      });
      return;
    } else if (error instanceof GetStateError) {
      instance.logger.info('process-event-get-job-error', { error });
      return;
    }
    instance.logger.error('activity-process-event-error', {
      error,
      message: error.message,
      stack: error.stack,
      name: error.name,
    });
    telemetry?.setActivityError(error.message);
    throw error;
  } finally {
    telemetry?.endActivitySpan();
    instance.logger.debug('activity-process-event-end', { jid, aid });
  }
}
