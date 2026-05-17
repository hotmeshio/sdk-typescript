import {
  HMSH_CODE_DURABLE_MAXED,
  HMSH_RESERVATION_TIMEOUT_S,
} from '../../../modules/enums';
import {
  CollationError,
  GenerationalError,
  GetStateError,
  InactiveJobError,
} from '../../../modules/errors';
import { CollationFaultType } from '../../../types/collator';
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

// Per-instance collation error tracking for reservation timeout detection
let collationErrorCount = 0;
let collationWindowStart = Date.now();
const COLLATION_WARN_THRESHOLD = 10;
const COLLATION_WINDOW_MS = 60_000;

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
      //FORBIDDEN: Leg1 not complete — signal arrived in the window
      //between registerHook (standalone) and Leg1 transaction commit.
      //Rethrow so the stream message is retried with backoff; by then
      //Leg1 will have committed and Leg2 processing will succeed.
      //The GUID marker was already committed by notarizeLeg2Entry;
      //on retry, collateLeg2Entry's SETNX is a no-op for the same
      //GUID, and verifySyntheticInteger sees no steps done → allowed.
      if (error.fault === CollationFaultType.FORBIDDEN) {
        instance.logger.warn('process-event-forbidden-retry', {
          jid: instance.context.metadata.jid,
          aid: instance.metadata.aid,
          message: 'Leg1 not committed yet; rethrowing for stream retry',
          error,
        });
        throw error;
      }
      // INACTIVE/DUPLICATE: legitimate duplicate detection — the
      // Postgres atomic CTE (collateLeg2Entry) serializes via row
      // locks, so the GUID ledger value is correct. Silent ack is
      // the right behavior: the work was already done by a prior
      // delivery of this message.
      const now = Date.now();
      if (now - collationWindowStart > COLLATION_WINDOW_MS) {
        collationErrorCount = 0;
        collationWindowStart = now;
      }
      collationErrorCount++;
      if (collationErrorCount === COLLATION_WARN_THRESHOLD) {
        instance.logger.warn('process-event-collation-rate-exceeded', {
          count: collationErrorCount,
          windowMs: COLLATION_WINDOW_MS,
          reservationTimeoutS: HMSH_RESERVATION_TIMEOUT_S,
          message: `${COLLATION_WARN_THRESHOLD} collation errors in ${COLLATION_WINDOW_MS / 1000}s. ` +
            `This typically means HMSH_RESERVATION_TIMEOUT_S (currently ${HMSH_RESERVATION_TIMEOUT_S}s) ` +
            `is too short for your workload — messages are being re-reserved before processing completes, ` +
            `causing duplicate delivery. Increase HMSH_RESERVATION_TIMEOUT_S.`,
        });
      }
      instance.logger.warn(`process-event-${error.fault}-error`, {
        jid: instance.context.metadata.jid,
        aid: instance.metadata.aid,
        error,
      });
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
