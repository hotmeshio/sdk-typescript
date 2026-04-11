import {
  CollationError,
  InactiveJobError,
} from '../../../modules/errors';
import { CollatorService } from '../../collator';
import { EngineService } from '../../engine';
import { StoreService } from '../../store';
import {
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
} from '../../../types/activity';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../types/provider';
import { JobState } from '../../../types/job';

interface VerifyContext {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
  store: StoreService<ProviderClient, ProviderTransaction>;
  engine: EngineService;
  adjacentIndex: number;
  guidLedger: number;
  setLeg(leg: ActivityLeg): void;
  getState(): Promise<void>;
  setStatus(amount: number, txn?: ProviderTransaction): Promise<void | any>;
  mapStatusThreshold(): number;
}

export async function verifyEntry(instance: VerifyContext): Promise<void> {
  instance.setLeg(1);
  await instance.getState();
  const threshold = instance.mapStatusThreshold();
  try {
    CollatorService.assertJobActive(
      instance.context.metadata.js,
      instance.context.metadata.jid,
      instance.metadata.aid,
      threshold,
    );
  } catch (error) {
    await CollatorService.notarizeEntry(instance as any);
    if (threshold > 0) {
      if (instance.context.metadata.js <= threshold) {
        const status = await instance.setStatus(-threshold);
        if (Number(status) === 0) {
          const txn = instance.store.transact();
          await instance.engine.runJobCompletionTasks(
            instance.context,
            {},
            txn,
          );
          await txn.exec();
        }
      }
    } else {
      throw error;
    }
    return;
  }
  await CollatorService.notarizeEntry(instance as any);
}

export async function verifyReentry(
  instance: VerifyContext,
): Promise<number> {
  const msgGuid = instance.context.metadata.guid;
  instance.setLeg(2);
  await instance.getState();
  instance.context.metadata.guid = msgGuid;
  CollatorService.assertJobActive(
    instance.context.metadata.js,
    instance.context.metadata.jid,
    instance.metadata.aid,
  );
  const [activityLedger, guidLedger] =
    await CollatorService.notarizeLeg2Entry(instance as any, msgGuid);
  instance.guidLedger = guidLedger;
  return activityLedger;
}

export async function verifyLeg1Entry(
  instance: VerifyContext,
): Promise<boolean> {
  const msgGuid = instance.context.metadata.guid;
  instance.setLeg(1);
  await instance.getState();
  instance.context.metadata.guid = msgGuid;
  const threshold = instance.mapStatusThreshold();
  try {
    CollatorService.assertJobActive(
      instance.context.metadata.js,
      instance.context.metadata.jid,
      instance.metadata.aid,
      threshold,
    );
  } catch (error) {
    if (error instanceof InactiveJobError && threshold > 0) {
      await CollatorService.notarizeEntry(instance as any);
      if (instance.context.metadata.js === threshold) {
        const status = await instance.setStatus(-threshold);
        if (Number(status) === 0) {
          await instance.engine.runJobCompletionTasks(instance.context);
        }
      }
    }
    throw error;
  }
  try {
    await CollatorService.notarizeEntry(instance as any);
    return false;
  } catch (error) {
    if (error instanceof CollationError && error.fault === 'duplicate') {
      if (instance.config.cycle) {
        const [activityLedger, guidLedger] =
          await CollatorService.notarizeLeg2Entry(instance as any, msgGuid);
        instance.adjacentIndex =
          CollatorService.getDimensionalIndex(activityLedger);
        instance.guidLedger = guidLedger;
        return false;
      }
      const guidValue = await instance.store.collateSynthetic(
        instance.context.metadata.jid,
        msgGuid,
        0,
      );
      instance.guidLedger = guidValue;
      return true;
    }
    throw error;
  }
}
