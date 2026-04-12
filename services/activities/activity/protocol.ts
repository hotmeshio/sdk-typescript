import { CollatorService } from '../../collator';
import { EngineService } from '../../engine';
import { ILogger } from '../../logger';
import { StoreService } from '../../store';
import { ActivityMetadata, ActivityType } from '../../../types/activity';
import {
  ProviderClient,
  ProviderTransaction,
  TransactionResultList,
} from '../../../types/provider';
import { JobState } from '../../../types/job';
import { StreamData } from '../../../types/stream';

interface ProtocolContext {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
  store: StoreService<ProviderClient, ProviderTransaction>;
  engine: EngineService;
  logger: ILogger;
  adjacencyList: StreamData[];
  adjacentIndex: number;
  guidLedger: number;
  mapStatusThreshold(): number;
  setState(txn?: ProviderTransaction): Promise<string>;
  shouldEmit(): boolean;
  shouldPersistJob(): boolean;
  resolveThresholdHit(results: TransactionResultList): boolean;
}

/**
 * 3-step Leg2 protocol using GUID ledger for crash-safe resume.
 * Each step bundles durable writes with its concluding digit
 * update in a single transaction.
 */
export async function executeStepProtocol(
  instance: ProtocolContext,
  delta: number,
  shouldFinalize: boolean,
): Promise<boolean> {
  const msgGuid = instance.context.metadata.guid;
  const threshold = instance.mapStatusThreshold();
  const { id: appId } = await instance.engine.getVID();

  //Step 1: Save work (skip if GUID 10B already set)
  if (!CollatorService.isGuidStep1Done(instance.guidLedger)) {
    const txn1 = instance.store.transact();
    await instance.setState(txn1);
    await CollatorService.notarizeStep1(instance as any, msgGuid, txn1);
    await txn1.exec();
  }

  //Step 2: Spawn children + semaphore + edge capture
  let thresholdHit = false;
  if (!CollatorService.isGuidStep2Done(instance.guidLedger)) {
    const txn2 = instance.store.transact();
    await CollatorService.notarizeStep2(instance as any, msgGuid, txn2);
    for (const child of instance.adjacencyList) {
      await instance.engine.router?.publishMessage(null, child, txn2);
    }
    await instance.store.setStatusAndCollateGuid(
      delta,
      threshold,
      instance.context.metadata.jid,
      appId,
      msgGuid,
      CollatorService.WEIGHTS.GUID_SNAPSHOT,
      txn2,
    );
    const results = (await txn2.exec()) as TransactionResultList;
    thresholdHit = instance.resolveThresholdHit(results);
    instance.logger.debug('step-protocol-step2-complete', {
      jid: instance.context.metadata.jid,
      aid: instance.metadata.aid,
      delta,
      threshold,
      thresholdHit,
      lastResult: results[results.length - 1],
      resultCount: results.length,
    });
  } else {
    thresholdHit = CollatorService.isGuidJobClosed(instance.guidLedger);
  }

  //Step 3: Job completion tasks
  const isErrorTerminal =
    !thresholdHit &&
    instance.context.data?.done === true &&
    !!instance.context.data?.$error;
  const needsCompletion =
    thresholdHit ||
    instance.shouldEmit() ||
    instance.shouldPersistJob() ||
    isErrorTerminal;
  if (
    needsCompletion &&
    !CollatorService.isGuidStep3Done(instance.guidLedger)
  ) {
    const txn3 = instance.store.transact();
    const options =
      thresholdHit || isErrorTerminal
        ? {}
        : { emit: !instance.shouldPersistJob() };
    await instance.engine.runJobCompletionTasks(
      instance.context,
      options,
      txn3,
    );
    await CollatorService.notarizeStep3(instance as any, msgGuid, txn3);
    const shouldFinalizeNow =
      thresholdHit || isErrorTerminal
        ? shouldFinalize
        : instance.shouldPersistJob();
    if (shouldFinalizeNow) {
      await CollatorService.notarizeFinalize(instance as any, txn3);
    }
    await txn3.exec();
  } else if (needsCompletion) {
    instance.logger.debug('step-protocol-step3-skipped-already-done', {
      jid: instance.context.metadata.jid,
      aid: instance.metadata.aid,
    });
  } else {
    instance.logger.debug('step-protocol-no-threshold', {
      jid: instance.context.metadata.jid,
      aid: instance.metadata.aid,
      thresholdHit,
    });
  }

  return thresholdHit;
}

/**
 * 3-step Leg1 protocol for Category B activities
 * (Leg1-only with children: Hook passthrough, Signal, Interrupt-another).
 *
 * Step A: setState + Leg1 completion + step1 marker
 * Step B: publish children + semaphore + edge capture
 * Step C: if edge → completion tasks + finalize
 */
export async function executeLeg1StepProtocol(
  instance: ProtocolContext,
  delta: number,
): Promise<boolean> {
  const msgGuid = instance.context.metadata.guid;
  const threshold = instance.mapStatusThreshold();
  const { id: appId } = await instance.engine.getVID();

  //Step A: Save work + Leg1 completion marker
  if (!CollatorService.isGuidStep1Done(instance.guidLedger)) {
    const txn1 = instance.store.transact();
    await instance.setState(txn1);
    if (instance.adjacentIndex === 0) {
      await CollatorService.notarizeLeg1Completion(instance as any, txn1);
    }
    await CollatorService.notarizeStep1(instance as any, msgGuid, txn1);
    await txn1.exec();
  }

  //Step B: Spawn children + semaphore + edge capture
  let thresholdHit = false;
  if (!CollatorService.isGuidStep2Done(instance.guidLedger)) {
    const txn2 = instance.store.transact();
    await CollatorService.notarizeStep2(instance as any, msgGuid, txn2);
    for (const child of instance.adjacencyList) {
      await instance.engine.router?.publishMessage(null, child, txn2);
    }
    await instance.store.setStatusAndCollateGuid(
      delta,
      threshold,
      instance.context.metadata.jid,
      appId,
      msgGuid,
      CollatorService.WEIGHTS.GUID_SNAPSHOT,
      txn2,
    );
    const results = (await txn2.exec()) as TransactionResultList;
    thresholdHit = instance.resolveThresholdHit(results);
    instance.logger.debug('leg1-step-protocol-stepB-complete', {
      jid: instance.context.metadata.jid,
      aid: instance.metadata.aid,
      delta,
      threshold,
      thresholdHit,
      lastResult: results[results.length - 1],
    });
  } else {
    thresholdHit = CollatorService.isGuidJobClosed(instance.guidLedger);
  }

  //Step C: Job completion tasks
  const isErrorTerminal =
    !thresholdHit &&
    instance.context.data?.done === true &&
    !!instance.context.data?.$error;
  const needsCompletion =
    thresholdHit ||
    instance.shouldEmit() ||
    instance.shouldPersistJob() ||
    isErrorTerminal;
  if (
    needsCompletion &&
    !CollatorService.isGuidStep3Done(instance.guidLedger)
  ) {
    const txn3 = instance.store.transact();
    const options =
      thresholdHit || isErrorTerminal
        ? {}
        : { emit: !instance.shouldPersistJob() };
    await instance.engine.runJobCompletionTasks(
      instance.context,
      options,
      txn3,
    );
    await CollatorService.notarizeStep3(instance as any, msgGuid, txn3);
    await CollatorService.notarizeFinalize(instance as any, txn3);
    await txn3.exec();
  }

  return thresholdHit;
}
