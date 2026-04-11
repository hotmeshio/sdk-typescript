import { guid } from '../../../modules/utils';
import { CollatorService } from '../../collator';
import { EngineService } from '../../engine';
import { MapperService } from '../../mapper';
import { Pipe } from '../../pipe';
import { StoreService } from '../../store';
import { ActivityMetadata, ActivityType } from '../../../types/activity';
import { JobState, JobStatus } from '../../../types/job';
import {
  ProviderClient,
  ProviderTransaction,
  TransactionResultList,
} from '../../../types/provider';
import { StreamCode, StreamData, StreamDataType } from '../../../types/stream';
import { TransitionRule } from '../../../types/transition';

interface TransitionContext {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
  store: StoreService<ProviderClient, ProviderTransaction>;
  engine: EngineService;
  code: StreamCode;
  resolveAdjacentDad(): string;
}

export async function filterAdjacent(
  instance: TransitionContext,
): Promise<StreamData[]> {
  const adjacencyList: StreamData[] = [];
  const transitions = await instance.store.getTransitions(
    await instance.engine.getVID(),
  );
  const transition = transitions[`.${instance.metadata.aid}`];
  const adjacentDad = instance.resolveAdjacentDad();
  if (transition) {
    for (const toActivityId in transition) {
      const transitionRule: boolean | TransitionRule =
        transition[toActivityId];
      if (
        MapperService.evaluate(transitionRule, instance.context, instance.code)
      ) {
        adjacencyList.push({
          metadata: {
            guid: guid(),
            jid: instance.context.metadata.jid,
            gid: instance.context.metadata.gid,
            dad: adjacentDad,
            aid: toActivityId,
            spn: instance.context['$self'].output.metadata?.l2s,
            trc: instance.context.metadata.trc,
          },
          type: StreamDataType.TRANSITION,
          data: {},
        });
      }
    }
  }
  return adjacencyList;
}

export function resolveThresholdHit(
  results: TransactionResultList,
): boolean {
  const last = results[results.length - 1];
  const value = Array.isArray(last) ? last[1] : last;
  return Number(value) === 1;
}

export function resolveStatus(
  multiResponse: TransactionResultList,
): number {
  const activityStatus = multiResponse[multiResponse.length - 1];
  if (Array.isArray(activityStatus)) {
    return Number(activityStatus[1]);
  }
  return Number(activityStatus);
}

export function shouldEmit(instance: TransitionContext): boolean {
  if (instance.config.emit) {
    return Pipe.resolve(instance.config.emit, instance.context) === true;
  }
  return false;
}

export function shouldPersistJob(instance: TransitionContext): boolean {
  if (instance.config.persist !== undefined) {
    return Pipe.resolve(instance.config.persist, instance.context) === true;
  }
  return false;
}

export function isJobComplete(jobStatus: JobStatus): boolean {
  return jobStatus <= 0;
}

export function jobWasInterrupted(jobStatus: JobStatus): boolean {
  return jobStatus < -100_000_000;
}
