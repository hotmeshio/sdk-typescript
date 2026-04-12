import { HMSH_EXPIRE_DURATION } from '../../../modules/enums';
import { GenerationalError } from '../../../modules/errors';
import { formatISODate } from '../../../modules/utils';
import { CollatorService } from '../../collator';
import { Pipe } from '../../pipe';
import { ActivityData, ActivityMetadata, ActivityType } from '../../../types/activity';
import { JobState } from '../../../types/job';
import { StringAnyType } from '../../../types/serializer';
import { StreamStatus } from '../../../types/stream';

interface ContextInstance {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
  data: ActivityData;
  status: StreamStatus;
  adjacentIndex: number;
}

export function initSelf(
  instance: ContextInstance,
  context: StringAnyType,
): JobState {
  const activityId = instance.metadata.aid;
  if (!context[activityId]) {
    context[activityId] = {};
  }
  const self = context[activityId];
  if (!self.output) self.output = {};
  if (!self.input) self.input = {};
  if (!self.hook) self.hook = {};
  if (!self.output.metadata) self.output.metadata = {};
  self.output.metadata.au = formatISODate(new Date());
  context['$self'] = self;
  context['$job'] = context; //NEVER call STRINGIFY! (now circular)
  return context as JobState;
}

export function initPolicies(
  instance: ContextInstance,
  context: JobState,
): void {
  const expire = Pipe.resolve(
    instance.config.expire ?? HMSH_EXPIRE_DURATION,
    context,
  );
  context.metadata.expire = expire;
  if (instance.config.persistent != undefined) {
    const persistent = Pipe.resolve(
      instance.config.persistent ?? false,
      context,
    );
    context.metadata.persistent = persistent;
  }
}

export function initDimensionalAddress(
  instance: ContextInstance,
  dad: string,
): void {
  instance.metadata.dad = dad;
}

export function assertGenerationalId(
  jobGID: string,
  msgGID: string,
  context: JobState,
): void {
  if (msgGID !== jobGID) {
    throw new GenerationalError(
      jobGID,
      msgGID,
      context?.metadata?.jid ?? '',
      context?.metadata?.aid ?? '',
      context?.metadata?.dad ?? '',
    );
  }
}

export function resolveDad(instance: ContextInstance): string {
  let dad = instance.metadata.dad;
  if (instance.adjacentIndex > 0) {
    dad = `${dad.substring(0, dad.lastIndexOf(','))},${instance.adjacentIndex}`;
  }
  return dad;
}

export function resolveAdjacentDad(instance: ContextInstance): string {
  return `${resolveDad(instance)}${CollatorService.getDimensionalSeed(0)}`;
}

export function bindActivityData(
  instance: ContextInstance,
  type: 'output' | 'hook',
): void {
  instance.context[instance.metadata.aid][type].data = instance.data;
}

export function bindActivityError(
  instance: ContextInstance,
  data: Record<string, unknown>,
): void {
  const md = instance.context[instance.metadata.aid].output.metadata;
  md.err = JSON.stringify(instance.data);
  md.$error = { ...data, is_stream_error: true };
}

export function bindJobError(
  instance: ContextInstance,
  data: Record<string, unknown>,
): void {
  instance.context.metadata.err = JSON.stringify({
    ...data,
    is_stream_error: true,
  });
}
