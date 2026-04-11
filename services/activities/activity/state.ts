import {
  formatISODate,
  getValueByPath,
  restoreHierarchy,
} from '../../../modules/utils';
import { CollatorService } from '../../collator';
import { EngineService } from '../../engine';
import { ILogger } from '../../logger';
import { MDATA_SYMBOLS } from '../../serializer';
import { StoreService } from '../../store';
import { TelemetryService } from '../../telemetry';
import {
  ActivityData,
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
  Consumes,
} from '../../../types/activity';
import { JobState } from '../../../types/job';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../types/provider';
import { StringAnyType } from '../../../types/serializer';
import { StreamStatus } from '../../../types/stream';

interface StateContext {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
  store: StoreService<ProviderClient, ProviderTransaction>;
  engine: EngineService;
  logger: ILogger;
  status: StreamStatus;
  data: ActivityData;
  leg: ActivityLeg;
  getJobStatus(): number | null;
  authorizeEntry(state: StringAnyType): string[];
  resolveDad(): string;
  getTriggerConfig(): Promise<ActivityType>;
  bindJobMetadataPaths(): string[];
  bindActivityMetadataPaths(): string[];
  assertGenerationalId(jobGID: string, msgGID: string): void;
  initDimensionalAddress(dad: string): void;
  initSelf(context: StringAnyType): JobState;
  initPolicies(context: JobState): void;
}

//─── persist full activity + job state ───────────────────────────────

export async function setState(
  instance: StateContext,
  transaction?: ProviderTransaction,
): Promise<string> {
  const jobId = instance.context.metadata.jid;
  bindJobMetadata(instance);
  bindActivityMetadata(instance);
  const state: StringAnyType = {};
  await bindJobState(instance, state);
  const presets = instance.authorizeEntry(state);
  bindDimensionalAddress(instance, state);
  bindActivityState(instance, state);
  const symbolNames = [
    `$${instance.config.subscribes}`,
    instance.metadata.aid,
    ...presets,
  ];
  const dIds = CollatorService.getDimensionsById(
    [...instance.config.ancestors, instance.metadata.aid],
    instance.resolveDad(),
  );
  return await instance.store.setState(
    state,
    instance.getJobStatus(),
    jobId,
    symbolNames,
    dIds,
    transaction,
  );
}

//─── load state from store into context ──────────────────────────────

export async function getState(instance: StateContext): Promise<void> {
  const gid = instance.context.metadata.gid;
  const jobSymbolHashName = `$${instance.config.subscribes}`;
  const consumes: Consumes = {
    [jobSymbolHashName]: MDATA_SYMBOLS.JOB.KEYS.map(
      (key) => `metadata/${key}`,
    ),
  };
  for (let [activityId, paths] of Object.entries(instance.config.consumes)) {
    if (activityId === '$job') {
      for (const path of paths) {
        consumes[jobSymbolHashName].push(path);
      }
    } else {
      if (activityId === '$self') {
        activityId = instance.metadata.aid;
      }
      if (!consumes[activityId]) {
        consumes[activityId] = [];
      }
      for (const path of paths) {
        consumes[activityId].push(`${activityId}/${path}`);
      }
    }
  }
  TelemetryService.addTargetTelemetryPaths(
    consumes,
    instance.config,
    instance.metadata,
    instance.leg,
  );
  const { dad, jid } = instance.context.metadata;
  const dIds = CollatorService.getDimensionsById(
    [...instance.config.ancestors, instance.metadata.aid],
    dad || '',
  );
  const [state, _status] = await instance.store.getState(jid, consumes, dIds);
  instance.context = restoreHierarchy(state) as JobState;
  instance.assertGenerationalId(instance.context?.metadata?.gid, gid);
  instance.initDimensionalAddress(dad);
  instance.initSelf(instance.context);
  instance.initPolicies(instance.context);
}

//─── semaphore status update ─────────────────────────────────────────

export async function setStatus(
  instance: StateContext,
  amount: number,
  transaction?: ProviderTransaction,
): Promise<void | any> {
  const { id: appId } = await instance.engine.getVID();
  return await instance.store.setStatus(
    amount,
    instance.context.metadata.jid,
    appId,
    transaction,
  );
}

//─── metadata binding ────────────────────────────────────────────────

export function bindJobMetadata(instance: StateContext): void {
  instance.context.metadata.ju = formatISODate(new Date());
}

export function bindActivityMetadata(instance: StateContext): void {
  const self: StringAnyType = instance.context['$self'];
  if (!self.output.metadata) {
    self.output.metadata = {};
  }
  if (instance.status === StreamStatus.ERROR) {
    self.output.metadata.err = JSON.stringify(instance.data);
  }
  const ts = formatISODate(new Date());
  self.output.metadata.ac = ts;
  self.output.metadata.au = ts;
  self.output.metadata.atp = instance.config.type;
  if (instance.config.subtype) {
    self.output.metadata.stp = instance.config.subtype;
  }
  self.output.metadata.aid = instance.metadata.aid;
}

//─── state path resolution ───────────────────────────────────────────

export async function bindJobState(
  instance: StateContext,
  state: StringAnyType,
): Promise<void> {
  const triggerConfig = await instance.getTriggerConfig();
  const PRODUCES = [
    ...(triggerConfig.PRODUCES || []),
    ...instance.bindJobMetadataPaths(),
  ];
  for (const path of PRODUCES) {
    const value = getValueByPath(instance.context, path);
    if (value !== undefined) {
      state[path] = value;
    }
  }
  for (const key in instance.context?.data ?? {}) {
    if (key.startsWith('-') || key.startsWith('_')) {
      state[key] = instance.context.data[key];
    }
  }
  TelemetryService.bindJobTelemetryToState(
    state,
    instance.config,
    instance.context,
  );
}

export function bindActivityState(
  instance: StateContext,
  state: StringAnyType,
): void {
  const produces = [
    ...instance.config.produces,
    ...instance.bindActivityMetadataPaths(),
  ];
  for (const path of produces) {
    const prefixedPath = `${instance.metadata.aid}/${path}`;
    const value = getValueByPath(instance.context, prefixedPath);
    if (value !== undefined) {
      state[prefixedPath] = value;
    }
  }
  TelemetryService.bindActivityTelemetryToState(
    state,
    instance.config,
    instance.metadata,
    instance.context,
    instance.leg,
  );
}

export function bindDimensionalAddress(
  instance: StateContext,
  state: StringAnyType,
): void {
  const dad = instance.resolveDad();
  state[`${instance.metadata.aid}/output/metadata/dad`] = dad;
}

//─── default metadata path resolvers (overridden by Trigger) ─────────

export function bindJobMetadataPaths(): string[] {
  return MDATA_SYMBOLS.JOB_UPDATE.KEYS.map((key) => `metadata/${key}`);
}

export function bindActivityMetadataPaths(leg: ActivityLeg): string[] {
  const keys_to_save = leg === 1 ? 'ACTIVITY' : 'ACTIVITY_UPDATE';
  return MDATA_SYMBOLS[keys_to_save].KEYS.map(
    (key) => `output/metadata/${key}`,
  );
}
