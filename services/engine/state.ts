/**
 * Job state retrieval, export, and symbol compression.
 *
 * Provides read access to job data at various granularities:
 *   - `getState()`      → full hydrated job tree
 *   - `getQueryState()` → specific fields only
 *   - `getRaw()`        → raw hash data (no hydration)
 *   - `getStatus()`     → job status code only
 *   - `export()`        → full export via ExporterService
 */

import { restoreHierarchy } from '../../modules/utils';
import { ExporterService } from '../exporter';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import { ILogger } from '../logger';
import { AppVID } from '../../types/app';
import { Consumes } from '../../types/activity';
import { ExportOptions, JobExport } from '../../types/exporter';
import { JobOutput, JobStatus } from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { StringAnyType, StringStringType } from '../../types/serializer';

interface StateContext {
  appId: string;
  store: StoreService<ProviderClient, ProviderTransaction>;
  exporter: ExporterService;
  logger: ILogger;
  getVID(vid?: AppVID): Promise<AppVID>;
}

export async function exportJob(
  instance: StateContext,
  jobId: string,
  options: ExportOptions = {},
): Promise<JobExport> {
  return await instance.exporter.export(jobId, options);
}

export async function getRaw(
  instance: StateContext,
  jobId: string,
): Promise<StringStringType> {
  return await instance.store.getRaw(jobId);
}

export async function getStatus(
  instance: StateContext,
  jobId: string,
): Promise<JobStatus> {
  const { id: appId } = await instance.getVID();
  return await instance.store.getStatus(jobId, appId);
}

export async function getState(
  instance: StateContext,
  topic: string,
  jobId: string,
): Promise<JobOutput> {
  const jobSymbols = await instance.store.getSymbols(`$${topic}`);
  const consumes: Consumes = {
    [`$${topic}`]: Object.keys(jobSymbols),
  };
  const dIds = {} as StringStringType;
  const output = await instance.store.getState(jobId, consumes, dIds);
  if (!output) {
    throw new Error(`not found ${jobId}`);
  }
  const [state, status] = output;
  const stateTree = restoreHierarchy(state) as JobOutput;
  if (status != null && stateTree.metadata) {
    stateTree.metadata.js = status;
  }
  return stateTree;
}

export async function getQueryState(
  instance: StateContext,
  jobId: string,
  fields: string[],
): Promise<StringAnyType> {
  return await instance.store.getQueryState(jobId, fields);
}

/**
 * @deprecated
 */
export async function compress(
  instance: StateContext,
  terms: string[],
): Promise<boolean> {
  const existingSymbols = await instance.store.getSymbolValues();
  const startIndex = Object.keys(existingSymbols).length;
  const maxIndex = Math.pow(52, 2) - 1;
  const newSymbols = SerializerService.filterSymVals(
    startIndex,
    maxIndex,
    existingSymbols,
    new Set(terms),
  );
  return await instance.store.addSymbolValues(newSymbols);
}
