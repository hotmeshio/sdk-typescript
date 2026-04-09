import { EngineService } from '../engine';
import {
  JobState,
  JobData,
  JobOutput,
  JobStatus,
  JobInterruptOptions,
} from '../../types/job';
import { ExportOptions, JobExport } from '../../types/exporter';
import { StringAnyType, StringStringType } from '../../types/serializer';
import {
  JobStatsInput,
  GetStatsOptions,
  IdsResponse,
  StatsResponse,
} from '../../types/stats';
import { StreamCode, StreamStatus } from '../../types/stream';

interface JobsContext {
  engine: EngineService | null;
}

/**
 * Exports the full job state as a structured JSON object.
 */
export async function exportJob(
  instance: JobsContext,
  jobId: string,
  options: ExportOptions = {},
): Promise<JobExport> {
  return await instance.engine?.export(jobId, options);
}

/**
 * Returns all raw key-value pairs for a job's HASH record.
 */
export async function getRaw(
  instance: JobsContext,
  jobId: string,
): Promise<StringStringType> {
  return await instance.engine?.getRaw(jobId);
}

/**
 * Reporter-related method to get the status of a job.
 * @private
 */
export async function getStats(
  instance: JobsContext,
  topic: string,
  query: JobStatsInput,
): Promise<StatsResponse> {
  return await instance.engine?.getStats(topic, query);
}

/**
 * Returns the numeric status semaphore for a job.
 */
export async function getStatus(
  instance: JobsContext,
  jobId: string,
): Promise<JobStatus> {
  return instance.engine?.getStatus(jobId);
}

/**
 * Returns the structured job state (data and metadata) for a job.
 */
export async function getState(
  instance: JobsContext,
  topic: string,
  jobId: string,
): Promise<JobOutput> {
  return instance.engine?.getState(topic, jobId);
}

/**
 * Returns specific searchable fields from a job's HASH record.
 */
export async function getQueryState(
  instance: JobsContext,
  jobId: string,
  fields: string[],
): Promise<StringAnyType> {
  return await instance.engine?.getQueryState(jobId, fields);
}

/**
 * @private
 */
export async function getIds(
  instance: JobsContext,
  topic: string,
  query: JobStatsInput,
  queryFacets: string[] = [],
): Promise<IdsResponse> {
  return await instance.engine?.getIds(topic, query, queryFacets);
}

/**
 * @private
 */
export async function resolveQuery(
  instance: JobsContext,
  topic: string,
  query: JobStatsInput,
): Promise<GetStatsOptions> {
  return await instance.engine?.resolveQuery(topic, query);
}

/**
 * Interrupts (terminates) an active workflow job.
 */
export async function interrupt(
  instance: JobsContext,
  topic: string,
  jobId: string,
  options: JobInterruptOptions = {},
): Promise<string> {
  return await instance.engine?.interrupt(topic, jobId, options);
}

/**
 * Immediately deletes a completed job from the system.
 */
export async function scrub(
  instance: JobsContext,
  jobId: string,
) {
  await instance.engine?.scrub(jobId);
}

/**
 * Sends a signal to a paused workflow, resuming its execution.
 */
export async function signal(
  instance: JobsContext,
  topic: string,
  data: JobData,
  status?: StreamStatus,
  code?: StreamCode,
): Promise<string> {
  return await instance.engine?.signal(topic, data, status, code);
}

/**
 * Fan-out variant of `signal()`.
 * @private
 */
export async function signalAll(
  instance: JobsContext,
  hookTopic: string,
  data: JobData,
  query: JobStatsInput,
  queryFacets: string[] = [],
): Promise<string[]> {
  return await instance.engine?.signalAll(hookTopic, data, query, queryFacets);
}
