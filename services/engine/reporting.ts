/**
 * Stats queries and job-ID lookups.
 *
 * Builds a `GetStatsOptions` query by instantiating a Trigger activity
 * to resolve the job key and granularity, then delegates to
 * ReporterService for the actual data retrieval.
 */

import { ReporterService } from '../reporter';
import { Trigger } from '../activities/trigger';
import { AppVID } from '../../types/app';
import { JobData } from '../../types/job';
import {
  GetStatsOptions,
  IdsResponse,
  JobStatsInput,
  StatsResponse,
} from '../../types/stats';

interface ReportingContext {
  getVID(vid?: AppVID): Promise<AppVID>;
  initActivity(
    topic: string,
    data?: JobData,
  ): Promise<any>;
  store: any;
  logger: any;
}

export async function getStats(
  instance: ReportingContext,
  topic: string,
  query: JobStatsInput,
): Promise<StatsResponse> {
  const vid = await instance.getVID();
  const reporter = new ReporterService(vid, instance.store, instance.logger);
  const resolvedQuery = await resolveQuery(instance, topic, query);
  return await reporter.getStats(resolvedQuery);
}

export async function getIds(
  instance: ReportingContext,
  topic: string,
  query: JobStatsInput,
  queryFacets: string[] = [],
): Promise<IdsResponse> {
  const vid = await instance.getVID();
  const reporter = new ReporterService(vid, instance.store, instance.logger);
  const resolvedQuery = await resolveQuery(instance, topic, query);
  return await reporter.getIds(resolvedQuery, queryFacets);
}

export async function resolveQuery(
  instance: ReportingContext,
  topic: string,
  query: JobStatsInput,
): Promise<GetStatsOptions> {
  const trigger = (await instance.initActivity(topic, query.data)) as Trigger;
  await trigger.getState();
  return {
    end: query.end,
    start: query.start,
    range: query.range,
    granularity: trigger.resolveGranularity(),
    key: trigger.resolveJobKey(trigger.createInputContext()),
    sparse: query.sparse,
  } as GetStatsOptions;
}
