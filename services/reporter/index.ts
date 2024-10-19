import { ILogger } from '../logger';
import { Pipe } from '../pipe';
import { StoreService } from '../store';
import { TriggerActivity } from '../../types/activity';
import { AppVID } from '../../types/app';
import { JobState } from '../../types/job';
import { RedisClient, RedisMulti } from '../../types/redis';
import {
  GetStatsOptions,
  StatsResponse,
  AggregatedData,
  Measure,
  Segment,
  JobStatsRange,
  IdsData,
  IdsResponse,
  MeasureIds,
  TimeSegment,
  CountByFacet,
  StatsType,
  StatType,
} from '../../types/stats';

class ReporterService {
  private appVersion: AppVID;
  private logger: ILogger;
  private store: StoreService<RedisClient, RedisMulti>;
  static DEFAULT_GRANULARITY = '5m';

  constructor(
    appVersion: AppVID,
    store: StoreService<RedisClient, RedisMulti>,
    logger: ILogger,
  ) {
    this.appVersion = appVersion;
    this.logger = logger;
    this.store = store;
  }

  async getStats(options: GetStatsOptions): Promise<StatsResponse> {
    this.logger.debug('reporter-getstats-started', options);
    const { key, granularity, range, end, start } = options;
    this.validateOptions(options);
    const dateTimeSets = this.generateDateTimeSets(
      granularity,
      range,
      end,
      start,
    );
    const redisKeys = dateTimeSets.map((dateTime) =>
      this.buildRedisKey(key, dateTime),
    );
    const rawData = await this.store.getJobStats(redisKeys);
    const [count, aggregatedData] = this.aggregateData(rawData);
    const statsResponse = this.buildStatsResponse(
      rawData,
      redisKeys,
      aggregatedData,
      count,
      options,
    );
    return statsResponse;
  }

  private validateOptions(options: GetStatsOptions): void {
    const { start, end, range, granularity } = options;
    if (
      granularity !== 'infinity' &&
      (start && end && range || !start && !end && !range)
    ) {
      throw new Error(
        'Invalid combination of start, end, and range values. Provide either start+end, end+range, or start+range.',
      );
    }
  }

  private generateDateTimeSets(
    granularity: string,
    range: string | undefined,
    end: string,
    start?: string,
  ): string[] {
    if (granularity === 'infinity') {
      //if granularity is infinity, it means a date/time sequence/slice is not used to further segment the statistics
      return ['0'];
    }
    if (!range) {
      range = '0m';
    }
    const granularitiesInMinutes = {
      '5m': 5,
      '10m': 10,
      '15m': 15,
      '30m': 30,
      '1h': 60,
    };
    const granularityMinutes = granularitiesInMinutes[granularity];
    if (!granularityMinutes) {
      throw new Error('Invalid granularity value.');
    }
    const rangeMinutes = this.convertRangeToMinutes(range);
    if (rangeMinutes === null) {
      throw new Error('Invalid range value.');
    }
    // If start is provided, use it. Otherwise, calculate it from the end time and range.
    let startTime;
    let endTime;
    if (start) {
      startTime = new Date(start);
      endTime = new Date(startTime.getTime() + rangeMinutes * 60 * 1000);
    } else {
      endTime = end === 'NOW' ? new Date() : new Date(end);
      startTime = new Date(endTime.getTime() - rangeMinutes * 60 * 1000);
    }
    // Round the start time to the nearest granularity unit
    startTime.setUTCMinutes(
      Math.floor(startTime.getUTCMinutes() / granularityMinutes) *
        granularityMinutes,
    );
    const dateTimeSets: string[] = [];
    for (
      let time = startTime;
      time <= endTime;
      time.setUTCMinutes(time.getUTCMinutes() + granularityMinutes)
    ) {
      const formattedTime = [
        time.getUTCFullYear(),
        String(time.getUTCMonth() + 1).padStart(2, '0'),
        String(time.getUTCDate()).padStart(2, '0'),
        String(time.getUTCHours()).padStart(2, '0'),
        String(time.getUTCMinutes()).padStart(2, '0'),
      ].join('');
      dateTimeSets.push(formattedTime);
    }
    return dateTimeSets;
  }

  private convertRangeToMinutes(range: string): number | null {
    const timeUnit = range.slice(-1);
    const value = parseInt(range.slice(0, -1), 10);
    if (isNaN(value)) {
      return null;
    }
    switch (timeUnit) {
      case 'm':
        return value;
      case 'h':
        return value * 60;
      case 'd':
        return value * 60 * 24;
      default:
        return null;
    }
  }

  private buildRedisKey(key: string, dateTime: string, subTarget = ''): string {
    return `hmsh:${this.appVersion.id}:s:${key}:${dateTime}${subTarget ? ':' + subTarget : ''}`;
  }

  private aggregateData(rawData: JobStatsRange): [number, AggregatedData] {
    const aggregatedData: AggregatedData = {};
    let count = 0;
    Object.entries(rawData).forEach(([_, data]) => {
      for (const key in data) {
        if (key.startsWith('count:')) {
          const target = key.slice('count:'.length);
          if (!aggregatedData[target]) {
            aggregatedData[target] = 0;
          }
          aggregatedData[target] += data[key];
        } else if (key === 'count') {
          count += data[key];
        }
      }
    });
    return [count, aggregatedData];
  }

  private buildStatsResponse(
    rawData: JobStatsRange,
    redisKeys: string[],
    aggregatedData: AggregatedData,
    count: number,
    options: GetStatsOptions,
  ): StatsResponse {
    const measures: Measure[] = [];
    const measureKeys = Object.keys(aggregatedData).filter(
      (key) => key !== 'count',
    );
    let segments = undefined;
    if (options.sparse !== true) {
      segments = this.handleSegments(rawData, redisKeys);
    }
    measureKeys.forEach((key) => {
      const measure: Measure = {
        target: key,
        type: 'count',
        value: aggregatedData[key],
      };
      measures.push(measure);
    });
    const response: StatsResponse = {
      key: options.key,
      granularity: options.granularity,
      range: options.range,
      end: options.end,
      count,
      measures: measures,
    };
    if (segments) {
      response.segments = segments;
    }
    return response;
  }

  private handleSegments(data: JobStatsRange, hashKeys: string[]): Segment[] {
    const segments: Segment[] = [];
    hashKeys.forEach((hashKey, index) => {
      const segmentData: Measure[] = [];
      data[hashKey] &&
        Object.entries(data[hashKey]).forEach(([key, value]) => {
          if (key.startsWith('count:')) {
            const target = key.slice('count:'.length);
            segmentData.push({ target, type: 'count', value });
          }
        });
      const isoTimestamp = this.isoTimestampFromKeyTimestamp(hashKey);
      const count = data[hashKey] ? data[hashKey].count : 0;
      segments.push({ count, time: isoTimestamp, measures: segmentData });
    });
    return segments;
  }

  private isoTimestampFromKeyTimestamp(hashKey: string): string {
    if (hashKey.endsWith(':')) {
      return '0';
    }
    const keyTimestamp = hashKey.slice(-12);
    const year = keyTimestamp.slice(0, 4);
    const month = keyTimestamp.slice(4, 6);
    const day = keyTimestamp.slice(6, 8);
    const hour = keyTimestamp.slice(8, 10);
    const minute = keyTimestamp.slice(10, 12);
    return `${year}-${month}-${day}T${hour}:${minute}Z`;
  }

  async getIds(
    options: GetStatsOptions,
    facets: string[],
    idRange: [number, number] = [0, -1],
  ): Promise<IdsResponse> {
    if (!facets.length) {
      const stats = await this.getStats(options);
      facets = this.getUniqueFacets(stats);
    }
    const { key, granularity, range, end, start } = options;
    this.validateOptions(options);
    let redisKeys: string[] = [];
    facets.forEach((facet) => {
      const dateTimeSets = this.generateDateTimeSets(
        granularity,
        range,
        end,
        start,
      );
      redisKeys = redisKeys.concat(
        dateTimeSets.map((dateTime) =>
          this.buildRedisKey(key, dateTime, `index:${facet}`),
        ),
      );
    });
    const idsData = await this.store.getJobIds(redisKeys, idRange);
    const idsResponse = this.buildIdsResponse(idsData, options, facets);
    return idsResponse;
  }

  private buildIdsResponse(
    idsData: IdsData,
    options: GetStatsOptions,
    facets: string[],
  ): IdsResponse {
    const countsByFacet: { [key: string]: number } = {};
    const measureKeys = Object.keys(idsData);
    measureKeys.forEach((key) => {
      const target = this.getTargetForKey(key as string);
      const count = idsData[key].length;

      if (countsByFacet[target]) {
        countsByFacet[target] += count;
      } else {
        countsByFacet[target] = count;
      }
    });
    const counts: CountByFacet[] = Object.entries(countsByFacet).map(
      ([facet, count]) => ({ facet, count }),
    );
    const response: IdsResponse = {
      key: options.key,
      facets,
      granularity: options.granularity,
      range: options.range,
      start: options.start,
      counts,
      segments: this.buildTimeSegments(idsData),
    };
    return response;
  }

  private buildTimeSegments(idsData: IdsData): TimeSegment[] {
    const measureKeys = Object.keys(idsData);
    const timeSegments: { [time: string]: MeasureIds[] } = {};

    measureKeys.forEach((key) => {
      const measure: MeasureIds = {
        type: 'ids',
        target: this.getTargetForKey(key as string),
        time: this.isoTimestampFromKeyTimestamp(
          this.getTargetForTime(key as string),
        ),
        count: idsData[key].length,
        ids: idsData[key],
      };

      if (timeSegments[measure.time]) {
        timeSegments[measure.time].push(measure);
      } else {
        timeSegments[measure.time] = [measure];
      }
    });

    const segments: TimeSegment[] = Object.entries(timeSegments).map(
      ([time, measures]) => ({
        time,
        measures,
      }),
    );

    return segments;
  }

  getUniqueFacets(data: StatsResponse): string[] {
    const targets = data.measures.map((measure) => measure.target);
    return Array.from(new Set(targets));
  }

  getTargetForKey(key: string): string {
    return key.split(':index:')[1];
  }

  getTargetForTime(key: string): string {
    return key.split(':index:')[0];
  }

  async getWorkItems(
    options: GetStatsOptions,
    facets: string[],
  ): Promise<string[]> {
    if (!facets.length) {
      const stats = await this.getStats(options);
      facets = this.getUniqueFacets(stats);
    }
    const { key, granularity, range, end, start } = options;
    this.validateOptions(options);
    let redisKeys: string[] = [];
    facets.forEach((facet) => {
      const dateTimeSets = this.generateDateTimeSets(
        granularity,
        range,
        end,
        start,
      );
      redisKeys = redisKeys.concat(
        dateTimeSets.map((dateTime) =>
          this.buildRedisKey(key, dateTime, `index:${facet}`),
        ),
      );
    });
    const idsData = await this.store.getJobIds(redisKeys, [0, 1]);
    const workerLists = this.buildWorkerLists(idsData);
    return workerLists;
  }

  private buildWorkerLists(idsData: IdsData): string[] {
    const workerLists: string[] = [];
    for (const key in idsData) {
      if (idsData[key].length) {
        workerLists.push(key);
      }
    }
    return workerLists;
  }

  /**
   * called by `trigger` activity to generate the stats that should
   * be saved to the database. doesn't actually save the stats, but
   * just generates the info that should be saved
   */
  resolveTriggerStatistics(
    { stats: statsConfig }: TriggerActivity,
    context: JobState,
  ): StatsType {
    const stats: StatsType = {
      general: [],
      index: [],
      median: [],
    };
    stats.general.push({ metric: 'count', target: 'count', value: 1 });
    for (const measure of statsConfig.measures) {
      const metric = this.resolveMetric(
        { metric: measure.measure, target: measure.target },
        context,
      );
      if (this.isGeneralMetric(measure.measure)) {
        stats.general.push(metric);
      } else if (this.isMedianMetric(measure.measure)) {
        stats.median.push(metric);
      } else if (this.isIndexMetric(measure.measure)) {
        stats.index.push(metric);
      }
    }
    return stats;
  }

  isGeneralMetric(metric: string): boolean {
    return metric === 'sum' || metric === 'avg' || metric === 'count';
  }

  isMedianMetric(metric: string): boolean {
    return metric === 'mdn';
  }

  isIndexMetric(metric: string): boolean {
    return metric === 'index';
  }

  resolveMetric({ metric, target }, context: JobState): StatType {
    const pipe = new Pipe([[target]], context);
    const resolvedValue = pipe.process().toString();
    const resolvedTarget = this.resolveTarget(metric, target, resolvedValue);
    if (metric === 'index') {
      return { metric, target: resolvedTarget, value: context.metadata.jid };
    } else if (metric === 'count') {
      return { metric, target: resolvedTarget, value: 1 };
    }
    return { metric, target: resolvedTarget, value: resolvedValue } as StatType;
  }

  isCardinalMetric(metric: string): boolean {
    return metric === 'index' || metric === 'count';
  }

  resolveTarget(metric: string, target: string, resolvedValue: string): string {
    const trimmed = target.substring(1, target.length - 1);
    const trimmedTarget = trimmed.split('.').slice(3).join('/');
    let resolvedTarget: string;
    if (this.isCardinalMetric(metric)) {
      resolvedTarget = `${metric}:${trimmedTarget}:${resolvedValue}`;
    } else {
      resolvedTarget = `${metric}:${trimmedTarget}`;
    }
    return resolvedTarget;
  }
}

export { ReporterService };
