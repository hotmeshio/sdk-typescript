type MetricTypes = 'count' | 'sum' | 'avg' | 'mdn' | 'max' | 'min' | 'index';

interface StatType {
  target: string; //e.g, (a target on the input data: `<activity>.input.data`) => {`object/type:widgetA|widgetB:sum`: <sum>}, {`object/type:widgetA|widgetB:count`: <count>}
  metric: MetricTypes; //count, avg, etc
  value: number | string; //a value to increment (sum); value to save to sorted set (mdn) or an id to add to an `index` or just '1' for a count
}

interface Measure {
  target: string;
  type: string;
  value: number;
}

interface Segment {
  time: string;
  count: number;
  measures: Measure[];
}

interface StatsType {
  general: StatType[];
  index: StatType[];
  median: StatType[];
}

interface JobStats {
  count?: number;
  [field: string]: number;
}

interface JobStatsRange {
  [key: string]: JobStats;
}

interface JobStatsInput {
  data: Record<string, unknown>;
  range?: string;
  start?: string;
  end?: string;
  sparse?: boolean;
  scrub?: boolean; //remove all indexes upon completion
}

interface GetStatsOptions {
  key: string;
  granularity: string;
  range?: string;
  start?: string;
  end?: string;
  sparse?: boolean;
}

interface StatsResponse {
  key: string;
  granularity: string;
  range: string;
  end: string | Date;
  count: number;
  measures: Measure[];
  segments?: Segment[]; //`sparse` output does not subdived by datetime segments
}

interface AggregatedData {
  [key: string]: number;
}

interface IdsData {
  [target: string]: string[];
}

interface MeasureIds {
  time: string;
  target: string;
  count: number;
  type: 'ids';
  ids: string[];
}

interface TimeSegment {
  time: string;
  measures: MeasureIds[];
}

interface CountByFacet {
  facet: string;
  count: number;
}

interface IdsResponse {
  key: string;
  facets: string[];
  granularity: string;
  range: string;
  start: string;
  counts: CountByFacet[];
  segments: TimeSegment[];
}

export {
  StatsType,
  StatType,
  MetricTypes,
  JobStats,
  JobStatsRange,
  JobStatsInput,
  GetStatsOptions,
  StatsResponse,
  AggregatedData,
  Measure,
  Segment,
  IdsData,
  MeasureIds,
  TimeSegment,
  IdsResponse,
  CountByFacet,
};
