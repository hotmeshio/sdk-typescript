import { HMNS } from '../../../../modules/key';
import { ILogger } from '../../../../services/logger';
import { ReporterService } from '../../../../services/reporter';
import { IORedisStoreService as IORedisStore } from '../../../../services/store/clients/ioredis';
import { JobStatsRange } from '../../../../types/stats';
import { RedisClientType, RedisConnection } from '../../../$setup/cache/ioredis';

jest.mock('../../../../services/store/clients/ioredis', () => {
  return {
    IORedisStoreService: jest.fn().mockImplementation(() => {
      return {
        getJobStats: jest.fn(),
        getJobIds: jest.fn(),
      };
    }),
  };
});

const getTimeSeries = (granularity = '5m', minutesInThePast = 0): string => {
  const _now = new Date();
  const now = new Date(_now.getTime() - minutesInThePast * 60 * 1000);
  const granularityUnit = granularity.slice(-1);
  const granularityValue = parseInt(granularity.slice(0, -1), 10);
  if (granularityUnit === 'm') {
    const minute = Math.floor(now.getMinutes() / granularityValue) * granularityValue;
    now.setUTCMinutes(minute, 0, 0);
  } else if (granularityUnit === 'h') {
    now.setUTCMinutes(0, 0, 0);
  }
  return now.toISOString().replace(/:\d\d\..+|-|T/g, '').replace(':','');
};

describe('ReporterService', () => {
  const CONNECTION_KEY = 'manual-test-connection';
  const appId = 'test-app';
  const appVersion = '1';
  let reporter: ReporterService;
  let redisConnection: RedisConnection;
  let redisClient: RedisClientType;
  let redisStore: IORedisStore;

  const logger: ILogger = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };

  beforeAll(async () => {
    redisConnection = await RedisConnection.getConnection(CONNECTION_KEY);
    redisClient = await redisConnection.getClient();
    redisClient.flushdb();
    redisStore = new IORedisStore(redisClient);
  });

  beforeEach(() => {
    jest.resetAllMocks();
    reporter = new ReporterService({ id: appId, version: appVersion }, redisStore, logger);
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('getStats()', () => {
    it('should return correct stats for given options', async () => {
      const options = {
        key: 'widgetB',
        granularity: '5m',
        range: '1h',
        end: 'NOW',
      };
      const sampleRedisData: JobStatsRange = {
        [`${HMNS}:${appId}:s:${options.key}:${getTimeSeries(options.granularity, 10)}`]: {
          'count': 25,
          'count:color:12315': 5,
          'count:size:12145': 5,
          'count:color:12335': 5,
          'count:size:12345': 5,
          'count:color:12355': 5,
          'count:size:12545': 5,
          'count:color:12375': 5,
          'count:size:12745': 5,
          'count:color:12395': 5,
          'count:size:12945': 5,
        },
        [`${HMNS}:${appId}:s:${options.key}:${getTimeSeries(options.granularity)}`]: {
          'count': 15,
          'count:color:12315': 5,
          'count:size:12145': 4,
          'count:color:12335': 3,
          'count:size:12345': 2,
          'count:color:12355': 1,
          'count:size:12545': 5,
          'count:color:12375': 4,
          'count:size:12745': 3,
          'count:color:12395': 2,
          'count:size:12945': 1,
        },
      };

      (redisStore.getJobStats as jest.Mock).mockResolvedValue(sampleRedisData);
      const stats = await reporter.getStats(options);
      expect(stats.key).toBe(options.key);
      expect(stats.granularity).toBe(options.granularity);
      expect(stats.range).toBe(options.range);
      expect(stats.end).toBe(options.end);
    });

    it('should return sparse stats with only top-level aggregations', async () => {
      const options = {
        key: 'widgetB',
        granularity: '5m',
        range: '1h',
        end: 'NOW',
        sparse: true,
      };
      const sampleRedisData: JobStatsRange = {
        [`${HMNS}:${appId}:s:${options.key}:${getTimeSeries(options.granularity, 10)}`]: {
          'count': 25,
          'count:color:12315': 5,
          'count:size:12145': 5,
          'count:color:12335': 5,
          'count:size:12345': 5,
          'count:color:12355': 5,
          'count:size:12545': 5,
          'count:color:12375': 5,
          'count:size:12745': 5,
          'count:color:12395': 5,
          'count:size:12945': 5,
        },
        [`${HMNS}:${appId}:s:${options.key}:${getTimeSeries(options.granularity)}`]: {
          'count': 15,
          'count:color:12315': 5,
          'count:size:12145': 4,
          'count:color:12335': 3,
          'count:size:12345': 2,
          'count:color:12355': 1,
          'count:size:12545': 5,
          'count:color:12375': 4,
          'count:size:12745': 3,
          'count:color:12395': 2,
          'count:size:12945': 1,
        },
      };

      (redisStore.getJobStats as jest.Mock).mockResolvedValue(sampleRedisData);
      const stats = await reporter.getStats(options);
      expect(stats.key).toBe(options.key);
      expect(stats.granularity).toBe(options.granularity);
      expect(stats.range).toBe(options.range);
      expect(stats.end).toBe(options.end);
      expect(stats.segments).toBeUndefined();
    });
  });

  describe('getJobIds', () => {
    it('should return ids data based on given options and facets', async () => {
      const getStatsOptions = {
        key: 'testKey',
        granularity: '5m',
        range: '1h',
        end: 'NOW',
      };
      const facets = ['facet1', 'facet2'];
      const mockedJobIds = {
        'some:key:index:facet1': ['id1', 'id2'],
        'some:key:index:facet2': ['id3', 'id4'],
      };

      (redisStore.getJobIds as jest.Mock).mockResolvedValue(mockedJobIds);
      const result = await reporter.getIds(getStatsOptions, facets);
      expect(redisStore.getJobIds).toHaveBeenCalledWith(expect.any(Array), expect.any(Object));
      expect(result).toHaveProperty('key', getStatsOptions.key);
      expect(result).toHaveProperty('facets', facets);
      expect(result).toHaveProperty('granularity', getStatsOptions.granularity);
      expect(result).toHaveProperty('range', getStatsOptions.range);
      expect(result).toHaveProperty('counts');
      expect(result).toHaveProperty('segments');
    });
  });

  describe('getWorkItems', () => {
    it('should return Redis keys based on given options and facets', async () => {
      const getStatsOptions = {
        key: 'testKey',
        granularity: '5m',
        range: '1h',
        end: 'NOW',
      };
      const facets = ['facet1', 'facet2'];
      const mockedIdsData = {
        'some:key:index:facet1': ['id1', 'id2'],
        'some:key:index:facet2': ['id3', 'id4'],
      };
      const expectedWorkerLists = [
        'some:key:index:facet1',
        'some:key:index:facet2',
      ];
      (redisStore.getJobIds as jest.Mock).mockResolvedValue(mockedIdsData);
      const result = await reporter.getWorkItems(getStatsOptions, facets);
      expect(redisStore.getJobIds).toHaveBeenCalledWith(expect.any(Array), expect.any(Array));
      expect(result).toEqual(expectedWorkerLists);
    });
  });  
});
