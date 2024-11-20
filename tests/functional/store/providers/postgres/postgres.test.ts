import { Client } from 'pg';

import config from '../../../../$setup/config'
import { KeyType, HMNS, VALSEP } from '../../../../../modules/key';
import { getSymKey, sleepFor } from '../../../../../modules/utils';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { LoggerService } from '../../../../../services/logger';
import { MDATA_SYMBOLS } from '../../../../../services/serializer';
import { PostgresStoreService } from '../../../../../services/store/providers/postgres/postgres';
import { ActivityType, Consumes } from '../../../../../types/activity';
import { HookSignal } from '../../../../../types/hook';
import {
  StringAnyType,
  StringStringType,
  Symbols,
} from '../../../../../types/serializer';
import { StatsType } from '../../../../../types/stats';
import { PostgresClientType } from '../../../../../types/postgres';
import { ProviderNativeClient, ProviderClient } from '../../../../../types/provider';

describe('FUNCTIONAL | PostgresStoreService', () => {
  const appConfig = { id: 'mystoreapp', version: '1' };
  const cacheConfig = { appId: 'mystoreapp', appVersion: '1' };
  let postgresClient: ProviderNativeClient;
  let postgresStoreService: PostgresStoreService;
  let tables: string[] = [];

  beforeAll(async () => {
    // Initialize PostgreSQL connection
    postgresClient = (
      await PostgresConnection.connect('test', Client, {
        user: config.POSTGRES_USER,
        host: config.POSTGRES_HOST,
        database: config.POSTGRES_DB,
        password: config.POSTGRES_PASSWORD,
        port: config.POSTGRES_PORT,
      })
    ).getClient();

    // Query the list of tables in the public schema dynamically
    const result = await postgresClient.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public';
    `);

    tables = result.rows.map(row => row.table_name);

    // Drop each table if it exists
    for (const table of tables) {
      await postgresClient.query(`DROP TABLE IF EXISTS ${table}`);
    }
    
    postgresStoreService = new PostgresStoreService(
      postgresClient as PostgresClientType & ProviderClient,
    );
    await postgresStoreService.init(
      HMNS,
      appConfig.id,
      new LoggerService()
    );
  });

  beforeEach(async () => {
    await postgresClient.query('BEGIN');
    try {
      for (const table of tables) {
        const res = await postgresClient.query(
          `SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = $1
          );`,
          [table]
        );
        if (res.rows[0].exists) {
          await postgresClient.query(`TRUNCATE ${table} RESTART IDENTITY CASCADE`);
        }
      }
      await postgresClient.query('COMMIT');
    } catch (error) {
      await postgresClient.query('ROLLBACK');
      throw error;
    }
  });
   

  afterEach(async () => {
  });

  afterAll(async () => {
    await postgresClient.end();
  });

  describe('mintKey', () => {
    it('should mint the key to access hotmesh global settings', () => {
      const result = postgresStoreService.mintKey(KeyType.HOTMESH, {});
      expect(result).toEqual(HMNS);
    });

    it('should mint the key to access hotmesh apps', () => {
      const result = postgresStoreService.mintKey(KeyType.APP, cacheConfig);
      expect(result).toEqual(`${HMNS}:a:${cacheConfig.appId}`);
    });
  });

  describe('reserveSymbolRange', () => {
    it('should reserve a symbol range for a given activity and handle existing values', async () => {
      const activityId = 'a1';
      const size = 286;

      // First case: No existing key
      let [lowerLimit, upperLimit] = await postgresStoreService.reserveSymbolRange(
        activityId,
        size,
        'ACTIVITY',
      );
      expect(lowerLimit).toEqual(26); //0 + reserved metadata slots (first available slot)
      const rangeKey = postgresStoreService.mintKey(KeyType.SYMKEYS, {
        appId: appConfig.id,
      });

      const range = await postgresStoreService.storeClient.hget(rangeKey, activityId);
      expect(range).toEqual(
        `${lowerLimit - MDATA_SYMBOLS.SLOTS}:${lowerLimit - MDATA_SYMBOLS.SLOTS + size - 1}`,
      );
      //26 metadata slots are reserved; lowerLimit = 26; upperLimit = 286 - 1 = 285
      const symbols: Symbols = {
        'a1/data/abc': getSymKey(lowerLimit),
        'a1/data/def': getSymKey(lowerLimit + 1),
      };

      // Second case : Existing key
      await postgresStoreService.addSymbols(activityId, symbols);
      let savedSymbols: Symbols;
      [lowerLimit, upperLimit, savedSymbols] =
        await postgresStoreService.reserveSymbolRange(
          activityId,
          size,
          'ACTIVITY',
        );

        expect(lowerLimit).toEqual(
        MDATA_SYMBOLS.SLOTS + MDATA_SYMBOLS.ACTIVITY.KEYS.length + 2,
      ); //lower limit starts at first usable slot
      expect(upperLimit).toEqual(size - 1); // [0 + ]286 - 1 = 285
      expect(Object.keys(savedSymbols).length).toEqual(
        MDATA_SYMBOLS.ACTIVITY.KEYS.length + 2,
      ); //total of meta/data keys
    });
  });

  describe('getSymbols', () => {
    it('should retrieve symbols for a given activity', async () => {
      const activityId = 'a2';
      const appId = 'app2';
      const symbols: Symbols = {
        'a2/data/ghi': 'baa',
        'a2/data/jkl': 'bab',
      };
      await postgresStoreService.addSymbols(activityId, symbols);
      const result = await postgresStoreService.getSymbols(activityId);
      expect(result).toEqual(symbols);
    });
  });

  describe('addSymbols', () => {
    it('should store symbols for a given activity', async () => {
      const activityId = 'a3';
      const appId = 'app3';
      const symbols: Symbols = {
        'a3/data/mno': 'caa',
        'a3/data/pqr': 'cab',
      };
      const result = await postgresStoreService.addSymbols(activityId, symbols);
      expect(result).toEqual(true);
    });
  });

  describe('setState/getState', () => {
    it('sets and gets job/activity state', async () => {
      //1) add symbol sets for activity a1
      const jobId = 'jid';
      const topic = '$job.topic';
      const activityId = 'a1';
      const size = 286;

      let [lowerLimit] = await postgresStoreService.reserveSymbolRange(
        activityId,
        size,
        'ACTIVITY',
      );

      let symbols: Symbols = {
        'a1/output/data/some/field': getSymKey(lowerLimit),
        'a1/output/data/another/field': getSymKey(lowerLimit + 1),
      };
      await postgresStoreService.addSymbols(activityId, symbols);

      
      //2) add symbol sets for the parent job/topic ($job.topic)
      const response2 = await postgresStoreService.reserveSymbolRange(
        topic,
        size,
        'JOB',
      );
      lowerLimit = response2[0];

      symbols = {
        'data/name': getSymKey(lowerLimit),
        'data/age': getSymKey(lowerLimit + 1),
      };
      await postgresStoreService.addSymbols(topic, symbols);

      //3) set job state
      const jobStatus = 1;
      const jobState: StringAnyType = {
        'a1/output/data/some/field': true,
        'a1/output/data/another/field': { complex: 'object' },
        'a1/output/metadata/aid': activityId,
        'a1/output/metadata/atp': 'activity',
        'data/name': new Date(),
        'data/age': 55,
        'metadata/jid': jobId,
        'metadata/js': jobStatus,
      };
      const dIds: StringStringType = {
        a1: ',0,0',
      };
      const result = await postgresStoreService.setState(
        jobState,
        jobStatus,
        jobId,
        [activityId, topic],
        dIds,
      );
      expect(result).toEqual(jobId);

      //4) get job state
      const consumes: Consumes = {
        [activityId]: [
          'a1/output/data/some/field',
          'a1/output/data/another/field',
          'a1/output/metadata/aid',
          'a1/output/metadata/atp',
        ],
        [topic]: ['data/name', 'data/age', 'metadata/jid'],
      };
      const response = await postgresStoreService.getState(jobId, consumes, dIds);
      if (response) {
        const [resolvedJobState, resolvedJobStatus] = response;
        expect(resolvedJobState).toEqual(jobState);
        expect(resolvedJobStatus).toEqual(jobStatus);
      } else {
        fail('Job state/status not found');
      }
    });
  });

  describe('exec', () => {
    it('should execute an arbitrary command', async () => {
      const setResponse = await postgresStoreService.storeClient.set(
        `hmsh:${appConfig.id}:w:time`,
        '25',
      );

      expect(setResponse).toEqual(true);
      const getResponse = await postgresStoreService.storeClient.get(
        `hmsh:${appConfig.id}:w:time`
      );
      expect(getResponse).toEqual('25');

      const resp = await postgresStoreService.storeClient.hset(
        `hmsh:${appConfig.id}:j:myjob`,
        { a: '25', b: '50'}
      );
      expect(resp).toEqual(2);

      const delResp = await postgresStoreService.storeClient.hdel(
        `hmsh:${appConfig.id}:j:myjob`,
        ['a', 'b'],
      );
      expect(delResp).toEqual(2);
    });
  });

  describe('findJobs', () => {
    it('should find hashes matching the query string', async () => {
      const hashKey = postgresStoreService.mintKey(KeyType.JOB_STATE, {
        appId: appConfig.id,
        jobId: '',
      });
      for (let i = 0; i < 10; i++) {
        await postgresStoreService.storeClient.hset(`${hashKey}${i}`, { a: '25' });
      }
      const [cursor, result] = await postgresStoreService.findJobs('*', 15, 15);
      expect(result.length).toEqual(10);
      const [cursor2, result2] = await postgresStoreService.findJobs(
        'xxx',
        15,
        15,
      );
      expect(result2.length).toEqual(0);
    });
  });

  describe('findJobFields', () => {
    it('should find hash fields matching the query string', async () => {
      const hashKey = postgresStoreService.mintKey(KeyType.JOB_STATE, {
        appId: appConfig.id,
        jobId: 'test',
      });
      for (let i = 0; i < 10; i++) {
        await postgresStoreService.storeClient.hset(
          hashKey,
          {
            '-b1,0,0-1': '1',
            '-b1,1,0-1': '1',
            '-a1': '1',
            '-a2': '2',
            '-a3': '3',
            '-a4': '4',
            '-a5': '5',
            '-a6': '6',
            '-a7': '7',
            'a8': '8',
          }
        );        
      }
      const [_cursor, result] = await postgresStoreService.findJobFields(
        'test',
        '-*[ehklptydr]-*', //anything without a comma
        7, //limit to 7 results
        2, //unused
      );
      expect(Object.keys(result).length).toBeGreaterThanOrEqual(7);
      const [_cursor2, result2] = await postgresStoreService.findJobFields(
        'test',
        '-*0,0-*',
        20, //limit to 20 results (only 1 should match and be returned)
        10, //unused
      );
      expect(Object.keys(result2).length).toEqual(1);
    });
  });

  describe('setStateNX', () => {
    it('should set the job data in the store with NX behavior', async () => {
      const jobId = 'job-1';
      const response = await postgresStoreService.setStateNX(jobId, appConfig.id);
      expect(response).toEqual(true);
      const secondResponse = await postgresStoreService.setStateNX(
        jobId,
        appConfig.id,
      );
      expect(secondResponse).toEqual(false);
      const hashKey = postgresStoreService.mintKey(KeyType.JOB_STATE, {
        appId: appConfig.id,
        jobId,
      });
      const storedActivityId = await postgresStoreService.storeClient.hget(
        hashKey,
        ':',
      );
      expect(storedActivityId).toEqual('1');
    });
  });

  describe('setStats', () => {
    it('should set and get job stats correctly', async () => {
      const jobKey = 'job-key';
      const jobId = `hmsh:${appConfig.id}:j:job-id`;
      const dateTime = '202304170000';
      const stats: StatsType = {
        general: [{ metric: 'count', target: 'target1', value: 1 }],
        index: [{ metric: 'index', target: 'target2', value: 20 }],
        median: [{ metric: 'mdn', target: 'target3', value: 30 }],
      };

      const result = await postgresStoreService.setStats(
        jobKey,
        jobId,
        dateTime,
        stats,
        appConfig,
      );
      expect(result).not.toBeNull();

      const generalStatsKey = postgresStoreService.mintKey(
        KeyType.JOB_STATS_GENERAL,
        { ...cacheConfig, jobId, jobKey, dateTime },
      );
      const generalStats = await postgresStoreService.storeClient.hgetall(generalStatsKey);
      expect(generalStats[stats.general[0].target]).toEqual(
        stats.general[0].value.toString(),
      );

      const indexStatsKey = postgresStoreService.mintKey(KeyType.JOB_STATS_INDEX, {
        ...cacheConfig,
        jobId,
        jobKey,
        dateTime,
        facet: stats.index[0].target,
      });
      const indexStats = await postgresStoreService.storeClient.lrange(indexStatsKey, 0, -1);
      expect(indexStats[0]).toEqual(stats.index[0].value.toString());

      const medianStatsKey = postgresStoreService.mintKey(
        KeyType.JOB_STATS_MEDIAN,
        {
          ...cacheConfig,
          jobId,
          jobKey,
          dateTime,
          facet: stats.median[0].target,
        },
      );

      const medianStats = await postgresStoreService.storeClient.zrange(
        medianStatsKey,
        0,
        -1,
        'WITHSCORES',
      );
      expect(medianStats[1]).toEqual(stats.median[0].value.toString());

      //expect getStats to cast the value to a number, so it is an exact match even though a string in redis
      const jobStats = await postgresStoreService.getJobStats([generalStatsKey]);
      expect(jobStats[generalStatsKey][stats.general[0].target]).toEqual(
        stats.general[0].value,
      );
    });
  });

  describe('Time Triggers', () => {
    it('should register job activities for sleep/awake events', async () => {
      // Register jobs to be awakened/triggered
      const jobId1 = 'job-id-1';
      const jobId2 = 'job-id-2';
      const gId1 = 'gid-1';
      const gId2 = 'gid-2';
      const dad1 = ',0,1';
      const dad2 = ',0,2';
      const activityId = 'activity-id';
      const awakenTime = Date.now();
      const type = 'sleep';
      await postgresStoreService.registerTimeHook(
        jobId1,
        gId1,
        activityId,
        type,
        awakenTime,
        dad1,
      );
      await postgresStoreService.registerTimeHook(
        jobId2,
        gId2,
        activityId,
        type,
        awakenTime,
        dad2,
      );

      // Check that the jobs were added to the correct list
      const listKey = postgresStoreService.mintKey(KeyType.TIME_RANGE, {
        appId: appConfig.id,
        timeValue: awakenTime,
      });

      const jobList = await postgresStoreService.storeClient.lrange(listKey, 0, -1);
      expect(jobList?.[0]).toEqual(
        `${type}${VALSEP}${activityId}${VALSEP}${gId1}${VALSEP}${dad1}${VALSEP}${jobId1}`,
      );
      expect(jobList?.[1]).toEqual(
        `${type}${VALSEP}${activityId}${VALSEP}${gId2}${VALSEP}${dad2}${VALSEP}${jobId2}`,
      );

      // Retrieve the next job to be triggered (to receive a time event)
      const [nextListKey, nextJobId, nextGID, nextActivityId] =
        (await postgresStoreService.getNextTask()) as [
          string,
          string,
          string,
          string,
          'sleep' | 'expire' | 'interrupt',
        ];
      expect(nextListKey).toEqual(listKey);
      expect(nextJobId).toEqual(jobId1);
      expect(nextGID).toEqual(gId1);
      expect(nextActivityId).toEqual(activityId);
      // Check that jobId1 was removed from the list
      const updatedJobList = await postgresStoreService.storeClient.lrange(listKey, 0, -1);
      expect(updatedJobList.length).toEqual(1);
      expect(updatedJobList[0]).toEqual(
        `${type}${VALSEP}${activityId}${VALSEP}${gId2}${VALSEP}${dad2}${VALSEP}${jobId2}`,
      );
    });
  });

  describe('getSchema', () => {
    it('should retrieve the schema for the given topic from the store', async () => {
      const topic = 'topic1';
      const schemas: Record<string, ActivityType> = {
        topic1: {
          type: 'activity',
        },
        topic2: {
          type: 'trigger',
        },
      };
      await postgresStoreService.setSchemas(schemas, appConfig);
      const result = await postgresStoreService.getSchema(topic, appConfig);
      expect(result).toEqual(schemas[topic]);
    });
  });

  describe('getSchemas', () => {
    it('should retrieve all schemas from the store', async () => {
      const schemas: Record<string, ActivityType> = {
        topic1: {
          type: 'activity',
        },
        topic2: {
          type: 'trigger',
        },
      };
      await postgresStoreService.setSchemas(schemas, appConfig);
      const result = await postgresStoreService.getSchemas(appConfig);
      expect(result).toEqual(schemas);
    });
  });

  describe('setSchemas', () => {
    it('should store all schemas in the store', async () => {
      const schemas: Record<string, ActivityType> = {
        topic1: {
          type: 'activity',
        },
        topic2: {
          type: 'trigger',
        },
      };
      const result = await postgresStoreService.setSchemas(schemas, appConfig);
      expect(result).toEqual(2); //number of schemas
    });
  });

  describe('reserveScoutRole', () => {
    it('should reserve an expirable role when available', async () => {
      const role = 'time';
      const result = await postgresStoreService.reserveScoutRole(role, 3);
      expect(result).toEqual(true);
      const key = postgresStoreService.mintKey(KeyType.WORK_ITEMS, {
        appId: appConfig.id,
        scoutType: role,
      });
      const reservedRole = await postgresStoreService.storeClient.get(key);
      expect(reservedRole).not.toBeNull();

      const result2 = await postgresStoreService.reserveScoutRole(role, 3);
      expect(result2).toEqual(false);
      const reservedRole2 = await postgresStoreService.storeClient.get(key);
      expect(reservedRole2).not.toBeNull();

      await sleepFor(2000);
      const reservedRole3 = await postgresStoreService.storeClient.get(key);
      expect(reservedRole3).toBeNull();
    });
  });

  describe('addTaskQueues', () => {
    it('should enqueue work items correctly', async () => {
      const keys = ['work-item-1', 'work-item-2', 'work-item-3'];
      await postgresStoreService.addTaskQueues(keys);
      const zsetKey = postgresStoreService.mintKey(KeyType.WORK_ITEMS, {
        appId: appConfig.id,
      });
      for (const key of keys) {
        const score = await postgresStoreService.storeClient.zscore(zsetKey, key);
        expect(score).not.toBeNull();
      }
    });

    it('should not overwrite existing work items with the same key', async () => {
      const existingKey = 'work-item-existing';
      const existingScore = Date.now() - 1000;
      const zsetKey = postgresStoreService.mintKey(KeyType.WORK_ITEMS, {
        appId: appConfig.id,
      });
      await postgresStoreService.storeClient.zadd(zsetKey, existingScore.toString(), existingKey);
      await postgresStoreService.addTaskQueues([existingKey]);
      const newScore = await postgresStoreService.storeClient.zscore(zsetKey, existingKey);
      expect(newScore).toEqual(existingScore);
    });
  });

  describe('getActiveTaskQueue', () => {
    beforeEach(async () => {
      postgresStoreService.cache.invalidate();
    });

    it('should return null if no work items are available', async () => {
      const workItemKey = await postgresStoreService.getActiveTaskQueue();
      expect(workItemKey).toBeNull();
    });

    it('should return the work item with the lowest score', async () => {
      const workItems = [
        { key: 'work-item-1', score: 1000 },
        { key: 'work-item-2', score: 2000 },
        { key: 'work-item-3', score: 3000 },
      ];
      const zsetKey = postgresStoreService.mintKey(KeyType.WORK_ITEMS, {
        appId: appConfig.id,
      });
      for (const item of workItems) {
        await postgresStoreService.storeClient.zadd(
          zsetKey,
          item.score.toString(),
          item.key,
        );
      }
      const workItemKey = await postgresStoreService.getActiveTaskQueue();
      expect(workItemKey).toEqual(workItems[0].key);
    });

    it('should return work item from cache if available', async () => {
      const cachedKey = 'work-item-cached';
      const zsetKey = postgresStoreService.mintKey(KeyType.WORK_ITEMS, {
        appId: appConfig.id,
      });
      await postgresStoreService.storeClient.zadd(zsetKey, '1000', cachedKey);
      postgresStoreService.cache.setWorkItem(appConfig.id, cachedKey);
      const workItemKey = await postgresStoreService.getActiveTaskQueue();
      expect(workItemKey).toEqual(cachedKey);
    });
  });

  describe('deleteProcessedTaskQueue', () => {
    beforeEach(async () => {
      postgresStoreService.cache.invalidate();
    });

    it('should remove the work item and processed item', async () => {
      const workItemKey = `hmsh:${appConfig.id}:w:working`; //'w:*' resolves to a 'string' key type
      const key = 'item-1';
      const processedKey = `hmsh:${appConfig.id}:w:processes`;
      const zsetKey = postgresStoreService.mintKey(KeyType.WORK_ITEMS, {
        appId: appConfig.id,
      });
      await postgresStoreService.storeClient.zadd(
        zsetKey,
        1000,
        workItemKey,
        { NX: 'NX' },
      );
      await postgresStoreService.storeClient.set(processedKey, 'processed data'); //set the string
      await postgresStoreService.deleteProcessedTaskQueue(
        workItemKey,
        key,
        processedKey,
      );
      const workItemExists =
        await postgresStoreService.storeClient.exists(workItemKey);
      const processedItemExists =
        await postgresStoreService.storeClient.exists(processedKey);
      const workItemInZSet = await postgresStoreService.storeClient.zrank(
        zsetKey,
        workItemKey,
      );
      expect(workItemExists).toBe(0);
      expect(processedItemExists).toBe(0);
      expect(workItemInZSet).toBeNull();
    });

    it('should remove the work item from the cache', async () => {
      const workItemKey = 'work-item-cached';
      const key = 'item-cached';
      const processedKey = 'processed-item-cached';
      postgresStoreService.cache.setWorkItem(appConfig.id, workItemKey);
      await postgresStoreService.deleteProcessedTaskQueue(
        workItemKey,
        key,
        processedKey,
      );
      const cachedWorkItem = postgresStoreService.cache.getActiveTaskQueue(
        appConfig.id,
      );
      expect(cachedWorkItem).toBeUndefined();
    });
  });

  describe('processTaskQueue', () => {
    const sourceKey = `hmsh:${appConfig.id}:t:source-list`;
    const destinationKey = `hmsh:${appConfig.id}:t:destination-list`;
    const item1 = 'item-1';
    const item2 = 'item-2';

    beforeEach(async () => {
      await postgresStoreService.storeClient.del(sourceKey);
      await postgresStoreService.storeClient.del(destinationKey);
    });

    it('should move an item from the source list to the destination list', async () => {
      await postgresStoreService.storeClient.lpush(sourceKey, [item1, item2]);
      const val2 = await postgresStoreService.processTaskQueue(
        sourceKey,
        destinationKey,
      );
      let sourceList = await postgresStoreService.storeClient.lrange(
        sourceKey,
        0,
        -1,
      );
      let destinationList = await postgresStoreService.storeClient.lrange(
        destinationKey,
        0,
        -1,
      );
      expect(val2).toEqual(item2);
      expect(sourceList).toEqual([item1]);
      expect(destinationList).toEqual([item2]);
      const val1 = await postgresStoreService.processTaskQueue(
        sourceKey,
        destinationKey,
      );
      expect(val1).toEqual(item1);
      sourceList = await postgresStoreService.storeClient.lrange(sourceKey, 0, -1);
      destinationList = await postgresStoreService.storeClient.lrange(
        destinationKey,
        0,
        -1,
      );
      const val3 = await postgresStoreService.processTaskQueue(
        sourceKey,
        destinationKey,
      );
      expect(val3).toEqual(null);
      expect(sourceList).toEqual([]);
      expect(destinationList).toEqual([item2, item1]);
    });

    it('should not move any item when the source list is empty', async () => {
      await postgresStoreService.processTaskQueue(sourceKey, destinationKey);
      const sourceList = await postgresStoreService.storeClient.lrange(
        sourceKey,
        0,
        -1,
      );
      const destinationList = await postgresStoreService.storeClient.lrange(
        destinationKey,
        0,
        -1,
      );
      expect(sourceList).toEqual([]);
      expect(destinationList).toEqual([]);
    });
  });

  describe('setHookSignal', () => {
    it('should set the hook correctly', async () => {
      const hook: HookSignal = {
        topic: 'test-topic',
        resolved: 'test-resolved',
        jobId: 'test-job-id',
        expire: 100,
      };
      await postgresStoreService.setHookSignal(hook);
      const key = postgresStoreService.mintKey(KeyType.SIGNALS, {
        appId: appConfig.id,
      });
      const value = await postgresStoreService.storeClient.get(
        `${key}:${hook.topic}:${hook.resolved}`,
      );
      expect(value).toEqual(hook.jobId);
    });
  });

  describe('getHookSignal', () => {
    it('should set and get the hook', async () => {
      const hook: HookSignal = {
        topic: 'test-topic',
        resolved: 'test-resolved',
        jobId: 'test-job-id',
        expire: 60,
      };
      await postgresStoreService.setHookSignal(hook);
      const retrievedSignal = await postgresStoreService.getHookSignal(
        hook.topic,
        hook.resolved,
      );
      expect(retrievedSignal).toEqual(hook.jobId);
      const key = postgresStoreService.mintKey(KeyType.SIGNALS, {
        appId: appConfig.id,
      });
      const remainingValue = await postgresStoreService.storeClient.get(
        `${key}:${hook.topic}:${hook.resolved}`,
      );
      expect(remainingValue).toEqual(hook.jobId);
    });
  });

  describe('deleteHookSignal', () => {
    it('should delete the hook', async () => {
      const hook: HookSignal = {
        topic: 'test-topic',
        resolved: 'test-resolved',
        jobId: 'test-job-id',
        expire: 90,
      };
      await postgresStoreService.setHookSignal(hook);
      let retrievedSignal = await postgresStoreService.getHookSignal(
        hook.topic,
        hook.resolved,
      );
      expect(retrievedSignal).not.toBeNull();
      let deletedCount = await postgresStoreService.deleteHookSignal(
        hook.topic,
        hook.resolved,
      );
      expect(deletedCount).toEqual(1);
      retrievedSignal = await postgresStoreService.getHookSignal(
        hook.topic,
        hook.resolved,
      );
      expect(retrievedSignal).toBeUndefined();
      deletedCount = await postgresStoreService.deleteHookSignal(
        hook.topic,
        hook.resolved,
      );
      expect(deletedCount).toBeUndefined();
    });
  });
});
