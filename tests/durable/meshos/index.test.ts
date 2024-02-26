import * as Redis from 'redis';

import config from '../../$setup/config'
import { RedisConnection } from '../../../services/connector/clients/redis';
import { guid, sleepFor } from '../../../modules/utils';
import { MyClass as MeshOSTest } from './src/subclass';
import { Durable } from '../../../services/durable';

describe('DURABLE | MeshOS', () => {
  const prefix = 'ord_';
  const GUID = `${prefix}${guid()}`;
  const options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      options
    );
    redisConnection.getClient().flushDb();
  });

  afterAll(async () => {
    await sleepFor(5000);
    await Durable.shutdown();
  }, 25_000);

  describe('Worker', () => {
    it('should start the workers', async () => {
      await MeshOSTest.startWorkers();
    }, 15_000);
  });

  describe('Search', () => {
    it('should create a search index', async () => {
      await MeshOSTest.createIndex();
    });
  });

  describe('Create/Start a Workflow and await the result', () => {
    it('should start a new workflow and await', async () => {
      const client = new MeshOSTest({ id: guid(), await: true });
      const doubled = await client.stringDoubler('hello');
      expect(doubled).toBe('hellohello');
    });
  });

  describe('Create/Start a Workflow and return the workflow handle', () => {
    it('should start a new workflow', async () => {
      const client = new MeshOSTest(GUID);
      const handle = await client.create(100);
      expect(handle).not.toBeUndefined();
    });
  });

  describe('Query Custom Value', () => {
    it('should query for custom state fields', async () => {
      const handle = await MeshOSTest.get(GUID);
      let result = await handle.queryState(['quantity']);
      while (result.quantity !== '100') {
        await sleepFor(500);
        result = await handle.queryState(['quantity']);
      }
      expect(result).not.toBeUndefined();
    }, 10_000);
  });

  describe('Update Workflow', () => {
    it('should hook into a running workflow and update state', async () => {
      const client = new MeshOSTest(GUID);
      const result = await client.decrement(11);
      expect(result).not.toBeUndefined();
    }, 10_000);
  });

  describe('Get Workflow', () => {
    it('should get the workflow status', async () => {
      const handle = await MeshOSTest.get(GUID);
      const result = await handle.status();
      expect(result).not.toBeUndefined();
    });

    it('should get the workflow data and metadata', async () => {
      const handle = await MeshOSTest.get(GUID);
      const result = await handle.state(true);
      expect(result).not.toBeUndefined();
    });
  });

  describe('Search', () => {
    it('should find workflows using `find`', async () => {
      let count: any;
      let rest: any;
      do {
        [count, ...rest] = await MeshOSTest.find(
          {},
          'FT.SEARCH',
          'inventory-orders',
          '@_quantity:[89 89] @_quality:"great"',
          'RETURN',
          '1',
          '_quantity'
        );
        await sleepFor(500);
      } while (count as number === 0);
      expect(count).toBe(1);
    }, 15_000);

    it('should find workflows using `findWhere` convenience method', async () => {
      let count: any;
      let rest: any;
      //loop like the prior test, as this test adds a condition (e.g., `status=ordered`)
      // which might not get set in time. If an additional condition were not to have
      // been added, it would be unnecessary to wait as the prior test already established
      // that there is a record where `quantity=89 AND quality=great`
      do {
        [count, ...rest] = await MeshOSTest.findWhere(
          { query: [
              { field: 'quantity', is: '=', value: 89 },
              { field: 'quality', is: '=', value: 'great'},
              { field: 'status', is: '=', value: 'ordered' }
            ],
            return: ['quantity', 'status']
          });
        await sleepFor(500);
      } while (count as number === 0);
      const [key, items] = rest;
      const [f1, v1, f2, v2] = items as unknown as string[];
      expect(count).toBe(1);
      expect((key as string).includes(GUID)).toBe(true);
      expect(f1).toBe('_quantity');
      expect(v1).toBe('89');
      expect(f2).toBe('_status');
      expect(v2).toBe('ordered');
    }, 15_000);

    it('should count workflows using `findWhere` convenience method', async () => {
      const [count, ...rest] = await MeshOSTest.findWhere(
        { query: [
            { field: 'quantity', is: '>=', value: 88 }
          ],
          count: true 
        });
      expect(count).toBe(1);
      expect(rest.length).toBe(0);
    });

    it('should paginate workflows using `findWhere` convenience method', async () => {
      const [count, ...rest] = await MeshOSTest.findWhere(
        { query: [
            { field: 'quantity', is: '[]', value: [88, 90] } //between 88 and 90
          ],
          return: ['status'],
          limit: { start: 0, size: 1 } 
        });
      expect(count).toBe(1);
      expect(rest.length).toBe(2); //[key, [fields]]
    });
  });

  describe('Result', () => {
    it('should publish the workflow results', async () => {
      const handle = await MeshOSTest.get(GUID);
      const result = await handle.result(true);
      expect(result).toBe('89');
    }, 60_000);
  });
});
