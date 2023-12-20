import * as Redis from 'redis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import { nanoid } from 'nanoid';
import { RedisConnection } from '../../../services/connector/clients/redis';
import { StreamSignaler } from '../../../services/signaler/stream';
import { sleepFor } from '../../../modules/utils';
import { MyClass as MeshOSTest } from './src/subclass';

describe('DURABLE | MeshOS', () => {
  const prefix = 'ord_';
  const guid = `${prefix}${nanoid()}`;
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
      nanoid(),
      Redis,
      options
    );
    redisConnection.getClient().flushDb();
  });

  afterAll(async () => {
    await sleepFor(5000);
    await MeshOSTest.stopWorkers();
    await RedisConnection.disconnectAll();
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
      const client = new MeshOSTest({ id: nanoid(), await: true });
      const doubled = await client.stringDoubler('hello');
      expect(doubled).toBe('hellohello');
    });
  });

  describe('Create/Start a Workflow and return the workflow handle', () => {
    it('should start a new workflow', async () => {
      const client = new MeshOSTest(guid);
      const handle = await client.create(100);
      expect(handle).not.toBeUndefined();
    });
  });

  describe('Query Custom Value', () => {
    it('should query for custom state fields', async () => {
      const handle = await MeshOSTest.get(guid);
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
      const client = new MeshOSTest(guid);
      const result = await client.decrement(11);
      expect(result).not.toBeUndefined();
    }, 10_000);
  });

  describe('Get Workflow', () => {
    it('should get the workflow status', async () => {
      const handle = await MeshOSTest.get(guid);
      const result = await handle.status();
      expect(result).not.toBeUndefined();
    });

    it('should get the workflow data and metadata', async () => {
      const handle = await MeshOSTest.get(guid);
      const result = await handle.state(true);
      expect(result).not.toBeUndefined();
    });
  });

  describe('Search', () => {
    it('should find a workflow using FT search query syntax', async () => {
      let count: string | number = 0;
      let rest: any;
      do {
        [count, ...rest] = await MeshOSTest.find(
          {},
          '@_quantity:[89 89]',
          'RETURN',
          '1',
          '_quantity'
        );
        await sleepFor(500);
      } while (count as number === 0);
      expect(count).toBe(1);
    }, 15_000);
  });

  describe('Result', () => {
    it('should publish the workflow results', async () => {
      const handle = await MeshOSTest.get(guid);
      const result = await handle.result(true);
      expect(result).toBe('89');
    }, 25_000);
  });
});
