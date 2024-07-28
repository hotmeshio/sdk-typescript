import * as Redis from 'redis';

import config from '../$setup/config';
import { guid, sleepFor } from '../../modules/utils';
import { MeshCall } from '../../services/meshcall';
import { RedisConnection } from '../../services/connector/clients/redis';
import { RedisRedisClassType } from '../../types';

describe('MESHCALL', () => {
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
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis as unknown as RedisRedisClassType,
      options,
    );
    redisConnection.getClient().flushDb();
  });

  afterAll(async () => {
    await MeshCall.shutdown();
  }, 10_000);

  describe('Worker', () => {
    describe('connect', () => {
      it('should connect a worker', async () => {
        const worker = await MeshCall.connect({
          guid: 'jimmy',
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          callback: async (
            payload: Record<string, any>,
          ): Promise<Record<string, any>> => {
            return { hello: payload };
          },
        });
        expect(worker).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('exec', () => {
      it('should call a function', async () => {
        const response = await MeshCall.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
        });
        expect(response.hello.payload).toBe('HotMesh');
      });
    });

    describe('cache', () => {
      it('should call a function and cache the result', async () => {
        let response = await MeshCall.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'CoolMesh' }],
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        expect(response.hello.payload).toBe('CoolMesh');

        //send a new request with different arg, but same id and ttl
        response = await MeshCall.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        //expect cached value
        expect(response.hello.payload).toBe('CoolMesh');
      });

      it('should ignore the cache and call the function', async () => {
        const response = await MeshCall.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mytest123', ttl: '1 minute', flush: true },
        });
        expect(response.hello.payload).toBe('HotMesh');
      });

      it('should use the cached response (Redis HMGET)', async () => {
        const response = await MeshCall.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        expect(response.hello.payload).toBe('HotMesh');
      });

      it('should flush the cache', async () => {
        //manually flush first
        await MeshCall.flush({
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mytest123' },
        });

        //expect nothing in the cache and input to be echoed
        const response = await MeshCall.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'ColdMesh' }],
          topic: 'my.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        expect(response.hello.payload).toBe('ColdMesh');
      });
    });
  });

  describe('Cron', () => {
    describe('infinite cron', () => {
      it('should run an infinite cron', async () => {
        let counter = 0;
        const inited = await MeshCall.cron({
          guid: 'franky',
          args: [{ payload: 'HotMesh' }],
          topic: 'my.cron.function',
          redis: {
            class: Redis,
            options,
          },
          options: {
            id: 'mycron123',
            interval: '1 second',
          },
          callback: async (): Promise<number> => {
            counter++;
            return counter;
          },
        });
        expect(inited).toBe(true);
        await sleepFor(3_500);
        expect(counter).toBeGreaterThan(1);
      }, 5_000);

      it('should silently fail when the same cron is inited', async () => {
        const didSucceed = await MeshCall.cron({
          guid: 'freddy',
          args: [{ payload: 'HotMesh' }],
          topic: 'my.cron.function',
          redis: {
            class: Redis,
            options,
          },
          options: {
            id: 'mycron123',
            interval: '1 second',
          },
          callback: async (): Promise<void> => {
            //do nothing
          },
        });
        expect(didSucceed).toBe(false);
      });

      it('should interrupt an infinite cron', async () => {
        await MeshCall.interrupt({
          topic: 'my.cron.function',
          redis: {
            class: Redis,
            options,
          },
          options: { id: 'mycron123' },
        });
      });

      it('should run a cron with maxCycles and a delay', async () => {
        let counter = 0;
        const inited = await MeshCall.cron({
          guid: 'buddy',
          args: [{ payload: 'HotMesh' }],
          //NOTE: must use different topic for this cron,
          //      so the other cron callback isn't called
          //      (which references the other `counter`)
          topic: 'my.cron.function.max',
          redis: {
            class: Redis,
            options,
          },
          options: {
            id: 'mycron456',
            interval: '1 second',
            maxCycles: 2,
            delay: '1 second',
          },
          callback: async (): Promise<number> => {
            counter++;
            return counter;
          },
        });
        expect(inited).toBe(true);
        await sleepFor(4_500);
        expect(counter).toBe(2);
      }, 6_500);
    });
  });
});