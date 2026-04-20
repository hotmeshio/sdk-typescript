import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { guid, sleepFor } from '../../modules/utils';
import { Virtual } from '../../services/virtual';
import { PostgresConnection } from '../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../types/provider';
import { dropTables, postgres_options } from '../$setup/postgres';

describe('VIRTUAL | Postgres', () => {
  let postgresClient: ProviderNativeClient;

  const connection = {
    class: Postgres,
    options: postgres_options,
  };

  beforeAll(async () => {
    // Initialize Postgres and drop tables (and data) from prior tests
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await Virtual.shutdown();
  }, 10_000);

  describe('Worker', () => {
    describe('connect', () => {
      it('should connect a worker', async () => {
        const worker = await Virtual.connect({
          guid: 'jimmy',
          topic: 'my.function',
          connection,
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
        const response = await Virtual.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          connection,
        });
        expect(response.hello.payload).toBe('HotMesh');
      });
    });

    describe('cache', () => {
      it('should call a function and cache the result', async () => {
        let response = await Virtual.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'CoolMesh' }],
          topic: 'my.function',
          connection,
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        expect(response.hello.payload).toBe('CoolMesh');

        //send a new request with different arg, but same id and ttl
        response = await Virtual.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          connection,
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        //expect cached value
        expect(response.hello.payload).toBe('CoolMesh');
      });

      it('should ignore the cache and call the function', async () => {
        const response = await Virtual.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          connection,
          options: { id: 'mytest123', ttl: '1 minute', flush: true },
        });
        expect(response.hello.payload).toBe('HotMesh');
      });

      it('should use the cached response', async () => {
        const response = await Virtual.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'HotMesh' }],
          topic: 'my.function',
          connection,
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        expect(response.hello.payload).toBe('HotMesh');
      });

      it('should flush the cache', async () => {
        //manually flush first
        await Virtual.flush({
          topic: 'my.function',
          connection,
          options: { id: 'mytest123' },
        });

        //expect nothing in the cache and input to be echoed
        const response = await Virtual.exec<{ hello: { payload: string } }>({
          args: [{ payload: 'ColdMesh' }],
          topic: 'my.function',
          connection,
          options: { id: 'mytest123', ttl: '1 minute' },
        });
        expect(response.hello.payload).toBe('ColdMesh');
      });
    });
  });

  describe('Cron', () => {
    describe('idempotent cron', () => {
      it('should start an idempotent cron (readonly mode)', async () => {
        //kick off the cron, but don't do work
        //(the next test will register the callback)
        const inited = await Virtual.cron({
          //NOTE: this ID will show up in the logs as being inited in readonly mode
          guid: 'idemcron-RO',
          args: [{ payload: 'HotMesh' }],
          topic: 'my.cron.function',
          connection,
          options: {
            id: 'mycron123',
            interval: '1 second',
          },
        });
        expect(inited).toBe(true);
      });

      it('should run an idempotent cron', async () => {
        let counter = 0;
        const inited = await Virtual.cron({
          guid: 'idemcron-RW',
          args: [{ payload: 'HotMesh' }],
          topic: 'my.cron.function',
          connection,
          options: {
            id: 'mycron123',
            interval: '1 second',
          },
          callback: async (): Promise<number> => {
            counter++;
            return counter;
          },
        });
        //the cron was already started (this test provides the callback)
        expect(inited).toBe(false);
        //sleepFor is to ensure sufficient cycles run;
        //todo: subscribe to channel and listen instead
        await sleepFor(7_500);
        expect(counter).toBeGreaterThan(1);
      }, 10_000);

      it('should silently fail when the same cron is inited', async () => {
        const didSucceed = await Virtual.cron({
          guid: 'freddy',
          args: [{ payload: 'HotMesh' }],
          topic: 'my.cron.function',
          connection,
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

      it('should interrupt an idempotent cron', async () => {
        let interrupted = await Virtual.interrupt({
          topic: 'my.cron.function',
          connection,
          options: { id: 'mycron123' },
        });
        expect(interrupted).toBe(true);

        //method returns false if the cron is not running
        interrupted = await Virtual.interrupt({
          topic: 'my.cron.function',
          connection,
          options: { id: 'mycron123' },
        });
        expect(interrupted).toBe(false);
      });

      it('should run a cron with maxCycles and a delay', async () => {
        let counter = 0;
        const inited = await Virtual.cron({
          guid: 'buddy',
          args: [{ payload: 'HotMesh' }],
          //NOTE: must use different topic for this cron,
          //      so the other cron callback isn't called
          //      (which references the other `counter`)
          topic: 'my.cron.function.max',
          connection,
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

    describe('error handling', () => {
      it('should stop after 3 retries when retry policy set to 3', async () => {
        let callCount = 0;

        await Virtual.cron({
          topic: 'test.cron.retry3',
          connection,
          args: [],
          callback: async () => {
            callCount++;
            throw new Error('deliberate failure');
          },
          options: { id: 'retry3-test', interval: '1 second' },
          retry: {
            maximumAttempts: 3,
            backoffCoefficient: 2,
            maximumInterval: 30,
          },
        });

        await sleepFor(15_000);

        console.log(`retry3: callCount=${callCount}`);
        expect(callCount).toBe(3);
      }, 30_000);

      it('should stop after 5 retries when retry policy set to 5', async () => {
        let callCount = 0;

        await Virtual.cron({
          topic: 'test.cron.retry5',
          connection,
          args: [],
          callback: async () => {
            callCount++;
            throw new Error('deliberate failure');
          },
          options: { id: 'retry5-test', interval: '1 second' },
          retry: {
            maximumAttempts: 5,
            backoffCoefficient: 2,
            maximumInterval: 10,
          },
        });

        await sleepFor(45_000);

        console.log(`retry5: callCount=${callCount}`);
        expect(callCount).toBe(5);
      }, 60_000);

      it('should respect backoff between retries on exec', async () => {
        const timestamps: number[] = [];

        await Virtual.connect({
          topic: 'test.backoff',
          connection,
          callback: async () => {
            timestamps.push(Date.now());
            throw new Error('always fails');
          },
          retry: {
            maximumAttempts: 3,
            backoffCoefficient: 2,
            maximumInterval: 30,
          },
        });

        try {
          await Virtual.exec({
            args: [],
            topic: 'test.backoff',
            connection,
          });
        } catch {
          // expected to fail after retries exhausted
        }

        console.log(`backoff attempts: ${timestamps.length}`);
        if (timestamps.length > 1) {
          const gaps = timestamps.slice(1).map((t, i) => t - timestamps[i]);
          console.log(`backoff gaps (ms): ${JSON.stringify(gaps)}`);
          for (let i = 1; i < gaps.length; i++) {
            expect(gaps[i]).toBeGreaterThanOrEqual(gaps[i - 1] * 0.5);
          }
        }
        expect(timestamps.length).toBeLessThanOrEqual(3);
      }, 30_000);
    });
  });
});
