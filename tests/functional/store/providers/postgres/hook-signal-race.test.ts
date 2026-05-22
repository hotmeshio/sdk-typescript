import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Client } from 'pg';

import { HMNS } from '../../../../../modules/key';
import { guid } from '../../../../../modules/utils';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { LoggerService } from '../../../../../services/logger';
import { PostgresStoreService } from '../../../../../services/store/providers/postgres/postgres';
import { PostgresClientType } from '../../../../../types/postgres';
import {
  ProviderNativeClient,
  ProviderClient,
} from '../../../../../types/provider';
import { dropTables, truncateTables } from '../../../../$setup/postgres';

/**
 * Targeted store-level concurrency test for the hook-signal race.
 *
 * Uses two independent PG connections (simulating engine and worker)
 * to exercise the exact race the reviewer described:
 *
 *   1. Leg1 (setHookSignal) registers a hook
 *   2. Leg2 (getHookSignal) delivers a signal / stores $pending
 *   3. Under concurrency, both may start when no row exists
 *
 * For EVERY iteration, exactly one of these must be true:
 *   A. getHookSignal returned the hookId  (signal delivered directly)
 *   B. setHookSignal returned pendingData (signal consumed from $pending)
 *
 * If neither is true, the signal was lost.
 */
describe('FUNCTIONAL | Hook-Signal Race | Store-Level Concurrency', () => {
  let pgClientA: ProviderNativeClient;
  let pgClientB: ProviderNativeClient;
  let storeA: PostgresStoreService;
  let storeB: PostgresStoreService;
  const APP_ID = 'hook-race-test';

  const pgOpts = {
    user: 'postgres',
    host: 'postgres',
    database: 'hotmesh',
    password: 'password',
    port: 5432,
  };

  beforeAll(async () => {
    // Two independent PG connections — no shared transaction state
    pgClientA = (
      await PostgresConnection.connect(guid(), Client, pgOpts)
    ).getClient();
    pgClientB = (
      await PostgresConnection.connect(guid(), Client, pgOpts)
    ).getClient();

    await dropTables(pgClientA);

    storeA = new PostgresStoreService(
      pgClientA as PostgresClientType & ProviderClient,
    );
    await storeA.init(HMNS, APP_ID, new LoggerService());

    storeB = new PostgresStoreService(
      pgClientB as PostgresClientType & ProviderClient,
    );
    await storeB.init(HMNS, APP_ID, new LoggerService());
  }, 30_000);

  beforeEach(async () => {
    await truncateTables(pgClientA);
  });

  afterAll(async () => {
    await pgClientA.end();
    await pgClientB.end();
  });

  it('should never lose a signal under concurrent setHookSignal/getHookSignal (100 iterations)', async () => {
    const ITERATIONS = 100;
    let directDeliveries = 0;
    let pendingDeliveries = 0;
    let signalsLost = 0;

    for (let i = 0; i < ITERATIONS; i++) {
      const topic = `race-topic`;
      const resolved = `iter-${i}-${guid()}`;
      const hookJobId = `hook-${i}`;
      const signalPayload = JSON.stringify({ data: `signal-${i}` });

      // Launch Leg1 and Leg2 concurrently on separate connections
      const [setResult, getResult] = await Promise.all([
        // Leg1: register hook (connection A)
        storeA.setHookSignal({
          topic,
          resolved,
          jobId: hookJobId,
          expire: 600,
        }),
        // Leg2: deliver signal (connection B)
        storeB.getHookSignal(topic, resolved, signalPayload, 600),
      ]);

      const deliveredDirectly = getResult === hookJobId;
      const deliveredViaPending = !!setResult.pendingData;

      if (deliveredDirectly) {
        directDeliveries++;
      } else if (deliveredViaPending) {
        pendingDeliveries++;
        // Verify the pending data is correct
        expect(setResult.pendingData).toBe(signalPayload);
      } else {
        signalsLost++;
      }
    }

    console.log(
      `[hook-signal-race] direct=${directDeliveries} pending=${pendingDeliveries} lost=${signalsLost}`,
    );

    expect(signalsLost).toBe(0);
    expect(directDeliveries + pendingDeliveries).toBe(ITERATIONS);
  }, 60_000);

  it('should handle Leg2-first (pending path) correctly', async () => {
    const topic = 'pending-test';
    const resolved = `pending-${guid()}`;
    const hookJobId = 'hook-pending';
    const signalPayload = JSON.stringify({ order: 'approved' });

    // Leg2 arrives first — stores $pending
    const getResult = await storeB.getHookSignal(
      topic, resolved, signalPayload, 600,
    );
    expect(getResult).toBeUndefined(); // no hook yet, pending stored

    // Leg1 arrives later — should consume $pending
    const setResult = await storeA.setHookSignal({
      topic,
      resolved,
      jobId: hookJobId,
      expire: 600,
    });
    expect(setResult.success).toBe(true);
    expect(setResult.pendingData).toBe(signalPayload);
  });

  it('should handle Leg1-first (direct path) correctly', async () => {
    const topic = 'direct-test';
    const resolved = `direct-${guid()}`;
    const hookJobId = 'hook-direct';
    const signalPayload = JSON.stringify({ order: 'shipped' });

    // Leg1 arrives first — registers hook
    const setResult = await storeA.setHookSignal({
      topic,
      resolved,
      jobId: hookJobId,
      expire: 600,
    });
    expect(setResult.success).toBe(true);
    expect(setResult.pendingData).toBeUndefined();

    // Leg2 arrives later — finds hook, returns it
    const getResult = await storeB.getHookSignal(
      topic, resolved, signalPayload, 600,
    );
    expect(getResult).toBe(hookJobId);
  });

  it('should handle rapid alternating races without signal loss', async () => {
    const ITERATIONS = 50;
    let lost = 0;

    // Alternate which leg "starts first" by varying the topic
    const promises = Array.from({ length: ITERATIONS }, (_, i) => {
      const topic = 'alternating';
      const resolved = `alt-${i}-${guid()}`;
      const hookJobId = `hook-alt-${i}`;
      const signalPayload = JSON.stringify({ i });

      // Odd iterations: Leg2 starts slightly before Leg1
      // Even iterations: Leg1 starts slightly before Leg2
      // Both still race — the slight ordering bias exercises both paths
      if (i % 2 === 0) {
        return Promise.all([
          storeA.setHookSignal({ topic, resolved, jobId: hookJobId, expire: 600 }),
          storeB.getHookSignal(topic, resolved, signalPayload, 600),
        ]).then(([setRes, getRes]) => {
          if (getRes !== hookJobId && !setRes.pendingData) lost++;
        });
      } else {
        return Promise.all([
          storeB.getHookSignal(topic, resolved, signalPayload, 600),
          storeA.setHookSignal({ topic, resolved, jobId: hookJobId, expire: 600 }),
        ]).then(([getRes, setRes]) => {
          if (getRes !== hookJobId && !setRes.pendingData) lost++;
        });
      }
    });

    await Promise.all(promises);

    expect(lost).toBe(0);
  }, 60_000);
});
