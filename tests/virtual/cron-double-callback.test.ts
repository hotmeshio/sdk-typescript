/**
 * Tests for the cron double-callback fix and Virtual.getContext().
 *
 * Root cause of the double-callback bug:
 *   Math.floor in registerTimeHook (services/task/index.ts) rounded the
 *   wakeup time slot DOWN, firing the time hook up to one fidelity
 *   period BEFORE the intended sleep elapsed.  When that early wakeup
 *   preceded a cron tick boundary, nextDelay() computed a small
 *   residual (≤ fidelity), causing a rapid re-cycle that fired the
 *   callback a second time once the real tick arrived.
 *
 * Fix: Math.floor → Math.ceil ensures the hook fires AT or AFTER the
 * intended wakeup, so nextDelay always sees the tick as past and
 * computes the full interval to the NEXT tick.
 *
 * Run with HMSH_FIDELITY_SECONDS=5 to reproduce the bug under stash:
 *   docker compose exec -e HMSH_FIDELITY_SECONDS=5 hotmesh \
 *     npx vitest run tests/virtual/cron-double-callback.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { guid, sleepFor } from '../../modules/utils';
import { Virtual } from '../../services/virtual';
import { VirtualContext } from '../../types/virtual';
import { PostgresConnection } from '../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../types/provider';
import { HMSH_FIDELITY_SECONDS } from '../../modules/enums';
import { dropTables, postgres_options } from '../$setup/postgres';

describe('CRON DOUBLE-CALLBACK | Postgres', () => {
  let postgresClient: ProviderNativeClient;

  const connection = {
    class: Postgres,
    options: postgres_options,
  };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  }, 15_000);

  afterAll(async () => {
    await Virtual.shutdown();
  }, 10_000);

  it(`should fire the cron callback exactly once per tick (fidelity=${HMSH_FIDELITY_SECONDS}s)`, async () => {
    const callbacks: number[] = [];
    const contexts: VirtualContext[] = [];
    const CRON_ID = 'cron-dbl-cb-001';

    const started = await Virtual.cron({
      guid: 'cron-dbl-cb',
      args: [],
      topic: 'cron.double.callback.test',
      connection,
      options: {
        id: CRON_ID,
        interval: '* * * * *',
      },
      callback: async (): Promise<void> => {
        callbacks.push(Date.now());
        contexts.push(Virtual.getContext());
      },
    });
    expect(started).toBe(true);

    // Wait long enough to cross exactly one minute boundary + buffer
    const now = new Date();
    const msUntilNextMinute =
      (60 - now.getSeconds()) * 1000 - now.getMilliseconds();
    const waitMs = msUntilNextMinute + 15_000;
    console.log(
      `[cron-dbl-cb] started at ${now.toISOString()}, ` +
        `waiting ${Math.round(waitMs / 1000)}s to cross minute boundary`,
    );

    await sleepFor(waitMs);

    await Virtual.interrupt({
      topic: 'cron.double.callback.test',
      connection,
      options: { id: CRON_ID },
    });

    await sleepFor(3_000);

    console.log(
      `[cron-dbl-cb] callback count: ${callbacks.length}`,
      callbacks.map((t) => new Date(t).toISOString()),
    );

    // ── No double callbacks ────────────────────────────────────────
    expect(callbacks.length).toBeGreaterThanOrEqual(1);

    for (let i = 1; i < callbacks.length; i++) {
      const gap = callbacks[i] - callbacks[i - 1];
      console.log(
        `[cron-dbl-cb] gap between callback ${i - 1}→${i}: ${gap} ms`,
      );
      expect(gap).toBeGreaterThan(30_000);
    }

    // ── Virtual.getContext() provides cron execution metadata ──────
    const ctx = contexts[0];
    console.log('[cron-dbl-cb] context:', ctx);
    expect(ctx).toBeDefined();
    expect(ctx.topic).toBe('cron.double.callback.test');
    expect(ctx.workflowId).toBe(CRON_ID);
    expect(ctx.workflowName).toBe('hmsh.cron');
    expect(ctx.guid).toBeTruthy();
    expect(typeof ctx.guid).toBe('string');
    expect(ctx.attempt).toBeGreaterThanOrEqual(1);
    expect(typeof ctx.dimension).toBe('string');
    expect(typeof ctx.traceId).toBe('string');
    expect(typeof ctx.spanId).toBe('string');
  }, 90_000);

  it('should provide context for Virtual.exec callbacks', async () => {
    let captured: VirtualContext | undefined;

    await Virtual.connect({
      guid: 'ctx-exec',
      topic: 'ctx.exec.test',
      connection,
      callback: async (
        payload: Record<string, any>,
      ): Promise<Record<string, any>> => {
        captured = Virtual.getContext();
        return { echo: payload };
      },
    });

    await Virtual.exec({
      args: [{ hello: 'world' }],
      topic: 'ctx.exec.test',
      connection,
    });

    expect(captured).toBeDefined();
    expect(captured.topic).toBe('ctx.exec.test');
    expect(captured.workflowId).toBeTruthy();
    expect(captured.workflowName).toBe('hmsh.call');
    expect(captured.guid).toBeTruthy();
    expect(captured.attempt).toBeGreaterThanOrEqual(1);
  }, 15_000);

  it('should throw when getContext() is called outside a callback', () => {
    expect(() => Virtual.getContext()).toThrow(
      'Virtual.getContext() called outside of a Virtual callback execution context',
    );
  });
});
