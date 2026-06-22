/**
 * Regression guard for the engine startup version race.
 *
 * Scenario: a worker starts during (or just before) a schema hot-swap.
 * The schema is activated and a NOTIFY is broadcast, but the worker misses
 * it (LISTEN not yet established on the eventClient).  The first call to
 * getVID() is triggered from getSchema(), which passes the `app` object it
 * got from store.getApp() — but that object came from the store's own
 * in-memory cache, which was populated before the activation committed.
 *
 * Before the fix: getVID() locked in the stale version from the `vid`
 * parameter without querying the DB, so the worker loaded the old schema
 * for its entire lifetime.  In the long-tail incident this meant the v15
 * schema (no `escalation:` block) was used → `this.config.escalation` was
 * undefined → PATH 1 silent-return in addEscalationToTransaction → zero
 * escalation rows written for ~41% of requests (one of two workers stuck).
 *
 * After the fix: getVID() always DB-refreshes on first initialization,
 * ignoring the stale `vid` parameter.  One extra query per engine lifetime.
 */
import { describe, it, expect, vi } from 'vitest';
import { getVID } from '../../../../services/engine/version';

const makeInstance = (
  freshVersion: string,
  appId = 'test-app',
): { instance: Parameters<typeof getVID>[0]; mockGetApp: ReturnType<typeof vi.fn> } => {
  const mockGetApp = vi.fn().mockResolvedValue({
    id: appId,
    version: freshVersion,
    active: true,
  });

  const instance = {
    appId,
    guid: 'test-guid',
    apps: null as any,
    cacheMode: 'cache' as const,
    untilVersion: null as any,
    store: { getApp: mockGetApp } as any,
    logger: {
      info: vi.fn(),
      debug: vi.fn(),
      error: vi.fn(),
      warn: vi.fn(),
    } as any,
  };

  return { instance, mockGetApp };
};

describe('engine/version | getVID | startup race', () => {
  it('refreshes from DB on first lock-in, ignoring stale vid parameter', async () => {
    const STALE_VERSION = '15';
    const FRESH_VERSION = '16';
    const { instance, mockGetApp } = makeInstance(FRESH_VERSION);

    // Simulate what getSchema() does: it calls store.getApp() (which may be
    // stale/cached), gets the old version, then passes it to engine.getVID().
    const staleVid = { id: 'test-app', version: STALE_VERSION };
    const result = await getVID(instance, staleVid);

    // Must return the fresh DB version, not the stale vid.
    expect(result.version).toBe(FRESH_VERSION);

    // Must have done a DB read with refresh=true to confirm the version.
    expect(mockGetApp).toHaveBeenCalledWith('test-app', true);
  });

  it('does not issue extra DB queries when apps is already initialized', async () => {
    const CACHED_VERSION = '16';
    const { instance, mockGetApp } = makeInstance(CACHED_VERSION);

    // Pre-initialize apps (simulates all subsequent calls after first lock-in).
    instance.apps = {
      'test-app': { id: 'test-app', version: CACHED_VERSION, active: true } as any,
    };

    const result = await getVID(instance);

    expect(result.version).toBe(CACHED_VERSION);
    // No DB query — version already locked in and current.
    expect(mockGetApp).not.toHaveBeenCalled();
  });

  it('refreshes on every call in nocache mode until untilVersion is reached', async () => {
    const FRESH_VERSION = '16';
    const { instance, mockGetApp } = makeInstance(FRESH_VERSION);

    instance.cacheMode = 'nocache';
    instance.untilVersion = FRESH_VERSION;

    const result = await getVID(instance);

    expect(result.version).toBe(FRESH_VERSION);
    expect(mockGetApp).toHaveBeenCalledWith('test-app', true);
    // Once the target version is confirmed the engine switches back to cache mode.
    expect(instance.cacheMode).toBe('cache');
  });
});
